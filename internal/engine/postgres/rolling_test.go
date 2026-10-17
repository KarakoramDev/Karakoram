// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func fakeSharedMemory(parameters map[string]string) map[string]int64 {
	values := make(map[string]int64, len(sharedMemoryKeys))
	for _, key := range sharedMemoryKeys {
		values[key], _ = sharedMemoryValue(parameters, key)
	}
	return values
}

func parameterOptions(parameters map[string]string) json.RawMessage {
	raw, _ := json.Marshal(EngineOptions{SynchronousMode: "off", Parameters: parameters})
	return raw
}

func TestSharedMemoryRestartDirectionsResume(t *testing.T) {
	for _, tc := range []struct {
		name       string
		old, next  map[string]string
		directions []string
	}{
		{"increase", map[string]string{"max_connections": "100"}, map[string]string{"max_connections": "200"}, []string{"increase"}},
		{"decrease", map[string]string{"max_connections": "200"}, map[string]string{"max_connections": "100"}, []string{"decrease"}},
		{"mixed", map[string]string{"max_connections": "200", "max_worker_processes": "8"}, map[string]string{"max_connections": "100", "max_worker_processes": "16"}, []string{"increase", "decrease"}},
		{"defaults", map[string]string{"max_connections": "200"}, nil, []string{"decrease"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			rc.Cluster.Spec.EngineOptions = parameterOptions(tc.old)
			converge(t, p, rc)
			b.actions = nil
			rc.Cluster.Spec.EngineOptions = parameterOptions(tc.next)
			rc.Cluster.Generation++
			seen := map[int]bool{}
			var directions []string
			for range 100 {
				p = New(p.options) // Recreate the provider at every durable checkpoint.
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if s.Rolling != nil && !seen[s.Rolling.Phase] {
					seen[s.Rolling.Phase] = true
					directions = append(directions, s.Rolling.Phases[s.Rolling.Phase].Direction)
					// All pulls, including the old writer's, must render the same
					// mixed-change intermediate rather than patching stale DCS values.
					var expected map[string]string
					for _, m := range s.data() {
						config, err := p.MemberConfig(t.Context(), rc, m.Name)
						if err != nil {
							t.Fatal(err)
						}
						var spec pgspec.Spec
						if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
							t.Fatal(err)
						}
						if expected != nil && !reflect.DeepEqual(expected, spec.Postgres.Parameters) {
							t.Fatal("members rendered different dynamic phases")
						}
						expected = spec.Postgres.Parameters
					}
					if tc.name == "mixed" && s.Rolling.Phase == 0 && (expected["max_connections"] != "200" || expected["max_worker_processes"] != "16") {
						t.Fatal("mixed change lowered a GUC before completing increases", expected)
					}
				}
				if s.Rolling == nil && len(directions) > 0 && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					break
				}
			}
			if !slices.Equal(directions, tc.directions) {
				t.Fatalf("phase directions %v, want %v", directions, tc.directions)
			}
			s, _ := load(rc.Cluster)
			if s.Rolling != nil || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("directional restart did not converge")
			}
			var restarted []string
			for _, action := range b.actions {
				if strings.HasPrefix(action, "/v1/postgres/restart:") {
					restarted = append(restarted, strings.TrimPrefix(action, "/v1/postgres/restart:"))
				}
			}
			if len(restarted) != 3*len(tc.directions) {
				t.Fatal("unexpected restart sequence", b.actions)
			}
			if tc.directions[0] == "decrease" {
				if restarted[0] != "demop0" || b.leader != "" || !slices.Contains(b.actions, "primary-first:demop0") {
					t.Fatal("decrease switched to an old-value standby or failed to restart primary first", b.actions)
				}
			} else if restarted[0] == "demop0" || restarted[2] != "demop0" {
				t.Fatal("increase did not restart standbys before former primary", b.actions)
			}
			for _, m := range s.data() {
				if !reflect.DeepEqual(b.sharedMemory[m.Name], fakeSharedMemory(tc.next)) {
					t.Fatal("running GUC values did not converge", m.Name, b.sharedMemory[m.Name])
				}
			}
		})
	}
}

func TestSharedMemoryDecreaseWaitsForReplayBarrier(t *testing.T) {
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "200"})
	converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "100"})
	b.actions = nil
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demop1": {Role: "replica", Running: true, Streaming: true, Timeline: 1, WALPosition: "1/FF"},
	}
	for range 15 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.Rolling == nil || s.Rolling.Barrier != "1/100" || slices.Contains(b.actions, "/v1/postgres/restart:demop1") {
		t.Fatal("standby restarted before replaying the primary's decreased control values", b.actions)
	}
	st := b.statusOverrides["demop1"]
	st.WALPosition = "1/100"
	b.statusOverrides["demop1"] = st
	for range 10 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ = load(rc.Cluster)
	if s.Rolling != nil || !slices.Contains(b.actions, "/v1/postgres/restart:demop1") {
		t.Fatal("replayed standby did not resume", b.actions)
	}
}

func TestSharedMemoryAllParametersAndDefaultDirection(t *testing.T) {
	for _, key := range sharedMemoryKeys {
		t.Run(key, func(t *testing.T) {
			p, rc, _ := setup(t, "ha")
			s := converge(t, p, rc)
			defaultValue, _ := pgspec.SharedMemoryDefault(key)
			rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{key: "256"})
			journal, err := p.planSharedMemory(rc, s)
			if err != nil || journal == nil || journal.Phases[0].Direction != "increase" {
				t.Fatal("shared-memory increase not classified", defaultValue, err)
			}
			for _, m := range s.data() {
				m.Options = rc.Cluster.Spec.EngineOptions
			}
			rc.Cluster.Spec.EngineOptions = nil
			journal, err = p.planSharedMemory(rc, s)
			if err != nil || journal == nil || journal.Phases[0].Direction != "decrease" {
				t.Fatal("removing a shared-memory override did not restore default primary-first", err)
			}
		})
	}
}

func TestSharedMemoryDecreaseReobservesNewPrimary(t *testing.T) {
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "200"})
	converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "100"})
	b.actions = nil
	for range 15 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ := load(rc.Cluster)
		if s.Rolling != nil && s.Rolling.Barrier != "" {
			break
		}
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatal("queued operation can preempt the directional restart journal", err)
	}
	// A promotion after the first writer restarted invalidates its replay
	// barrier. The new old-value writer must decrease before any standby.
	b.leader = "demop1"
	b.actions = nil
	p = New(p.options)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(b.actions) != 2 || b.actions[0] != "/v1/postgres/restart:demop1" || b.actions[1] != "primary-first:demop1" {
		t.Fatal("leadership change restarted a standby before the new primary", b.actions)
	}
	for range 15 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.Rolling != nil {
		t.Fatal("new-primary decrease did not converge")
	}
}

func TestSharedMemoryRolloutDefersScaleUpAcrossRestart(t *testing.T) {
	for _, direction := range []string{"increase", "decrease", "mixed"} {
		t.Run(direction, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			old := map[string]string{"max_connections": "100", "max_worker_processes": "16"}
			next := map[string]string{"max_connections": "200", "max_worker_processes": "16"}
			if direction == "decrease" {
				old, next = next, old
			} else if direction == "mixed" {
				next["max_worker_processes"] = "8"
			}
			rc.Cluster.Spec.EngineOptions = parameterOptions(old)
			initial := converge(t, p, rc)
			nextIndex := initial.NextIndex
			rc.Cluster.Spec.EngineOptions = parameterOptions(next)
			rc.Cluster.Generation++
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil || s.Rolling == nil {
				t.Fatalf("rollout did not become durable: %v", err)
			}
			rc.Cluster.Spec.Components.Engine.Replicas = 5
			rc.Cluster.Generation++
			sawJournal, completed := false, false
			for range 200 {
				p = New(p.options) // Reload only durable state after every CP checkpoint.
				before, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if before.Rolling != nil {
					sawJournal = true
					if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
						t.Fatalf("queued work preempted rollout: %v", err)
					}
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if before.Rolling != nil && (len(s.data()) != 3 || s.NextIndex != nextIndex) {
					t.Fatal("membership allocated before durable rollout completed")
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if s.Rolling == nil && len(s.data()) == 5 && status.Phase == "Ready" && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					completed = true
					break
				}
			}
			if !sawJournal || !completed {
				t.Fatal("rollout and subsequent scale-up did not converge", string(rc.Cluster.Internal))
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
				t.Fatalf("queued operations not released: %v", err)
			}
			for _, m := range s.data() {
				if m.Stage != "joined" || m.EtcdLearner || !reflect.DeepEqual(b.sharedMemory[m.Name], fakeSharedMemory(next)) {
					t.Fatalf("member did not join with target GUCs: %s %+v", m.Name, b.sharedMemory[m.Name])
				}
			}
			if len(b.etcd) != 5 {
				t.Fatalf("scale-up did not produce five voters: %+v", b.etcd)
			}
		})
	}
}
