// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

func TestManagedListenerAliasesAndShellSubstitutionAreRejected(t *testing.T) {
	p, rc, _, _ := setup(t)
	for _, config := range []string{
		"management.listener.port = 1234",
		"management.listener.ssl = false",
		"stream.listeners.tcp.1 = 5552",
		"stream.listeners.ssl.1 = 5551",
		"stream.replication.port_range.min = 6000",
		"stream.replication.port_range.max = 6100",
		"stream.ssl.verify = verify_none",
		"osiris.replication.port_range.min = 6200",
		"osiris.replication.port_range.max = 6300",
		"osiris.replication.transport = tcp",
		"cluster_name = $(touch /tmp/untrusted)",
		"consumer_timeout = $(60000)",
	} {
		t.Run(config, func(t *testing.T) {
			c := cloneCluster(t, rc.Cluster)
			c.Spec.EngineConfig = config
			if err := p.Validate(context.Background(), nil, c); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("unsafe provider-managed override accepted: %q %v", config, err)
			}
		})
	}
}

func TestStreamListenerPortsFollowTLSModeAndRemainPrivate(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
			t.Run(fmt.Sprintf("enabled=%v/%s", enabled, mode), func(t *testing.T) {
				p, rc, _, _ := setup(t)
				rc.Cluster.Spec.TLS.Mode = mode
				if enabled {
					rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"plugins":["rabbitmq_stream"]}`)
				}
				s := state(t, rc)
				m := s.add(rc.Cluster.Name)
				if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
					t.Fatal(err)
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				cfg, err := p.MemberConfig(context.Background(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec rmqspec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				plain, secure := 0, 0
				if enabled && mode != "requireTLS" {
					plain = 5552
				}
				if enabled && mode != "disabled" {
					secure = 5551
				}
				if spec.StreamPort != plain || spec.StreamTLSPort != secure {
					t.Fatalf("stream listener TLS mismatch: %+v", spec)
				}
				text, err := p.zeropsYAML(rc, m, rc.Cluster.Spec.Version)
				if err != nil {
					t.Fatal(err)
				}
				var doc map[string]any
				if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
					t.Fatal(err)
				}
				ports := doc["zerops"].([]any)[0].(map[string]any)["run"].(map[string]any)["ports"].([]any)
				for _, want := range []int{5551, 5552} {
					found := false
					for _, raw := range ports {
						port := raw.(map[string]any)
						if port["port"] == want {
							found = true
							if port["protocol"] != "tcp" || port["httpSupport"] == true {
								t.Fatal("stream listener incorrectly declares public HTTP support", port)
							}
						}
					}
					if found != enabled {
						t.Fatalf("stream port %d declaration mismatch: enabled=%v yaml=%s", want, enabled, text)
					}
					if slices.Contains(p.RelayTargets(rc.Cluster), net.JoinHostPort(m.Name, strconv.Itoa(want))) {
						t.Fatalf("private stream listener exposed through control-plane relay: %d", want)
					}
				}
			})
		}
	}
}

func TestMissingCloneBackupPreservesRestorePhaseAndUnobservedGeneration(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "missing"}
	if err := st.UpdateClusterSpec(ctx, rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	cluster, err := st.GetCluster(ctx, rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster = cluster
	observed := rc.Cluster.Status.ObservedGeneration
	failed := false
	for range 200 {
		_, err := p.Sync(ctx, rc)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("clone failed for the wrong reason: %v", err)
			}
			failed = true
			break
		}
	}
	s := state(t, rc)
	if !failed || s.Phase != "restore" || s.CloneImported || rc.Cluster.Status.ObservedGeneration != observed || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
		t.Fatalf("failed clone published successful reconciliation: state=%s status=%+v", rc.Cluster.Internal, rc.Cluster.Status)
	}
	status, err := p.Status(ctx, rc)
	if err != nil || status.Phase != domain.PhaseRestoring || status.ObservedGeneration != observed {
		t.Fatalf("failed clone reported Ready: %+v %v", status, err)
	}
	if err := st.UpdateClusterState(ctx, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	persisted, err := st.GetCluster(ctx, rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster = persisted
	if state(t, rc).Phase != "restore" || rc.Cluster.Status.ObservedGeneration != observed {
		t.Fatal("failed clone restore checkpoint did not survive persistence")
	}
}

func TestRemovingMemberConfigIncludesSelfButNotOtherDepartures(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Members[1].Removing = true
	s.Members[2].Removing = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		cfg, err := p.MemberConfig(context.Background(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		var spec rmqspec.MemberSpec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		want := []string{"rabbit@demom0"}
		if name != "demom0" {
			want = append(want, "rabbit@"+name)
		}
		if !slices.Equal(spec.Peers, want) || !slices.Contains(spec.Peers, spec.NodeName) {
			t.Fatalf("removal rendered an invalid peer list for %s: %+v", name, spec)
		}
	}
}

func TestDepartureLostAfterStopAppRequiresSurvivingMetadataMajority(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	syncUntil(t, p, rc, func(s *internalState) bool {
		m := s.find("demom2")
		return m != nil && m.RemoveStep == "forget"
	})
	a.broker("demom2").down = true
	a.log.clear()
	before := f.mutations
	if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("departure lost after stop_app bypassed metadata-majority gate", err)
	}
	m := state(t, rc).find("demom2")
	if m == nil || !m.Lost || m.RemoveStep != "forget" || !strings.Contains(m.RemovalBlocked, "Khepri voter majority") || f.mutations != before {
		t.Fatalf("post-stop_app loss was not safely checkpointed: %+v", m)
	}
	for _, call := range a.log.snapshot() {
		if strings.Contains(call, ":forget") {
			t.Fatalf("forgot a newly lost voter without surviving majority: %s", call)
		}
	}
}
