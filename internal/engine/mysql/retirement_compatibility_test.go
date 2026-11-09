// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Released agents expose ONLINE counts, but no full native membership view.
func legacyGRStatus(b *fakeBackend, unavailable bool) func(http.ResponseWriter, *http.Request, string) bool {
	return func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/mysql/status" {
			return false
		}
		if unavailable {
			http.Error(w, "unreachable", http.StatusServiceUnavailable)
			return true
		}
		st := b.native[name]
		st.GroupView = nil
		st.GroupMembers = 0
		for _, peer := range b.native {
			if peer.MemberState == "ONLINE" {
				st.GroupMembers++
			}
		}
		json.NewEncoder(w).Encode(st)
		return true
	}
}

func TestGRStructuralCapabilityAdmission(t *testing.T) {
	for _, mode := range []string{"healthy", "lost-unreachable", "automatic-unreachable", "scale-in", "unreachable"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			if mode == "scale-in" {
				rc.Cluster.Spec.Components.Engine.Replicas = 5
				day2Ticks(t, rc, b, func(s *state) bool { return len(s.data()) == 5 && s.data()[4].Stage == "joined" })
				rc.Cluster.Spec.Components.Engine.Replicas = 3
			}
			s, _ := load(rc.Cluster)
			old := s.data()[len(s.data())-1]
			if mode == "lost-unreachable" || mode == "automatic-unreachable" {
				old.ConfirmedVolumeLost, old.RuntimeRecovery.VolumeLost = true, true
				old.LastGTIDExecuted = recoverySID + ":1-10"
				for name, st := range b.native {
					st.GTIDExecuted = recoverySID + ":1-20"
					b.native[name] = st
				}
				delete(b.native, old.Name)
				p.options.AutoReplaceLostVolumes = true
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			before := bytes.Clone(rc.Cluster.Internal)
			services := len(b.services)
			b.action = legacyGRStatus(b, strings.Contains(mode, "unreachable"))
			var err error
			if mode == "scale-in" || mode == "automatic-unreachable" {
				_, err = p.Sync(t.Context(), rc)
			} else {
				_, err = p.RunOperation(t.Context(), rc, &domain.Operation{ID: "refused", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + old.Name + `"}`)})
			}
			if err == nil || !strings.Contains(err.Error(), "GroupView capability") || !strings.Contains(err.Error(), "karactl -n default clusters upgrade-agents demo --wait") || !strings.Contains(err.Error(), "demom0") {
				t.Fatalf("missing capability precondition: %v", err)
			}
			if strings.Contains(mode, "unreachable") && !strings.Contains(err.Error(), "unknown or unreachable") {
				t.Fatal(err)
			}
			after, _ := load(rc.Cluster)
			if after.Replacement != nil || after.NextIndex != s.NextIndex || len(after.Members) != len(s.Members) || len(b.services) != services || len(b.stopped) != 0 || len(b.deleted) != 0 {
				t.Fatal("refused work changed membership or resources")
			}
			for _, m := range after.data() {
				if m.Retirement != "" {
					t.Fatal("refused work started retirement")
				}
			}
			if mode == "healthy" || mode == "unreachable" || mode == "lost-unreachable" {
				if !bytes.Equal(before, rc.Cluster.Internal) {
					t.Fatal("replacement refusal changed persisted state")
				}
			}
		})
	}
}

func TestGRConfirmedLossLegacyAgentRecovery(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		name := "explicit"
		if automatic {
			name = "automatic"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, b, s, lost := recoveryFixture(t)
			lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
			delete(b.deployed, lost.Name)
			rc.Backend = physicallyLostBackend{fakeBackend: b, volume: lost.Volume, volumeID: lost.VolumeID}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b.action = legacyGRStatus(b, false)
			events := &placementSafetyEvents{}
			rc.Events = events
			op := &domain.Operation{ID: "legacy-loss", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
			if automatic {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				queued, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref(), domain.PhasePending)
				if err != nil || len(queued) != 1 {
					t.Fatalf("replacement queue: %v %v", queued, err)
				}
				op = &queued[0]
			}
			if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
				t.Fatal(err)
			}
			current, _ := load(rc.Cluster)
			raw, _ := json.Marshal(current.Replacement)
			if bytes.Contains(raw, []byte(`"identityBarrier":true`)) {
				t.Fatal("legacy-agent loss recovery opted into unavailable barrier")
			}
			if len(events.events) != 1 || events.events[0].Reason != "LegacyGRRetirement" || !strings.Contains(events.events[0].Message, "upgrade agents after recovery") {
				t.Fatalf("missing admission warning: %+v", events.events)
			}
			runDay2Operation(t, rc, b, op)
			current, _ = load(rc.Cluster)
			if current.find(lost.Name) != nil || current.Replacement != nil || len(current.data()) != 3 {
				t.Fatal("legacy-agent loss recovery did not complete")
			}
		})
	}
}

func TestGRStructuralCapabilityAfterAgentUpgrade(t *testing.T) {
	p, rc, rollout := setupAgentRollout(t, "group")
	b := rollout.fakeBackend
	prior := b.action
	oldStatus := legacyGRStatus(b, false)
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if rollout.builds[name] != engine.CurrentAgentBuild().Build && oldStatus(w, r, name) {
			return true
		}
		return prior != nil && prior(w, r, name)
	}
	op := &domain.Operation{ID: "after-upgrade", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom2"}`)}
	if _, err := p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("old agents admitted replacement")
	}
	finishAgentRollout(t, rc, rollout, mysqlAgentRolloutOperation(false), false)
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	raw, _ := json.Marshal(s.Replacement)
	if !bytes.Contains(raw, []byte(`"identityBarrier":true`)) {
		t.Fatal("new replacement lacks durable identity barrier")
	}
	runDay2Operation(t, rc, b, op)
	// A completed executable rollout also admits scale-in and automatic loss
	// recovery, through their real Sync/queued-operation paths.
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	day2Ticks(t, rc, b, func(s *state) bool {
		if len(s.data()) != 5 {
			return false
		}
		for _, m := range s.data() {
			if m.Stage != "joined" {
				return false
			}
		}
		return true
	})
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	marked := false
	for _, m := range s.data() {
		if m.Retirement != "" {
			raw, _ := json.Marshal(m)
			marked = bytes.Contains(raw, []byte(`"identityBarrier":true`))
		}
	}
	if !marked {
		t.Fatal("post-upgrade scale-in lacks identity barrier")
	}
	day2Ticks(t, rc, b, func(s *state) bool { return len(s.data()) == 3 })
	s, _ = load(rc.Cluster)
	lost := s.data()[2]
	for _, m := range s.data() {
		if !onlinePrimary(b.native[m.Name]) {
			lost = m
			break
		}
	}
	lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
	lost.LastGTIDExecuted = recoverySID + ":1-10"
	for name, st := range b.native {
		st.GTIDExecuted = recoverySID + ":1-20"
		b.native[name] = st
	}
	delete(b.native, lost.Name)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	p.options.AutoReplaceLostVolumes = true
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	queued, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref(), domain.PhasePending)
	if err != nil {
		t.Fatal(err)
	}
	var replacement *domain.Operation
	for i := range queued {
		if queued[i].Kind == domain.OperationReplaceMember {
			replacement = &queued[i]
		}
	}
	if replacement == nil {
		t.Fatal("post-upgrade automatic recovery did not queue replacement")
	}
	if _, err := p.RunOperation(t.Context(), rc, replacement); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	raw, _ = json.Marshal(s.Replacement)
	if !bytes.Contains(raw, []byte(`"identityBarrier":true`)) {
		t.Fatal("post-upgrade automatic replacement lacks identity barrier")
	}
}

// Erase the additive marker as a released control plane's persisted JSON would.
func removeIdentityMarkers(t *testing.T, rc *engine.Context) {
	t.Helper()
	var root any
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	var clear func(any)
	clear = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			delete(x, "identityBarrier")
			for _, child := range x {
				clear(child)
			}
		case []any:
			for _, child := range x {
				clear(child)
			}
		}
	}
	clear(root)
	rc.Cluster.Internal, _ = json.Marshal(root)
}

func TestGRLegacyInFlightCompletion(t *testing.T) {
	for _, mode := range []string{"retire", "cancel-planned", "cancel-joined", "reconstructed-retirement", "marked-capability-lost"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			events := &placementSafetyEvents{}
			rc.Events = events
			op := &domain.Operation{ID: "in-flight", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom2"}`)}
			for tick := range 240 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				s, _ := load(rc.Cluster)
				if mode == "cancel-planned" || s.Replacement.Stage == "retire" {
					break
				}
				if _, err = p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				if tick == 239 {
					t.Fatal("replacement never reached retirement")
				}
			}
			if mode != "marked-capability-lost" {
				removeIdentityMarkers(t, rc)
			}
			if strings.HasPrefix(mode, "cancel-") {
				if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "reconstructed-retirement" {
				s, _ := load(rc.Cluster)
				target := s.find(s.Replacement.Old)
				target.Retirement = "handoff"
				s.Replacement = nil
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			b.action = legacyGRStatus(b, false)
			if mode == "marked-capability-lost" {
				blocked := false
				for range 20 {
					p = New(Options{})
					_, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						if !strings.Contains(err.Error(), "full GR membership view capability") {
							t.Fatal(err)
						}
						blocked = true
						break
					}
					b.apply(t, p, rc)
				}
				if !blocked || len(b.stopped) != 0 || len(b.deleted) != 0 {
					t.Fatal("marked work bypassed capability loss")
				}
				return
			}
			day2Ticks(t, rc, b, func(s *state) bool {
				if mode == "reconstructed-retirement" {
					return s.find("demom2") == nil
				}
				return s.Replacement == nil
			})
			found := false
			for _, event := range events.events {
				if event.Type == "Warning" && event.Reason == "LegacyGRRetirement" {
					found = true
				}
			}
			if !found {
				t.Fatal("legacy completion did not record warning")
			}
			s, _ := load(rc.Cluster)
			if strings.HasPrefix(mode, "cancel-") && (s.find("demom2") == nil || s.find("demom3") != nil) {
				t.Fatal("legacy cancellation retired predecessor")
			}
		})
	}
}
