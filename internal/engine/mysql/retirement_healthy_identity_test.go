// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestHealthyRetirementIdentityBarrier(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	old := s.data()[2]
	old.Retirement = "handoff"
	old.IdentityBarrier = true
	b.membership = map[string]*myspec.GroupView{}
	for _, observer := range s.data() {
		if observer == old {
			continue
		}
		view := &myspec.GroupView{}
		for _, peer := range s.data() {
			st := b.native[peer.Name]
			row := myspec.GroupViewMember{ID: st.ServerUUID, Host: peer.Name, Port: 3306, State: "ONLINE", Role: st.MemberRole}
			if peer == old {
				row.State = "UNREACHABLE"
			}
			view.Members = append(view.Members, row)
		}
		b.membership[observer.Name] = view
	}
	blocked := false
	for range 20 {
		done, err := p.retireMember(t.Context(), rc, s, old)
		if err != nil {
			if !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal(err)
			}
			if old.Retirement != "leave" || done || len(b.stopped) != 0 || len(b.deleted) != 0 {
				t.Fatalf("healthy barrier released backend stop: stage=%s err=%v", old.Retirement, err)
			}
			if b.native[old.Name].MemberState != "OFFLINE" {
				t.Fatal("healthy native leave was not requested")
			}
			blocked = true
			break
		}
		for _, m := range s.Members {
			b.applied[m.Name] = s.ConfigVersion
		}
	}
	if !blocked {
		t.Fatal("healthy retirement never reached identity wait")
	}
	clear(b.membership)
	for range 30 {
		done, err := p.retireMember(t.Context(), rc, s, old)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
		for _, m := range s.Members {
			b.applied[m.Name] = s.ConfigVersion
		}
	}
	t.Fatal("converged healthy retirement did not complete")
}

func TestRetirementViewIdentifiersOptional(t *testing.T) {
	_, rc, b := setup(t, "group")
	p := New(Options{})
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	old := s.data()[2]
	delete(b.native, old.Name)
	b.membership = map[string]*myspec.GroupView{}
	for i, observer := range s.data()[:2] {
		view := &myspec.GroupView{ViewID: "view:2"}
		if i == 1 {
			view.ViewID = ""
		}
		for _, peer := range s.data()[:2] {
			native := b.native[peer.Name]
			view.Members = append(view.Members, myspec.GroupViewMember{ID: native.ServerUUID, Host: peer.Name, Port: 3306, State: "ONLINE", Role: native.MemberRole})
		}
		b.membership[observer.Name] = view
	}
	if err := membershipRemoved(t.Context(), rc, s, old.Name); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementCancellationNeverDeployedProof(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	op := &domain.Operation{ID: "cancel-allocation", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom0"}`)}
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*state, *member)
	}{
		{"untouched allocation", nil},
		{"not replacement rollback", func(s *state, m *member) { s.Replacement.Recovery = "" }},
		{"runtime identity", func(s *state, m *member) { m.ServiceID = "runtime" }},
		{"volume identity", func(s *state, m *member) { m.VolumeID = "volume" }},
		{"resource settings", func(s *state, m *member) { m.Resources.MinRAM = 1 }},
		{"import intent", func(s *state, m *member) { m.ImportBatch = &engine.ImportPlan{} }},
		{"deploy receipt", func(s *state, m *member) { m.DeployOp = &infra.Op{ID: "deploy"} }},
		{"pending deployment", func(s *state, m *member) { s.Pending = &pending{Member: m.Name, Action: "deploy"} }},
		{"imported receipt lost", func(s *state, m *member) { m.Stage = "imported" }},
		{"deploying receipt lost", func(s *state, m *member) { m.Stage = "deploying" }},
		{"deployed receipt lost", func(s *state, m *member) { m.Stage = "deployed" }},
		{"joined receipt lost", func(s *state, m *member) { m.Stage = "joined" }},
		{"runtime deployed", func(s *state, m *member) { m.RuntimeRecovery.Deployed = true }},
		{"runtime import", func(s *state, m *member) { m.RuntimeRecovery.ImportPlan = &engine.ImportPlan{} }},
		{"initialized", func(s *state, m *member) { m.Initialized = true }},
		{"native readiness", func(s *state, m *member) { ready := true; m.NativeReadiness = &ready }},
		{"malformed UUID", func(s *state, m *member) { m.ServerUUID = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Re-load durable state: absence of process-local receipts after a
			// control-plane restart must not broaden the allocation exception.
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			next := s.find(s.Replacement.New)
			if tc.change != nil {
				tc.change(s, next)
			}
			next.Removing, next.Offline, next.Retirement = true, true, "credentials"
			pristine := tc.change == nil
			if got := replacementNeverDeployed(s, next); got != pristine {
				t.Fatalf("never-deployed proof=%v, want %v", got, pristine)
			}
			err = membershipRemoved(t.Context(), rc, s, next.Name)
			if pristine && err != nil || !pristine && !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("native barrier: %v (untouched=%v)", err, pristine)
			}
		})
	}
	s, _ := load(rc.Cluster)
	next := s.find(s.Replacement.New)
	next.Removing, next.Offline, next.Retirement = true, true, "credentials"
	b.membership = map[string]*myspec.GroupView{s.data()[0].Name: nil}
	if _, err := p.retireMember(t.Context(), rc, s, next); !errors.Is(err, engine.ErrUnavailable) || s.find(next.Name) == nil {
		t.Fatalf("untouched allocation bypassed survivor proof: %v", err)
	}
}
