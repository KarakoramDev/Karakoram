// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCompletedRecoveryAllowsAccessAndFreshJoinReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	suspendGroup(t, p, rc, b)
	day2Ticks(t, rc, b, func(s *state) bool {
		return s.Formation.State == "formed" && !s.Formation.reforming() && s.data()[2].Stage == "joined"
	})
	s, err := load(rc.Cluster)
	if err != nil || s.Formation.Reformation == nil || s.Formation.Reformation.State != "formed" {
		t.Fatal("missing completed recovery receipt", err)
	}
	status, err := New(Options{}).Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Status = status
	if err := accessFence(t.Context(), rc); err != nil {
		t.Fatal("completed recovery still fences access", err)
	}
	agents := rc.Agents
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		case "/v1/mysql/" + myspec.ActionAccessUsers:
			json.NewEncoder(w).Encode([]User{})
		default:
			t.Error("unexpected route", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	if _, err := New(Options{}).ListUsers(t.Context(), rc); err != nil {
		t.Fatal("access API unavailable after completed recovery", err)
	}
	rc.Agents = agents
	// A retained completed receipt permits new identities but is not itself
	// authorization to destructively re-clone an existing identity.
	m := s.data()[1]
	m.Initialized = false
	m.Clone = &myspec.CloneRequest{Receipt: "old-recovery-receipt"}
	for _, stage := range []string{"observing", "bootstrapping", "formed"} {
		s.Formation.Reformation.State = stage
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		cfg, err := New(Options{}).MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp myspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
			t.Fatal(err)
		}
		if sp.AllowInitialize != (stage == "formed") || sp.GroupReplication.Reclone {
			t.Fatalf("initialization/reclone fence incorrect for %s", stage)
		}
		if stage != "formed" {
			if err := accessFence(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("active recovery admitted access", err)
			}
			if err := maintenanceAllowed(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("active recovery admitted maintenance", err)
			}
		} else if sp.GroupReplication.RecloneReceipt != "" {
			t.Fatal("completed recovery leaked stale clone authorization")
		}
	}
	m.Initialized = true
	m.Clone = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	day2Ticks(t, rc, b, func(s *state) bool {
		return len(s.data()) == 5 && s.data()[4].Stage == "joined"
	})
	if b.clones["demom3"] == "" || b.clones["demom4"] == "" {
		t.Fatal("new members did not join after recovered formation")
	}
}

// assertRestoreCloneConfig checks the production renderer after reconstructing
// Core, including the interval after the seed is formed but followers are not.
func assertRestoreCloneConfig(t *testing.T, rc *engine.Context) bool {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	pendingAfterSeed := false
	for _, m := range s.data() {
		if !m.CloneReplacesData {
			continue
		}
		cfg, err := New(Options{}).MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp myspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
			t.Fatal(err)
		}
		pending := !m.Initialized && !m.CloneDone && m.Clone != nil
		if sp.AllowInitialize || sp.GroupReplication.Reclone != pending {
			t.Fatalf("restore clone authorization does not match member receipt: %+v", m)
		}
		if pending {
			if sp.GroupReplication.RecloneReceipt != m.Clone.Receipt {
				t.Fatal("restore clone authorization changed receipt")
			}
			pendingAfterSeed = pendingAfterSeed || s.Formation.State == "formed"
		} else if sp.GroupReplication.RecloneReceipt != "" {
			t.Fatal("completed member clone retained destructive authorization")
		}
	}
	return pendingAfterSeed
}
