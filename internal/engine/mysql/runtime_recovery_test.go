// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const recoverySID = "11111111-2222-3333-4444-555555555555"

func recoveryFixture(t *testing.T) (*Core, *engine.Context, *fakeBackend, *state, *member) {
	t.Helper()
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	lost := s.data()[2]
	lost.LastGTIDExecuted = recoverySID + ":1-10"
	for _, m := range s.data() {
		st := b.native[m.Name]
		st.GroupName = s.Formation.GroupName
		st.GTIDExecuted = recoverySID + ":1-20"
		b.native[m.Name] = st
	}
	delete(b.native, lost.Name)
	delete(b.services, lost.Volume)
	return p, rc, b, s, lost
}

func TestGRLostVolumeAdmission(t *testing.T) {
	for _, mode := range []string{"healthy", "no majority", "missing GTIDs", "unknown", "wrong group", "no donor"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b, s, lost := recoveryFixture(t)
			switch mode {
			case "no majority":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			case "missing GTIDs":
				st := b.native[s.data()[0].Name]
				st.GTIDExecuted = ""
				b.native[s.data()[0].Name] = st
			case "unknown":
				delete(b.native, s.data()[0].Name)
			case "wrong group":
				st := b.native[s.data()[0].Name]
				st.GroupName = ""
				b.native[s.data()[0].Name] = st
			case "no donor":
				lost.LastGTIDExecuted = recoverySID + ":1-30"
			}
			for range 2 {
				busy, err := p.recoverRuntimes(t.Context(), rc, s)
				if !busy || (err == nil) != (mode == "healthy") {
					t.Fatalf("busy=%v err=%v", busy, err)
				}
			}
			ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "healthy" {
				want = 1
			}
			if len(ops) != want || b.services[lost.Volume] != nil {
				t.Fatalf("operations=%d recreated=%v", len(ops), b.services[lost.Volume])
			}
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			applyMySQLRuntimeRecoveryStatus(&status, s, time.Now())
			if status.Phase != domain.PhaseDegraded || mode != "healthy" && !strings.Contains(status.Message, "refused") {
				t.Fatalf("status=%+v", status)
			}
			if mode == "healthy" {
				// A terminal deterministic operation is not silently retried.
				ops[0].Phase = domain.PhaseFailed
				if err := rc.Store.(*store.Store).UpdateOperation(t.Context(), &ops[0]); err != nil {
					t.Fatal(err)
				}
				if _, err := p.recoverRuntimes(t.Context(), rc, s); err == nil {
					t.Fatal("terminal operation requeued")
				}
			}
		})
	}
}

func TestPXCLostVolumeAdmission(t *testing.T) {
	for _, mode := range []string{"healthy", "non-Primary", "not Synced", "latched history", "no donor", "different history"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b, s, lost := recoveryFixture(t)
			rc.Cluster.Spec.Provider = "pxc"
			s.Topology = "cluster"
			s.Formation.Seed = lost.Name
			s.data()[0].GaleraSSTDonor = lost.Name
			if mode == "latched history" {
				s.GTIDIncidents = []gtidIncident{{DetectedAt: time.Now()}}
				if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "no donor" {
				lost.LastGTIDExecuted = recoverySID + ":1-30"
			}
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path != "/v1/mysql/galera-status" {
					return false
				}
				out := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: recoverySID, GTIDExecuted: b.native[name].GTIDExecuted, ClusterSize: 2}
				switch mode {
				case "non-Primary":
					out.ClusterStatus = "non-Primary"
				case "not Synced":
					out.LocalStateComment = "Donor/Desynced"
				case "different history":
					if name == s.data()[0].Name {
						out.StateUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
					}
				}
				json.NewEncoder(w).Encode(out)
				return true
			}
			// Exercise full physical-loss admission, including the existing queue helper.
			busy, err := p.recoverRuntimes(t.Context(), rc, s)
			if !busy || (err == nil) != (mode == "healthy") {
				t.Fatalf("busy=%v err=%v", busy, err)
			}
			ops, e := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
			if e != nil {
				t.Fatal(e)
			}
			want := 0
			if mode == "healthy" {
				want = 1
				if s.Formation.Seed == lost.Name || s.data()[0].GaleraSSTDonor == lost.Name {
					t.Fatal("lost SST donor retained")
				}
			}
			if len(ops) != want || b.services[lost.Volume] != nil {
				t.Fatalf("operations=%d volume=%v", len(ops), b.services[lost.Volume])
			}
		})
	}
}

func TestLostVolumeSingleAndUnreachable(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		p, rc, b := setup(t, "single")
		converge(t, p, rc, b)
		s, _ := load(rc.Cluster)
		lost := s.data()[0]
		delete(b.services, lost.Volume)
		if _, err := p.recoverRuntimes(t.Context(), rc, s); err == nil || !strings.Contains(err.Error(), "automatic empty-volume creation is refused") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		p, rc, b := setup(t, "group")
		converge(t, p, rc, b)
		s, _ := load(rc.Cluster)
		delete(b.native, s.data()[2].Name)
		if _, err := p.recoverRuntimes(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
		ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
		if err != nil || len(ops) != 0 {
			t.Fatalf("ops=%v err=%v", ops, err)
		}
	})
}

func TestLostMemberRetirementWithoutNativeObservation(t *testing.T) {
	p, rc, b, s, lost := recoveryFixture(t)
	lost.RuntimeRecovery.VolumeLost = true
	lost.Retirement = "handoff"
	// No old-member native action or observation is permitted.
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if name == lost.Name && strings.HasPrefix(r.URL.Path, "/v1/mysql/") {
			t.Errorf("contacted lost member: %s", r.URL.Path)
			http.Error(w, "lost", 503)
			return true
		}
		return false
	}
	for range 30 {
		done, err := p.retireMember(t.Context(), rc, s, lost)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if b.services[lost.Name] != nil || s.find(lost.Name) != nil {
				t.Fatal("lost runtime retained")
			}
			return
		}
		// Proxy acknowledges removal from desired routing before runtime fencing.
		for _, m := range s.Members {
			if m.Role == "proxy" {
				m.ConfigVersion = s.ConfigVersion
				b.applied[m.Name] = s.ConfigVersion
			}
		}
	}
	t.Fatal("retirement failed to complete")
}
