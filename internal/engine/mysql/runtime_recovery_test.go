// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
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
	lost.ConfirmedVolumeLost = true
	lost.Retirement = "handoff"
	// No old-member native action or observation is permitted.
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if name == lost.Name && strings.HasPrefix(r.URL.Path, "/v1/mysql/") {
			t.Errorf("contacted lost member: %s", r.URL.Path)
			http.Error(w, "lost", http.StatusServiceUnavailable)
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

func TestRuntimeIdentityConflictDoesNotAuthorizeReplacement(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	m := s.data()[2]
	original := *b.services[m.Volume]
	foreign := original
	foreign.ID = "unverified-incarnation"
	b.services[m.Volume] = &foreign
	if _, err := p.recoverRuntimes(t.Context(), rc, s); err == nil {
		t.Fatal("volume identity conflict was hidden")
	}
	if !m.RuntimeRecovery.VolumeLost || m.ConfirmedVolumeLost {
		t.Fatal("identity conflict became confirmed physical loss")
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	m = s.find(m.Name)
	b.services[m.Volume] = &original
	b.services[m.Name].Status = infra.StatusStopped
	rc.Backend = physicallyLostBackend{fakeBackend: b}
	// A valid retained mount may resume through normal runtime recovery, but
	// a broad historical status flag must never authorize deleting its data.
	if _, err := p.recoverRuntimes(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
	if err != nil || len(ops) != 0 || m.ConfirmedVolumeLost {
		t.Fatalf("identity conflict queued replacement: ops=%v err=%v", ops, err)
	}
	if b.services[m.Volume].ID != original.ID {
		t.Fatal("retained volume changed")
	}
}

type physicallyLostBackend struct {
	*fakeBackend
	volume, volumeID string
}

func (b physicallyLostBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.fakeBackend.GetService(ctx, name)
	if name == b.volume && service != nil {
		return service, &infra.VolumeLostError{Volume: b.volume, ServiceID: b.volumeID, Message: "physical storage is absent"}
	}
	return service, err
}

func (b physicallyLostBackend) Start(_ context.Context, name string) (infra.Op, error) {
	b.services[name].Status = infra.StatusRunning
	return infra.Op{}, nil
}

func TestConfirmedPhysicalLossReplacementEndToEnd(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, "group")
			}
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			old := s.find(s.Formation.Seed)
			oldName, oldVolume, oldVolumeID := old.Name, old.Volume, old.VolumeID
			old.LastGTIDExecuted = b.native[oldName].GTIDExecuted
			for _, m := range s.data() {
				st := b.native[m.Name]
				st.GroupName = s.Formation.GroupName
				if provider == "mysql" {
					st.GTIDExecuted = recoverySID + ":1-20"
					old.LastGTIDExecuted = recoverySID + ":1-10"
				}
				st.MemberRole, st.ReadOnly, st.SuperReadOnly = "SECONDARY", true, true
				if m.Name != oldName && b.leader == oldName {
					st.MemberRole, st.ReadOnly, st.SuperReadOnly = "PRIMARY", false, false
					b.leader = m.Name
				}
				b.native[m.Name] = st
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			delete(b.native, oldName)
			delete(b.deployed, oldName)
			previous := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if name == oldName {
					http.Error(w, "lost member is unreachable", http.StatusServiceUnavailable)
					return true
				}
				return previous != nil && previous(w, r, name)
			}
			// fakeBackend advertises DiskGrowth=false. The surviving logical
			// volume returns positive physical-loss evidence, not nil inventory.
			rc.Backend = physicallyLostBackend{fakeBackend: b, volume: oldVolume, volumeID: oldVolumeID}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
			if err != nil || len(ops) != 1 {
				t.Fatalf("replacement admission: ops=%v err=%v", ops, err)
			}
			rc.Operation = &ops[0]
			finished := false
			for tick := range 240 {
				result, err := p.RunOperation(t.Context(), rc, &ops[0])
				if err != nil {
					t.Fatalf("operation tick %d: %v", tick, err)
				}
				if result.Done {
					finished = true
					break
				}
				ops[0].Checkpoint = result.Checkpoint
				if _, err := p.Sync(t.Context(), rc); err != nil {
					current, _ := load(rc.Cluster)
					t.Fatalf("sync tick %d replacement=%+v certification=%+v: %v", tick, current.Replacement, current.Formation.GTIDCertification, err)
				}
				b.apply(t, p, rc)
			}
			if !finished {
				t.Fatal("lost-volume replacement did not finish")
			}
			rc.Operation = nil
			if provider == "pxc" {
				day2Ticks(t, rc, b, func(current *state) bool {
					return !pxcFormationMappingPending(current)
				})
			}
			s, _ = load(rc.Cluster)
			if s.find(oldName) != nil || b.services[oldName] != nil || b.services[oldVolume] != nil || len(s.data()) != 3 {
				t.Fatalf("lost predecessor was not retired: %+v", s)
			}
			if provider == "pxc" && (s.Formation.Seed == oldName || pxcFormationMappingPending(s)) {
				t.Fatal("PXC donor or final-membership certification did not converge")
			}
		})
	}
}
