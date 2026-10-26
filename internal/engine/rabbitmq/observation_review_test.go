// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestManagedAccessRepairDoesNotRequireManagementAuthentication(t *testing.T) {
	for _, firstDown := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-online", true: "first-offline"}[firstDown], func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			a.log.clear()
			rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
			// Management credentials/tags drift cluster-wide. The local cookie
			// and generic agent channel remain valid and can restore access.
			for _, m := range state(t, rc).runningMembers() {
				a.failures[m.Name+":status"] = http.StatusServiceUnavailable
			}
			expected := "demom0"
			if firstDown {
				a.broker("demom0").down = true
				expected = "demom1"
			}
			a.requestHook = func(action string) {
				if action == "bootstrap" {
					for _, m := range state(t, rc).runningMembers() {
						delete(a.failures, m.Name+":status")
					}
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase == domain.PhaseReady {
				t.Fatalf("management authentication failure did not withdraw readiness: phase=%s err=%v", status.Phase, err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatalf("cookie-authenticated access repair was blocked: %v", err)
			}
			if countCall(a.log.snapshot(), "agent:"+expected+":bootstrap") != 1 {
				t.Fatalf("access repair did not use reachable member %s: %v", expected, a.log.snapshot())
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil || !firstDown && status.Phase != domain.PhaseReady || firstDown && status.Phase == domain.PhaseReady {
				t.Fatalf("repaired status did not reflect actual membership: phase=%s err=%v", status.Phase, err)
			}
		})
	}
}

func TestManagedAccessRepairDefersToStagedPasswordRotation(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.SystemUsersReconciledAt = rc.Clock.Now().Add(-time.Minute)
	s.find("demom0").ChangeAction = "password"
	a.log.clear()
	if err := p.reconcileManagedAccess(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 0 {
		t.Fatal("periodic repair interfered with staged password rotation")
	}
}

func TestManagedAccessRepairCannotBlockRollingRecovery(t *testing.T) {
	for _, stopped := range []bool{true, false} {
		t.Run(map[bool]string{true: "no-running-broker", false: "native-repair-failure"}[stopped], func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			s := state(t, rc)
			before := s.SystemUsersReconciledAt
			rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
			m := s.find("demom0")
			beginChange(rc, s, m, "restart")
			m.ChangeStep = "start"
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if stopped {
				a.broker(m.Name).running = false
			} else {
				a.failures[m.Name+":bootstrap"] = http.StatusServiceUnavailable
			}
			_, err := p.Sync(t.Context(), rc)
			if stopped && err != nil || !stopped && err == nil {
				t.Fatalf("deferred/failed repair was not reported honestly: %v", err)
			}
			s = state(t, rc)
			if s.find(m.Name).ChangeStep != "wait" || !a.broker(m.Name).running {
				t.Fatal("access repair prevented the existing rolling recovery from starting the broker")
			}
			if !s.SystemUsersReconciledAt.Equal(before) {
				t.Fatal("deferred/failed repair claimed successful reconciliation")
			}
		})
	}
}

func TestManagedAccessRepairRefreshesCommittedPasswordBeforeMutation(t *testing.T) {
	for _, user := range []string{"karakoram", "admin"} {
		t.Run(user, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			oldConfig, err := p.MemberConfig(t.Context(), rc, "demom1")
			if err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{
				ID: "repair-rotation-" + user, Kind: domain.OperationRotateSystemUser,
				Payload: []byte(`{"user":"` + user + `"}`),
			}
			for range 4 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				op.Step = result.Step
			}
			if op.Step != "password-persisted" {
				t.Fatalf("rotation did not reach committed credential: %s", op.Step)
			}
			current, err := p.MemberConfig(t.Context(), rc, "demom1")
			if err != nil {
				t.Fatal(err)
			}
			cached := oldConfig.SystemUsers[user].Password
			committed := current.SystemUsers[user].Password
			if cached == committed {
				t.Fatal("fixture did not stage a different credential")
			}
			// The rotation becomes terminal before reload-members finishes.
			// Its original owner is offline; the reachable peer still has its
			// pre-rotation config. Model bootstrap's real use of Node.Config.
			a.broker("demom0").down = true
			native := committed
			a.requestHook = func(action string) {
				switch action {
				case "/v1/reload":
					cached = committed
				case "bootstrap":
					native = cached
				}
			}
			a.log.clear()
			rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
			for pass := range 3 {
				// Publishing the peer epoch temporarily leaves only one fully
				// acknowledged broker; the offline-owner observation stays honest.
				if _, err := p.Sync(t.Context(), rc); err != nil && (pass != 0 || err.Error() != "member demom0 unavailable") {
					t.Fatal(err)
				}
				if countCall(a.log.snapshot(), "agent:demom1:bootstrap") != 0 {
					break
				}
			}
			if countCall(a.log.snapshot(), "agent:demom1:bootstrap") != 1 {
				t.Fatal("repair did not reach the stale reachable peer")
			}
			if native != committed {
				t.Fatal("access repair reverted the committed password using stale agent configuration")
			}
			assertBefore(t, a.log.snapshot(), "agent:demom1:/v1/reload", "agent:demom1:bootstrap")
		})
	}
}

func TestManagedAccessRepairReloadFailurePreventsMutation(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	before := s.SystemUsersReconciledAt
	rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
	a.failures["demom0:/v1/reload"] = http.StatusServiceUnavailable
	a.log.clear()
	if err := p.reconcileManagedAccess(t.Context(), rc, s); err == nil {
		t.Fatal("failed credential refresh was reported as successful repair")
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 0 {
		t.Fatal("repair mutated credentials after the authoritative refresh failed")
	}
	if !s.SystemUsersReconciledAt.Equal(before) || s.SystemUsersError == "" {
		t.Fatal("failed credential refresh lost its error or claimed repair success")
	}
}

func TestManagedAccessRepairRejectsDelayedPreRotationConfig(t *testing.T) {
	for _, user := range []string{"karakoram", "admin"} {
		t.Run(user, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			delayed, err := p.MemberConfig(t.Context(), rc, "demom1")
			if err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{
				ID: "delayed-config-rotation-" + user, Kind: domain.OperationRotateSystemUser,
				Payload: []byte(`{"user":"` + user + `"}`),
			}
			for range 4 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint, op.Step = result.Checkpoint, result.Step
			}
			if op.Step != "password-persisted" {
				t.Fatalf("rotation did not reach committed credential: %s", op.Step)
			}
			current, err := p.MemberConfig(t.Context(), rc, "demom1")
			if err != nil {
				t.Fatal(err)
			}
			committed := current.SystemUsers[user].Password
			if committed == delayed.SystemUsers[user].Password {
				t.Fatal("fixture did not rotate the credential")
			}
			// A periodic pull fetched delayed before rotation but has not yet
			// entered Apply. Schedule it after the repair's synchronous reload
			// and before bootstrap. Apply accepts equal versions, so refreshing
			// the password alone cannot fence this old response.
			a.broker("demom0").down = true
			cached := delayed
			native := committed
			a.requestHook = func(action string) {
				switch action {
				case "/v1/reload":
					var err error
					cached, err = p.MemberConfig(t.Context(), rc, "demom1")
					if err != nil {
						t.Error(err)
					}
				case "bootstrap":
					if delayed.ConfigVersion >= cached.ConfigVersion {
						cached = delayed
					}
					native = cached.SystemUsers[user].Password
				}
			}
			a.log.clear()
			rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
			for pass := range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil && (pass != 0 || err.Error() != "member demom0 unavailable") {
					t.Fatal(err)
				}
				// Reconstruct the CP from SQLite after each persisted step,
				// including publication of the selected peer's refresh epoch.
				st := rc.Store.(*store.Store)
				if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
					t.Fatal(err)
				}
				var err error
				rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Store: st, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
				a.p = p
				if countCall(a.log.snapshot(), "agent:demom1:bootstrap") != 0 {
					break
				}
			}
			if countCall(a.log.snapshot(), "agent:demom1:bootstrap") != 1 {
				t.Fatal("repair did not converge through the selected peer")
			}
			if native != committed {
				t.Fatal("delayed equal-version configuration reverted the committed password after refresh")
			}
		})
	}
}

func TestManagedAccessRepairWaitsForRefreshEpoch(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	before := s.SystemUsersReconciledAt
	s.ConfigVersion++
	for _, m := range s.runningMembers() {
		m.ConfigVersion = s.ConfigVersion
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
	a.requestHook = func(action string) {
		if action == "/v1/status" {
			for _, m := range s.runningMembers() {
				a.broker(m.Name).config = s.ConfigVersion - 1
			}
		}
	}
	a.log.clear()
	if err := p.reconcileManagedAccess(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 0 {
		t.Fatal("repair mutated credentials before the published refresh epoch was acknowledged")
	}
	if !s.SystemUsersReconciledAt.Equal(before) {
		t.Fatal("unacknowledged refresh claimed repair success")
	}
	a.requestHook = nil
	if err := p.reconcileManagedAccess(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 1 {
		t.Fatal("acknowledged refresh did not release native repair")
	}
}

func TestManagedAccessRepairSkipsPeerThatNeverAcknowledges(t *testing.T) {
	for _, missingEpoch := range []bool{false, true} {
		t.Run(map[bool]string{false: "stale-epoch", true: "missing-epoch"}[missingEpoch], func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			s.ConfigVersion++
			s.find("demom0").ConfigVersion = s.ConfigVersion
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
			epoch := s.ConfigVersion - 1
			if missingEpoch {
				epoch = 0
			}
			a.broker("demom0").reportedConfig = &epoch
			a.log.clear()
			for range 3 {
				s = state(t, rc)
				if err := p.reconcileManagedAccess(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				st := rc.Store.(*store.Store)
				if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
					t.Fatal(err)
				}
				var err error
				rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Store: st, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
				a.p = p
			}
			if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 0 {
				t.Fatal("repair bypassed the unacknowledged peer's epoch fence")
			}
			if countCall(a.log.snapshot(), "agent:demom1:bootstrap") != 1 {
				t.Fatal("permanently unacknowledged first peer starved repair through an acknowledging peer")
			}
			if state(t, rc).find("demom1").ConfigVersion != s.ConfigVersion {
				t.Fatal("fallback peer epoch was not durably published before repair")
			}
		})
	}
}
