// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestOperationsResumeAfterCheckpointCrash(t *testing.T) {
	for _, test := range []struct{ kind, user string }{
		{kind: domain.OperationRestart},
		{kind: domain.OperationReplaceMember},
		{kind: domain.OperationRotateSystemUser, user: "admin"},
		{kind: domain.OperationRotateSystemUser, user: "karakoram"},
		{kind: domain.OperationRotateTLS},
	} {
		t.Run(test.kind+"/"+test.user, func(t *testing.T) {
			kind := test.kind
			rotationUser := test.user
			if rotationUser == "" {
				rotationUser = "admin"
			}
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			ctx := context.Background()
			st := rc.Store.(*store.Store)
			a.log.clear()
			password, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "users/"+rotationUser)
			if err != nil {
				t.Fatal(err)
			}
			certificate, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/members/demom0.pem")
			if err != nil {
				t.Fatal(err)
			}
			payload := json.RawMessage(`{}`)
			if kind == domain.OperationReplaceMember {
				payload = json.RawMessage(`{"member":"demom1"}`)
			}
			if kind == domain.OperationRotateSystemUser {
				payload = json.RawMessage(`{"user":"` + rotationUser + `"}`)
			}
			op := &domain.Operation{ID: "resume-" + kind, Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: kind, Payload: payload, Phase: domain.PhaseRunning}
			if err := st.EnqueueOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			complete := false
			checkpoints := 0
			for range 250 {
				result, err := p.RunOperation(ctx, rc, op)
				if err != nil {
					t.Fatalf("operation %s: %v state=%s checkpoint=%s", kind, err, rc.Cluster.Internal, op.Checkpoint)
				}
				if len(result.Checkpoint) == 0 {
					t.Fatal("operation did not return durable checkpoint")
				}
				op.Checkpoint = result.Checkpoint
				op.Step = result.Step
				if err = st.UpdateOperationAndClusterState(ctx, op, rc.Cluster); err != nil {
					t.Fatal(err)
				}
				// Reconstruct provider and cluster from sealed SQLite after every checkpoint.
				ref := rc.Cluster.Ref()
				rc.Cluster, err = st.GetCluster(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				op, err = st.GetOperation(ctx, op.ID)
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Store: st, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
				a.p = p
				checkpoints++
				if result.Done {
					complete = true
					break
				}
				if policy, ok := any(p).(engine.OperationSyncPolicy); ok && policy.SyncDuringOperation(op) {
					if _, err = p.Sync(ctx, rc); err != nil {
						t.Fatal(err)
					}
					if err = st.UpdateClusterState(ctx, rc.Cluster); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !complete || checkpoints < 2 {
				t.Fatalf("operation did not resume to completion: %s", op.Checkpoint)
			}
			calls := a.log.snapshot()
			switch kind {
			case domain.OperationRestart:
				for _, name := range []string{"demom0", "demom1", "demom2"} {
					if countCall(calls, "infra:"+name+":restart") != 1 {
						t.Fatalf("restart replayed or skipped: %v", calls)
					}
					assertBefore(t, calls, "agent:"+name+":stop", "infra:"+name+":restart")
				}
			case domain.OperationReplaceMember:
				s := state(t, rc)
				if s.find("demom1") != nil || s.find("demom3") == nil || len(s.Members) != 3 || f.services["demom1"] != nil || f.services["demom1v"] != nil {
					t.Fatalf("replacement state: %+v", s)
				}
				assertBefore(t, calls, "agent:demom3:join", "agent:demom1:drain")
				assertBefore(t, calls, "agent:demom1:stop", "infra:demom1:delete")
			case domain.OperationRotateSystemUser:
				changed, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "users/"+rotationUser)
				if err != nil || string(changed) == string(password) {
					t.Fatal("system password did not rotate", err)
				}
				n := 0
				for _, call := range calls {
					if strings.HasSuffix(call, ":set-password") {
						n++
					}
				}
				if n != 1 {
					t.Fatalf("password rotation replayed or skipped: %v", calls)
				}
			case domain.OperationRotateTLS:
				changed, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/members/demom0.pem")
				if err != nil || string(changed) == string(certificate) {
					t.Fatal("member certificate did not rotate", err)
				}
				for _, name := range []string{"demom0", "demom1", "demom2"} {
					if countCall(calls, "infra:"+name+":deploy") != 1 {
						t.Fatalf("TLS rolling deploy replayed or skipped: %v", calls)
					}
					assertBefore(t, calls, "agent:"+name+":stop", "infra:"+name+":deploy")
				}
			}
		})
	}
}

func TestOrphanHandoffRequiresBoundedMountedFailureHistory(t *testing.T) {
	for _, variant := range []string{"eligible", "too few failures", "too young", "other error", "runtime present", "foreign volume"} {
		t.Run(variant, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			ctx := context.Background()
			s := state(t, rc)
			s.UID = "orphan"
			m := s.add(rc.Cluster.Name)
			m.ServiceID = m.Name
			m.VolumeID = m.Volume
			m.Stage = "ready"
			m.Removing = true
			f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume}
			clock := rc.Clock.(*testClock)
			v := &pending{Action: "cleanup-volume", Member: m.Name}
			r := &platformRetry{Failures: 5, StillMountedFailures: 5, FirstFailedAt: clock.at.Add(-30 * time.Minute), FirstError: "still mounted", Message: "Local Storage STILL MOUNTED", LastProcessID: "failed-delete", RetryAfter: clock.at.Add(time.Minute)}
			switch variant {
			case "too few failures":
				r.StillMountedFailures = 4
			case "too young":
				r.FirstFailedAt = clock.at.Add(-29 * time.Minute)
			case "other error":
				r.Message = "permission denied"
			case "runtime present":
				f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name}
			case "foreign volume":
				f.services[m.Volume].ID = "foreign"
			}
			if err := p.ensureSecrets(ctx, rc, s); err != nil {
				t.Fatal(err)
			}
			s.Retry = v
			s.PlatformRetries = map[string]*platformRetry{platformRetryKey(v): r}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			handed, err := p.orphanVolume(ctx, rc, s, v, r)
			if variant == "foreign volume" {
				if !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("foreign handoff: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if handed != (variant == "eligible") {
				t.Fatalf("handoff=%v variant=%s", handed, variant)
			}
			orphans, err := rc.Store.ListOrphanedVolumes(ctx, rc.Cluster.Ref().Namespace)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "eligible" {
				if len(orphans) != 1 || orphans[0].Hostname != m.Volume || orphans[0].ServiceID != m.Volume {
					t.Fatal(orphans)
				}
				if f.services[m.Volume] == nil {
					t.Fatal("handoff deleted volume instead of transferring ownership")
				}
				// Even a stale restored allocation counter must not reuse janitor-owned names.
				s.NextIndex = 0
				fresh, err := allocateNewMember(ctx, rc, s)
				if err != nil {
					t.Fatal(err)
				}
				if fresh.Name == m.Name || fresh.Volume == m.Volume {
					t.Fatal("reused orphan identity")
				}
			} else if len(orphans) != 0 {
				t.Fatal("premature orphan record", orphans)
			}
		})
	}
}

func TestSyncMinorityOutageAllowsQueuedReplacement(t *testing.T) {
	for _, lost := range []string{"demom0", "demom1"} {
		t.Run(lost, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			ctx := t.Context()
			st := rc.Store.(*store.Store)
			a.broker(lost).down = true
			op := &domain.Operation{ID: "queued-lost-replace", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost + `"}`)}
			if err := st.EnqueueOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			// Match the reconciler's Sync/Status-before-claim ordering.
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal("minority loss blocked queued recovery", err)
			}
			status, err := p.Status(ctx, rc)
			if err != nil || status.Phase != domain.PhaseDegraded {
				t.Fatalf("status=%+v err=%v", status, err)
			}
			for _, m := range status.Members {
				if m.Name == lost && (m.Health || m.Agent.Healthy || m.Agent.Message == "") {
					t.Fatalf("lost member not reported unhealthy: %+v", m)
				}
			}
			if err := st.UpdateClusterState(ctx, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			claimed, err := st.ClaimOperation(ctx, rc.Cluster.Ref())
			if err != nil || claimed.ID != op.ID || claimed.Phase != domain.PhaseRunning {
				t.Fatalf("claimed=%+v err=%v", claimed, err)
			}
			result, err := p.RunOperation(ctx, rc, claimed)
			if err != nil || result.Step != "replacement-planned" {
				t.Fatalf("replacement did not start: %+v %v", result, err)
			}
		})
	}
}

func TestSyncMajorityUnreachableStillReturnsError(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.broker("demom0").down = true
	a.broker("demom1").down = true
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("majority outage was tolerated")
	}
}

func TestRollingOperationsFinishProvisioningBeforeChangingMembers(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS} {
		for _, scenario := range []string{"creation/imported", "creation/deployed", "scale-up/imported", "scale-up/deployed"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				p, rc, _, a := setup(t)
				target, last := "demom0", "demom2"
				if strings.HasPrefix(scenario, "scale-up") {
					ready(t, p, rc)
					rc.Cluster.Spec.Components.Engine.Replicas = 5
					target, last = "demom3", "demom4"
				}
				stage := "imported"
				if strings.HasSuffix(scenario, "deployed") {
					stage = "deployed"
				}
				syncUntil(t, p, rc, func(s *internalState) bool {
					m := s.find(target)
					return m != nil && m.Stage == stage && s.Pending == nil
				})
				a.log.clear()
				op := &domain.Operation{Kind: kind}
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil || result.Done || result.Step != "waiting-provisioning" {
					t.Fatalf("out=%+v err=%v", result, err)
				}
				op.Checkpoint = result.Checkpoint
				if !p.SyncDuringOperation(op) {
					t.Fatal("rolling operation excluded required provisioning Sync")
				}
				for _, m := range state(t, rc).Members {
					if m.ChangeAction != "" {
						t.Fatalf("rolling change began before provisioning: %+v", m)
					}
				}
				done := false
				for range 300 {
					if p.SyncDuringOperation(op) {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
					result, err = p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatalf("rolling operation: %v checkpoint=%s", err, op.Checkpoint)
					}
					op.Checkpoint = result.Checkpoint
					if result.Done {
						done = true
						break
					}
					var cp operationState
					if err := decodeJSON(op.Checkpoint, &cp); err != nil {
						t.Fatal(err)
					}
					if cp.Waiting != "" && p.SyncDuringOperation(op) {
						t.Fatal("rolling change allowed ordinary Sync")
					}
				}
				if !done || len(state(t, rc).runningMembers()) != rc.Cluster.Spec.Components.Engine.Replicas {
					t.Fatalf("rolling operation did not converge: %s", op.Checkpoint)
				}
				assertBefore(t, a.log.snapshot(), "agent:"+last+":join", "agent:demom0:drain")
			})
		}
	}
}
