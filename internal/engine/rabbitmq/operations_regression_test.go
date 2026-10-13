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
