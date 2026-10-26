// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type survivorDialer struct {
	testDialer
	attempted []string
}

func (d *survivorDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	d.attempted = append(d.attempted, member)
	if member == "m0" {
		return nil, errors.New("member unavailable")
	}
	return d.testDialer.Dial(ctx, ref, member)
}

func TestPBMObservationUsesSurvivorButStartRemainsPinned(t *testing.T) {
	env, _, fake, _ := setup(t)
	d := &survivorDialer{testDialer: env.RC.Agents.(testDialer)}
	env.RC.Agents = d
	for _, name := range []string{"describe-backup", "describe-restore", "config"} {
		if _, err := command(t.Context(), env, "m0", "", name, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := command(t.Context(), env, "m0", "start-op", "restore", nil); err == nil {
		t.Fatal("ambiguous initiation migrated to another member")
	}
	if fake.startCount() != 0 {
		t.Fatal("initiated restore through survivor")
	}
	env.Sharded = true
	env.Nodes[0].ReplSet = "cfg"
	if _, err := command(t.Context(), env, "m0", "", "describe-backup", nil); err == nil {
		t.Fatal("sharded command escaped config set")
	}
}

func TestRestorePreservesBalancerAcrossLostCheckpoint(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			env, st, _, h := setup(t)
			env.Sharded = true
			h.enabled = enabled
			if err := st.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: "target"}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "restore-op", Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: json.RawMessage(`{"step":"balancer-off"}`)}
			res, err := RunRestore(t.Context(), env, op)
			if err != nil {
				t.Fatal(err)
			}
			if len(h.balancers) != 0 {
				t.Fatal("balancer mutated before original state persisted")
			}
			op.Checkpoint = res.Checkpoint
			// Lose the result after mutation: replay must retain the captured state.
			if _, err = RunRestore(t.Context(), env, op); err != nil {
				t.Fatal(err)
			}
			res, err = RunRestore(t.Context(), env, op)
			if err != nil {
				t.Fatal(err)
			}
			var cp checkpoint
			if err = json.Unmarshal(res.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			cp.Step = "balancer-on"
			op.Checkpoint, _ = json.Marshal(cp)
			if _, err = RunRestore(t.Context(), env, op); err != nil {
				t.Fatal(err)
			}
			if h.enabled != enabled {
				t.Fatalf("balancer enabled=%v want=%v", h.enabled, enabled)
			}
		})
	}
}

func TestRecordedNativeInitiationResumesWithoutReceiptOwner(t *testing.T) {
	for _, kind := range []string{"backup", "restore"} {
		t.Run(kind, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			env.RC.Agents = &survivorDialer{testDialer: env.RC.Agents.(testDialer)}
			cp := checkpoint{Step: "start", Member: "m0", PBMName: "recorded-native-operation", Snapshot: "base"}
			raw, _ := json.Marshal(cp)
			op := &domain.Operation{ID: "operation", Checkpoint: raw}
			var err error
			var resCheckpoint json.RawMessage
			if kind == "backup" {
				if err = st.CreateBackup(t.Context(), &domain.Backup{Name: "work", Cluster: "target", Type: "logical"}); err != nil {
					t.Fatal(err)
				}
				op.Payload = json.RawMessage(`{"backup":"work"}`)
				res, e := RunBackup(t.Context(), env, op)
				err = e
				resCheckpoint = res.Checkpoint
			} else {
				if err = st.CreateRestore(t.Context(), &domain.Restore{Name: "work", Cluster: "target"}); err != nil {
					t.Fatal(err)
				}
				op.Payload = json.RawMessage(`{"restore":"work"}`)
				res, e := RunRestore(t.Context(), env, op)
				err = e
				resCheckpoint = res.Checkpoint
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(resCheckpoint, &cp); err != nil {
				t.Fatal(err)
			}
			starts := fake.startCount()
			if cp.Step != "poll" || cp.PBMName != "recorded-native-operation" || starts != 0 {
				t.Fatalf("native receipt not adopted safely: %s starts=%d", resCheckpoint, starts)
			}
		})
	}
}

func TestUnknownNativeReceiptStaysFencedAndNamesRecoveryAction(t *testing.T) {
	for _, kind := range []string{"backup", "restore"} {
		t.Run(kind, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			fake.unknownReceipt = true
			cp := checkpoint{Step: "start", Member: "m0", Snapshot: "base", Physical: true}
			raw, _ := json.Marshal(cp)
			op := &domain.Operation{ID: "operation", Checkpoint: raw}
			run := RunRestore
			if kind == "backup" {
				run = RunBackup
				op.Payload = json.RawMessage(`{"backup":"work"}`)
				if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "work", Cluster: "target", Type: "logical"}); err != nil {
					t.Fatal(err)
				}
			} else {
				op.Payload = json.RawMessage(`{"restore":"work"}`)
				if err := st.CreateRestore(t.Context(), &domain.Restore{Name: "work", Cluster: "target"}); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				res, err := run(t.Context(), env, op)
				if err == nil || !strings.Contains(err.Error(), "PBMReceiptUnknown") || !strings.Contains(err.Error(), "member m0") || !strings.Contains(err.Error(), "preserve receipts") {
					t.Fatalf("missing safe recovery diagnosis: %v", err)
				}
				if err := json.Unmarshal(res.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				if kind == "backup" {
					stored, e := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), "work")
					if e != nil || cp.Step != "done" || !res.Done || cp.PBMName != "" || stored.Phase != domain.PhaseFailed || stored.PBMName != "" {
						t.Fatalf("ambiguous backup not failed closed: %s backup=%+v err=%v", res.Checkpoint, stored, e)
					}
				} else if cp.Step != "start" || cp.PBMName != "" || res.Done {
					t.Fatalf("ambiguous restore advanced: %s", res.Checkpoint)
				}
				op.Checkpoint = res.Checkpoint
			}
			starts := fake.startCount()
			actions := fake.recordedActions()
			requests := fake.recordedRequests()
			if starts != 0 || len(actions) != 0 {
				t.Fatal("unknown restore restarted native operation or released holds")
			}
			if kind == "backup" && len(requests) != 1 {
				t.Fatalf("unknown backup initiation was replayed: %+v", requests)
			}
			for _, req := range requests {
				if req.RequestID != op.ID {
					t.Fatal("ambiguous initiation used a different receipt")
				}
			}
		})
	}
}
