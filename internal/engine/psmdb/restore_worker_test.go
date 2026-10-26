// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestRestoreWorkerFailureRequiresStaleNativeProgressAndReplacedLockOwner(t *testing.T) {
	const ts = int64(1792626275)
	for _, scenario := range []string{"dead-owner", "skewed-paused-original", "first-observation", "legacy-agent", "different-supervisor", "fresh-heartbeat", "fresh-progress", "no-heartbeat", "no-progress", "terminal-success", "terminal-failure", "no-lock", "unmapped-owner", "agent-unreachable", "mongo-unreachable"} {
		t.Run(scenario, func(t *testing.T) {
			id := bson.NewObjectID()
			heartbeat, transition := ts, ts-2
			status := "running"
			started := time.Unix(ts+3, 0)
			owner := agentapi.ProcessStatus{State: "running", PID: 7635, StartedAt: &started, SupervisorSession: "same-session", Launch: 2}
			workers := map[string]pbm.RestoreWorkerIdentity{id.Hex() + "/member0:27017": {Session: "same-session", Launch: 1}}
			var observationErr error
			switch scenario {
			case "skewed-paused-original":
				// Owner clock is minutes ahead of the leader's wall/cluster
				// clocks, but the original process is paused and still alive.
				started = time.Unix(ts+180, 0)
				owner.State, owner.Launch = "paused", 1
			case "first-observation":
				clear(workers)
			case "legacy-agent":
				owner.SupervisorSession, owner.Launch = "", 0
			case "different-supervisor":
				owner.SupervisorSession = "new-session"
			case "fresh-heartbeat":
				heartbeat = ts + 119
			case "fresh-progress":
				transition = ts + 119
			case "no-heartbeat":
				heartbeat = 0
			case "no-progress":
				transition = 0
			case "terminal-success":
				status = "done"
			case "terminal-failure":
				status = "error"
			case "unmapped-owner":
				owner = agentapi.ProcessStatus{}
			case "agent-unreachable":
				observationErr = errors.New("agent unreachable")
			}
			locks := []bson.D{{{Key: "node", Value: "member0:27017"}, {Key: "hb", Value: bson.Timestamp{T: uint32(heartbeat)}}}}
			if scenario == "no-lock" {
				locks = nil
			}
			lockFilterChecked := false
			opts := options.Client().SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				if coll, _ := e.Command.Lookup("find").StringValueOK(); coll == "pbmLock" {
					filter := e.Command.Lookup("filter").Document()
					if filter.Lookup("opid").StringValue() != id.Hex() || filter.Lookup("type").StringValue() != "restore" {
						t.Error("lock query was not bound to this restore receipt")
					}
					lockFilterChecked = true
				}
			}})
			receipt := absenceCursor("pbmCmd", bson.D{{Key: "_id", Value: id}})
			if scenario == "mongo-unreachable" {
				receipt = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "permission denied"}}
			}
			client := absenceWireClient(t, opts, receipt,
				absenceCursor("pbmRestores", bson.D{{Key: "status", Value: status}, {Key: "last_transition_ts", Value: transition}}),
				bson.D{{Key: "ok", Value: 1}, {Key: "localTime", Value: time.Unix(ts+120, 0)}},
				absenceCursor("pbmLock", locks...),
			)
			reason, unconfirmed, err := observeRestoreWorkerFailure(t.Context(), client, "native", workers, func(_ context.Context, host string) (agentapi.ProcessStatus, error) {
				if host != "member0" {
					t.Fatal(host)
				}
				return owner, observationErr
			})
			if (err != nil) != (scenario == "agent-unreachable" || scenario == "mongo-unreachable") {
				t.Fatal(reason, err)
			}
			if (reason != "") != (scenario == "dead-owner") {
				t.Fatal("unproven worker death classified", reason, err)
			}
			if scenario == "dead-owner" && (!lockFilterChecked || !strings.Contains(reason, "2m0s") || !strings.Contains(reason, "memory limits")) {
				t.Fatal(reason)
			}
			if scenario == "legacy-agent" || scenario == "first-observation" || scenario == "skewed-paused-original" {
				if !unconfirmed {
					t.Fatal("stale ambiguous worker has no operator warning")
				}
			}
		})
	}
}

func TestRestoreWorkerObserverUsesActivePITRIdentityAfterBackupPasswordReplaced(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
	s.Sets["rs0"].Stage = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	identity := &restorePBMIdentity{User: "restore_isolated", Secret: "restores/operation/pbm", Stage: "active"}
	if err := putSecret(t.Context(), rc, identity.Secret, []byte("target-only-password")); err != nil {
		t.Fatal(err)
	}
	if err := setInternal(rc.Cluster, "pbmRestoreIdentity", identity); err != nil {
		t.Fatal(err)
	}
	native := nativeHealingFixture(t, rc, s)
	client, err := scram.SHA256.NewClient(identity.User, "target-only-password", "")
	if err != nil {
		t.Fatal(err)
	}
	verifier := client.GetStoredCredentials(scram.KeyFactors{Salt: "identity-fixture", Iters: 4096})
	native.auth, err = scram.SHA256.NewServer(func(name string) (scram.StoredCredentials, error) {
		if name != identity.User {
			return scram.StoredCredentials{}, fmt.Errorf("ordinary backup password no longer authenticates")
		}
		return verifier, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	observations := 0
	native.command = func(command bson.Raw) bson.M {
		if command.Lookup("find").Type != 0 {
			observations++
			return bson.M{"ok": 1, "cursor": bson.M{"id": int64(0), "ns": "admin.pbmCmd", "firstBatch": bson.A{}}}
		}
		return bson.M{"ok": 1}
	}
	reason, unconfirmed, err := (hooks{p: p, rc: rc}).ObserveRestoreWorkerFailure(t.Context(), "native", pbm.Env{RC: rc}, map[string]pbm.RestoreWorkerIdentity{})
	if err != nil || reason != "" || unconfirmed || observations != 1 {
		t.Fatal("restore-scoped observation did not survive backup password replacement", reason, err, observations)
	}
}
