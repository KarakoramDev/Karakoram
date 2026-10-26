// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRestoreWorkerCheckpointRoundTripAndLegacyWarningClearsOnProgress(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "checkpoint-replacement"
		if legacy {
			name = "legacy-warning"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
			s.Phase, s.Sets["rs0"].Stage = "restore", "ready"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded, PBMName: "snapshot"}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "restore-operation", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"recover"}`)}
			op.Checkpoint, _ = json.Marshal(map[string]string{"step": "poll", "member": s.Members[0].Name, "pbmName": "native", "storage": "archive"})
			if err := rc.Store.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: rc.Cluster.Name, OperationID: op.ID, Source: domain.RestoreSource{Backup: "base"}, Phase: domain.PhaseRunning}); err != nil {
				t.Fatal(err)
			}
			const ts = int64(1792626275)
			nativeNow, heartbeat, transition := ts+5, ts, ts
			nativeID := bson.NewObjectID()
			native := nativeHealingFixture(t, rc, s)
			native.hello = bson.M{"ok": 1, "localTime": time.Unix(nativeNow, 0)}
			native.command = func(command bson.Raw) bson.M {
				collection, ok := command.Lookup("find").StringValueOK()
				if !ok {
					return bson.M{"ok": 1}
				}
				var document bson.M
				switch collection {
				case "pbmCmd":
					document = bson.M{"_id": nativeID}
				case "pbmRestores":
					document = bson.M{"status": "running", "last_transition_ts": transition}
				case "pbmLock":
					document = bson.M{"node": s.Members[0].Name + ":27017", "hb": bson.Timestamp{T: uint32(heartbeat)}}
				}
				return bson.M{"ok": 1, "cursor": bson.M{"id": int64(0), "ns": "admin." + collection, "firstBatch": bson.A{document}}}
			}
			process := agentapi.ProcessStatus{State: "running", SupervisorSession: "original-supervisor", Launch: 1}
			if legacy {
				process.SupervisorSession, process.Launch = "", 0
			}
			initiations := 0
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"pbm-agent": process}})
					return
				}
				var request agentapi.PBMRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				if request.Command == "restore" {
					initiations++
				}
				_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(`{"status":"running"}`)})
			})
			first, err := p.RunOperation(t.Context(), rc, op)
			if err != nil || first.Done {
				t.Fatal(first, err)
			}
			if !legacy && !strings.Contains(string(first.Checkpoint), "original-supervisor") {
				t.Fatal("first native observation did not checkpoint launch identity", string(first.Checkpoint))
			}
			op.Checkpoint = first.Checkpoint
			// Lose both in-memory provider objects, exactly as a CP restart does.
			raw, _ := json.Marshal(op)
			var reopened domain.Operation
			if err := json.Unmarshal(raw, &reopened); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			reopenedCluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster, p = reopenedCluster, New(p.options)
			nativeNow = ts + 121
			native.hello["localTime"] = time.Unix(nativeNow, 0)
			if !legacy {
				process.Launch++
			}
			second, err := p.RunOperation(t.Context(), rc, &reopened)
			if !legacy {
				if err == nil || !second.Done || !strings.Contains(err.Error(), "observed launch 1") {
					t.Fatal("persisted baseline did not classify replacement", second, err)
				}
			} else {
				if err != nil || second.Done || !strings.Contains(second.Message, "RestoreWorkerUnconfirmed") {
					t.Fatal("legacy stale worker was terminalized or hidden", second, err)
				}
				if !strings.Contains(second.Message, "PBMMemoryBudgetUnknown") {
					t.Fatal("legacy worker warning hid its unknown memory budget", second.Message)
				}
				status, _ := p.Status(t.Context(), rc)
				found := false
				for _, condition := range status.Conditions {
					found = found || condition.Reason == "RestoreWorkerUnconfirmed"
				}
				if !found {
					t.Fatal("cluster observation dropped worker warning", status.Conditions)
				}
				heartbeat, transition = nativeNow, nativeNow
				reopened.Checkpoint = second.Checkpoint
				third, err := p.RunOperation(t.Context(), rc, &reopened)
				if err != nil || third.Done || strings.Contains(third.Message, "RestoreWorkerUnconfirmed") {
					t.Fatal("fresh native progress did not clear warning", third, err)
				}
				reloaded, err := load(rc.Cluster)
				if err != nil || reloaded.RestoreWorkerUnconfirmed {
					t.Fatal("worker warning survived fresh progress", reloaded, err)
				}
			}
			if initiations != 0 {
				t.Fatal("observation replayed native restore", initiations)
			}
		})
	}
}
