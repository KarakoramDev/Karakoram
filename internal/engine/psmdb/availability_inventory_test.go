// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestOperationAvailabilityDoesNotReconcileAlternatePBMInventory(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	st := rc.Store.(*store.Store)
	s := retryState(t, rc)
	s.Phase = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = "archive"
	primary := &domain.Backup{Name: "primary-a", Cluster: rc.Cluster.Name, Storage: "archive", PBMName: "2026-10-19T12:00:00Z", PBMPrefix: "archive/" + s.PBMPrefix, Phase: domain.PhaseSucceeded, Type: "logical"}
	if err := st.CreateBackup(t.Context(), primary); err != nil {
		t.Fatal(err)
	}
	operation := &domain.Backup{Name: "operation-b", Cluster: rc.Cluster.Name, Storage: "alternate", PBMName: "2026-10-19T12:01:00Z", Phase: domain.PhaseRunning, Type: "logical"}
	if err := st.CreateBackup(t.Context(), operation); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enc := json.NewEncoder(w)
		switch r.URL.Path {
		case "/v1/status":
			_ = enc.Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
		case "/v1/pbm":
			calls++
			var request agentapi.PBMRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return
			}
			output := `{"snapshots":[{"name":"2026-10-19T12:02:00Z","status":"done","type":"logical"}],"pitr":{"ranges":[]}}`
			if request.Command == "status" {
				output = `{"backups":{"snapshot":[]}}`
			}
			_ = enc.Encode(agentapi.PBMResponse{Output: json.RawMessage(output)})
		default:
			_ = enc.Encode(agentapi.ActionResponse{})
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = testDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	rc.AvailabilityOnly = true
	_, _ = engine.ObserveStatus(t.Context(), p, rc)
	got, err := st.GetBackup(t.Context(), rc.Cluster.Ref(), primary.Name)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.Storage != "archive" || got.PBMPrefix != primary.PBMPrefix {
		t.Fatal("availability corrupted primary backup", got, err)
	}
	backups, err := st.ListBackups(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 2 || calls != 0 {
		t.Fatal("availability adopted alternate snapshots or touched operation-owned PBM configuration", len(backups), calls)
	}
}
