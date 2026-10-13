// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestRestoreCheckpointStageOrdering(t *testing.T) {
	var actions []string
	running := true
	hasIndex := true
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/v1/processes/kibana/stop":
			actions = append(actions, "stop")
			running = false
			opsJSON(w, map[string]any{})
		case path == "/v1/processes/kibana/start":
			actions = append(actions, "start")
			running = true
			opsJSON(w, map[string]any{})
		case path == "/v1/status":
			state := "stopped"
			if running {
				state = "running"
			}
			opsJSON(w, agentapi.Status{ConfigVersion: 10000, Processes: map[string]agentapi.ProcessStatus{"kibana": {State: state}}})
		case path == "/v1/reload":
			opsJSON(w, map[string]any{})
		case strings.HasSuffix(path, "/snapshot-one/_restore"):
			actions = append(actions, "restore")
			hasIndex = true
			var req esapi.RestoreRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Indices != "*" || !req.IncludeGlobalState || !req.IncludeAliases {
				t.Errorf("restore request: %+v %v", req, err)
			}
			opsJSON(w, map[string]any{"accepted": true})
		case strings.HasSuffix(path, "/snapshot-one"):
			opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{Snapshot: "snapshot-one", State: "SUCCESS", Version: "9.5.5", Indices: []string{"logs-test", ".security-7"}}}})
		case path == "/_cat/indices":
			rows := []map[string]string{{"index": ".security-7"}}
			if hasIndex {
				rows = append(rows, map[string]string{"index": "logs-test"})
			}
			opsJSON(w, rows)
		case r.Method == "DELETE" && path == "/logs-test":
			if running {
				t.Error("index deletion while component running")
			}
			actions = append(actions, "indices")
			hasIndex = false
			opsJSON(w, map[string]any{"acknowledged": true})
		case r.Method == "DELETE" && strings.HasPrefix(path, "/_data_stream/"):
			if running {
				t.Error("data stream deletion while component running")
			}
			actions = append(actions, "streams")
			opsJSON(w, map[string]any{"acknowledged": true})
		case r.Method == "DELETE" && strings.Contains(path, "security"):
			t.Error("deleted security index")
			opsJSON(w, map[string]any{"acknowledged": true})
		case path == "/_cat/recovery":
			opsJSON(w, []any{})
		case path == "/_cluster/health":
			opsJSON(w, esapi.Health{Status: "green", NumberOfDataNodes: 1})
		case path == "/_cluster/settings":
			var settings esapi.ClusterSettings
			_ = json.NewDecoder(r.Body).Decode(&settings)
			if v, ok := settings.Transient["action.destructive_requires_name"]; ok {
				if v == nil {
					actions = append(actions, "reset")
				} else {
					actions = append(actions, "destructive")
				}
			}
			opsJSON(w, map[string]any{"acknowledged": true})
		case path == "/_security/_authenticate":
			opsJSON(w, map[string]string{"username": "elastic/kibana"})
		case path == "/_security/user/elastic/_password":
			actions = append(actions, "security")
			opsJSON(w, map[string]any{})
		default:
			opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
		}
	})
	s.Members = append(s.Members, &member{Name: "testkb", Kind: kindKibana, Stage: stageJoined})
	st.backup.Phase = domain.PhaseSucceeded
	st.backup.Provider = "elasticsearch"
	st.backup.SourceVersion = "9.5.5"
	st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
	st.restore = domain.Restore{Name: "initial-restore", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhasePending}
	op := &domain.Operation{ID: "restore-one", Kind: domain.OperationRestore}
	cp := operationState{}
	for range 40 {
		// Reload JSON every call rather than passing an in-memory continuation.
		result, err := p.runRestore(context.Background(), rc, s, op, cp, st.restore.Name)
		if err != nil {
			t.Fatal(err)
		}
		cp = opsCheckpoint(t, result)
		if result.Done {
			break
		}
	}
	if cp.Stage != "done" || st.restore.Phase != domain.PhaseSucceeded || st.restore.StartedAt == nil || st.restore.CompletedAt == nil || s.RestoreActive || s.InitialRestore != "done" {
		t.Fatalf("checkpoint=%+v restore=%+v state=%+v", cp, st.restore, s)
	}
	previous := -1
	for _, action := range []string{"stop", "destructive", "streams", "indices", "restore", "reset", "security", "start"} {
		i := slices.Index(actions, action)
		if i <= previous {
			t.Fatalf("bad order for %s: %v", action, actions)
		}
		previous = i
	}
}

func TestRestoreVersionCompatibility(t *testing.T) {
	for _, tc := range []struct {
		source, target string
		want           bool
	}{{"9.5.4", "9.5.5", true}, {"8.19.23", "9.5.5", true}, {"9.5.5", "9.4.8", false}, {"8.19.23", "10.0.0", false}, {"bad", "9.5.5", false}} {
		if got := restoreVersionCompatible(tc.source, tc.target); got != tc.want {
			t.Fatalf("%s -> %s: %v", tc.source, tc.target, got)
		}
	}
}
