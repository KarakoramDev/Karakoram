// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type restoreInfra struct {
	*coreInfra
	completed func(string, string)
	polls     int
}

func (f *restoreInfra) OpStatus(_ context.Context, _ infra.Op) (infra.OpState, error) {
	f.polls++
	if f.polls%2 != 0 {
		return infra.OpState{}, nil
	}
	call := f.calls[len(f.calls)-1]
	action, name, _ := strings.Cut(call, ":")
	f.completed(action, name)
	return infra.OpState{Done: true}, nil
}

func TestRestoreCheckpointStageOrdering(t *testing.T) {
	var actions []string
	stopped := map[string]bool{}
	hasIndex := true
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/v1/"):
			t.Error("restore addressed a single component agent", path)
			opsJSON(w, map[string]any{})
		case strings.HasSuffix(path, "/snapshot-one/_restore"):
			actions = append(actions, "restore")
			hasIndex = true
			var req esapi.RestoreRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Indices != "*" || !req.IncludeGlobalState || !req.IncludeAliases {
				t.Errorf("restore request: %+v %v", req, err)
			}
			opsJSON(w, map[string]any{"snapshot": map[string]any{"shards": map[string]int{"total": 4, "successful": 4, "failed": 0}}})
		case strings.HasSuffix(path, "/snapshot-one"):
			opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{Snapshot: "snapshot-one", State: "SUCCESS", Version: "9.5.5", Indices: []string{"logs-test", ".tenant-cache", "hidden-user", ".security-7"}}}})
		case path == "/_cat/indices":
			if r.URL.Query().Get("expand_wildcards") != "all" {
				t.Error("restore did not enumerate hidden indices")
			}
			rows := []map[string]string{{"index": ".security-7"}}
			if hasIndex {
				for _, name := range []string{"logs-test", ".tenant-cache", "hidden-user"} {
					rows = append(rows, map[string]string{"index": name})
				}
			}
			opsJSON(w, rows)
		case path == "/_cluster/state/metadata":
			opsJSON(w, map[string]any{"metadata": map[string]any{"indices": map[string]any{".security-7": map[string]bool{"system": true}, ".tenant-cache": map[string]bool{"system": false}, "hidden-user": map[string]bool{"system": false}, "logs-test": map[string]bool{"system": false}}}})
		case r.Method == "DELETE" && !strings.HasPrefix(path, "/_"):
			if !stopped["testkb"] || !stopped["testls"] || !stopped["testapm"] {
				t.Error("index deletion before service-wide stops completed")
			}
			for _, name := range []string{"logs-test", ".tenant-cache", "hidden-user"} {
				if !slices.Contains(strings.Split(strings.TrimPrefix(path, "/"), ","), name) {
					t.Error("ordinary index was not deleted", name, path)
				}
			}
			if strings.Contains(path, "security") {
				t.Error("deleted system index")
			}
			actions = append(actions, "indices")
			hasIndex = false
			opsJSON(w, map[string]any{"acknowledged": true})
		case r.Method == "DELETE" && strings.HasPrefix(path, "/_data_stream/"):
			if !stopped["testkb"] || !stopped["testls"] || !stopped["testapm"] {
				t.Error("data stream deletion before service-wide stops completed")
			}
			if r.URL.Query().Get("expand_wildcards") != "all" {
				t.Error("hidden data streams were not deleted")
			}
			actions = append(actions, "streams")
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
	f := &restoreInfra{coreInfra: &coreInfra{services: map[string]*infra.Service{}}}
	f.completed = func(action, name string) {
		actions = append(actions, action)
		stopped[name] = action == "stop"
		if action == "start" && s.RestoreActive {
			t.Error("service started before restore intent was released")
		}
	}
	rc.Backend = f
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		name := map[string]string{kindKibana: "testkb", kindLogstash: "testls", kindAPM: "testapm"}[kind]
		s.Members = append(s.Members, &member{Name: name, Kind: kind, Stage: stageJoined, ServiceID: name, Containers: 2})
		f.services[name] = &infra.Service{ID: name, MinContainers: 2, MaxContainers: 2}
	}
	st.backup.Phase = domain.PhaseSucceeded
	st.backup.Provider = "elasticsearch"
	st.backup.SourceVersion = "9.5.5"
	st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
	st.restore = domain.Restore{Name: "initial-restore", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhasePending}
	op := &domain.Operation{ID: "restore-one", Kind: domain.OperationRestore}
	cp := operationState{}
	for range 80 {
		// Reload both checkpoints rather than passing an in-memory continuation.
		result, err := p.runRestore(context.Background(), rc, s, op, cp, st.restore.Name)
		if err != nil {
			t.Fatal(err)
		}
		cp = opsCheckpoint(t, result)
		if err = save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if cp.Pending != nil && cp.Pending.Op.ID == "" {
			t.Error("platform receipt missing from restore checkpoint")
		}
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
	if len(f.calls) != 6 {
		t.Fatal("component stop/start was not exactly once per service", f.calls)
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

type restoreBackupLookupStore struct {
	*opsStore
	lookups int
}

func (s *restoreBackupLookupStore) GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error) {
	s.lookups++
	return nil, engine.ErrNotFound
}

func TestRestoreRejectsInvalidSourceBeforeBackupLookup(t *testing.T) {
	for _, source := range []string{"pitr", "missing-backup"} {
		t.Run(source, func(t *testing.T) {
			st := &restoreBackupLookupStore{opsStore: &opsStore{restore: domain.Restore{Name: "invalid-source", Phase: domain.PhasePending}}}
			if source == "pitr" {
				at := operationNow(&engine.Context{})
				st.restore.Source.PITRTime = &at
			}
			rc := &engine.Context{Cluster: &domain.Cluster{Name: "test"}, Store: st}
			s := &state{ClusterUUID: "formed"}
			op := &domain.Operation{Kind: domain.OperationRestore, Payload: []byte(`{"restore":"invalid-source"}`)}
			p := New(ProviderOptions{})
			result, err := p.runOperation(context.Background(), rc, s, op)
			if !result.Done || !errors.Is(err, engine.ErrInvalid) || st.lookups != 0 || st.restore.Phase != domain.PhaseFailed || st.restore.CompletedAt == nil || st.restore.Error == "" || s.RestoreActive {
				t.Fatalf("invalid source was not terminal before lookup: %+v %v %+v lookups=%d", result, err, st.restore, st.lookups)
			}
			cp := opsCheckpoint(t, result)
			if cp.Stage != "done" || cp.Failure == "" {
				t.Fatal("terminal failure checkpoint missing", cp)
			}
			op.Checkpoint = result.Checkpoint
			result, err = p.runOperation(context.Background(), rc, s, op)
			if !result.Done || err == nil || st.lookups != 0 {
				t.Fatal("terminal failure retried backup lookup", result, err)
			}
		})
	}
}
