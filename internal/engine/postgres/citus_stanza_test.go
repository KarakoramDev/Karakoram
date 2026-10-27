// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestCitusStanzasTrackGroupRepositories(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	created := map[string]int{}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			// Archive failure must not prevent initializing the repository that
			// repairs it. Native formation readiness does not require archive health.
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), ArchiveFailing: true})
		case "/v1/postgres/stanza-create":
			var req pgspec.RepositoryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			created[req.Repository.Path]++
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	sync := func(wantHandled bool) {
		t.Helper()
		handled, err := p.syncBackups(t.Context(), rc, s)
		if err != nil || handled != wantHandled {
			t.Fatalf("sync = %v, %v; want handled %v", handled, err, wantHandled)
		}
	}
	sync(true) // Initial creation initializes every group.
	if len(created) != 2 {
		t.Fatalf("initial repositories: %v", created)
	}
	sync(false)
	worker := *s.group(1).data()[0]
	worker.Name, worker.Group, worker.Stage = "worker2", 2, "planned"
	s.Members = append(s.Members, &worker)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	sync(false) // Do not issue native actions before the new member joins.
	if len(created) != 2 {
		t.Fatal("stanza created before provisioning", created)
	}
	worker.Stage = "joined"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	sync(true) // Must run before the cluster's archive-health readiness gate.
	storage, err := p.backupStorage(t.Context(), rc, "", rc.Cluster.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	path := "/" + groupRepoPrefix(storage, rc.Cluster, s, 2)
	if created[path] != 1 {
		t.Fatalf("new worker stanza absent: %v", created)
	}
	sync(false)
	s.GroupRepositoryIncarnations = map[int]int{2: 1}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	sync(true)
	reincarnated := "/" + groupRepoPrefix(storage, rc.Cluster, s, 2)
	if created[reincarnated] != 1 {
		t.Fatalf("recreated worker stanza absent: %v", created)
	}
	sync(false)
	if created[path] != 1 || created[reincarnated] != 1 {
		t.Fatal("unchanged sync repeated stanza creation", created)
	}
}
