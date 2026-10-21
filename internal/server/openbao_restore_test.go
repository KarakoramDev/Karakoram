// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func openbaoRestoreServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, _, token := testServerWithDatabase(t, path)
	registry, err := engine.NewRegistry(&fakeOpenBaoAccess{}, &fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "openbao"
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	c.Internal = json.RawMessage(`{"uid":"original"}`)
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	b := domain.Backup{Cluster: "gone", Name: "snapshot", Provider: "openbao", Type: "raft-snapshot", Phase: domain.PhaseSucceeded, Location: "s3://archive/snapshot.snap"}
	if err := s.Store.CreateBackup(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	return s, token, path
}

func TestOpenBaoRestoreAdmission(t *testing.T) {
	for _, tc := range []struct{ name, shares string }{
		{"missing", ""},
		{"invalid threshold", `,"openbaoUnsealShares":{"shares":["private-share"],"threshold":2}`},
		{"duplicate shares", `,"openbaoUnsealShares":{"shares":["private-share","private-share"],"threshold":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, token, _ := openbaoRestoreServer(t)
			body := `{"name":"restored","source":{"sourceCluster":"gone","backup":"snapshot"}` + tc.shares + `}`
			w := call(s, "POST", "/api/v1/clusters/prod/restores", body, token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "openbaoUnsealShares") {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	s, token, _ := openbaoRestoreServer(t)
	shares := `,"openbaoUnsealShares":{"shares":["private-share"],"threshold":1}`
	if w := call(s, "POST", "/api/v1/clusters/dev/restores", `{"name":"restored","source":{"sourceCluster":"gone","backup":"snapshot"}`+shares+`}`, token); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOpenBaoLiveSourceDoesNotRequireSuppliedShares(t *testing.T) {
	s, token, _ := openbaoRestoreServer(t)
	backup := domain.Backup{Cluster: "prod", Name: "live", Provider: "openbao", Type: "raft-snapshot", Phase: domain.PhaseSucceeded, Location: "s3://archive/original/raft/live.snap"}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"restored","source":{"backup":"live"}}`, token)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOpenBaoRestoreSharesStayEncrypted(t *testing.T) {
	for _, clone := range []bool{false, true} {
		t.Run(map[bool]string{false: "restore", true: "clone"}[clone], func(t *testing.T) {
			s, token, path := openbaoRestoreServer(t)
			url := "/api/v1/clusters/prod/restores"
			body := `{"name":"restored","source":{"sourceCluster":"gone","backup":"snapshot"},"openbaoUnsealShares":{"shares":["private-share"],"threshold":1}}`
			target, name, status := "prod", "restored", 202
			if clone {
				url = "/api/v1/clusters"
				body = `{"name":"cloned","spec":{"provider":"openbao","dataSource":{"cluster":"gone","backup":"snapshot"}},"openbaoUnsealShares":{"shares":["private-share"],"threshold":1}}`
				target, name, status = "cloned", "clone", 201
			}
			first := idempotentCall(s, "POST", url, body, token, "restore-request")
			second := idempotentCall(s, "POST", url, body, token, "restore-request")
			assertReplay(t, first, second, status)
			if strings.Contains(first.Body.String(), "private-share") || strings.Contains(first.Body.String(), "openbaoUnsealShares") {
				t.Fatal("response echoed recovery material")
			}
			raw, err := s.Store.GetClusterSecret(t.Context(), domain.ClusterRef{Name: target}, openbao.RestoreUnsealSecret(name))
			if err != nil {
				t.Fatal(err)
			}
			var material openbao.RestoreUnsealShares
			if err := json.Unmarshal(raw, &material); err != nil || len(material.Shares) != 1 || material.Shares[0] != "private-share" {
				t.Fatal("missing encrypted restore input", err)
			}
			db, err := sql.Open("sqlite", "file:"+path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var hash, response string
			if err := db.QueryRow("SELECT body_hash,response FROM idempotency_requests").Scan(&hash, &response); err != nil {
				t.Fatal(err)
			}
			canonical, err := canonicalIdempotencyBody([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if hash == secrets.Hash(string(canonical)) || strings.Contains(hash+response, "private-share") {
				t.Fatal("idempotency stored plaintext or guessable share digest")
			}
			var ciphertext string
			if err := db.QueryRow("SELECT secret FROM records WHERE kind='secret' AND scope=? AND key=?", (domain.ClusterRef{Name: target}).Key(), openbao.RestoreUnsealSecret(name)).Scan(&ciphertext); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(ciphertext, "kk:v1:") || strings.Contains(ciphertext, "private-share") {
				t.Fatal("restore secret persisted in plaintext")
			}
			cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: target})
			if err != nil {
				t.Fatal(err)
			}
			operations, err := s.Store.ListOperations(t.Context(), cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Cluster: target, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			state, _ := json.Marshal([]any{cluster, operations, events})
			if strings.Contains(string(state), "private-share") || strings.Contains(string(state), "openbaoUnsealShares") {
				t.Fatal("recovery material leaked to public state or audit")
			}
		})
	}
}

func TestOpenBaoPostgreSQLStorageRejectsSnapshotAdmission(t *testing.T) {
	s, token, _ := openbaoRestoreServer(t)
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"database"}}}`)
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, body string }{
		{"/api/v1/clusters/prod/backups", `{"name":"snapshot","type":"raft-snapshot","storage":"archive"}`},
		{"/api/v1/clusters/prod/restores", `{"name":"restored","source":{"sourceCluster":"gone","backup":"snapshot"}}`},
		{"/api/v1/clusters", `{"name":"cloned","spec":{"provider":"openbao","engineOptions":{"storage":{"type":"postgresql","postgresql":{"cluster":"database"}}},"dataSource":{"cluster":"gone","backup":"snapshot"}}}`},
	} {
		w := call(s, "POST", tc.path, tc.body, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "PostgreSQL storage") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	operations, err := s.Store.ListOperations(t.Context(), c.Ref())
	if err != nil || len(operations) != 0 {
		t.Fatal("unsupported snapshot operation enqueued", operations, err)
	}
}

func TestOpenBaoRecreatedSourceRequiresSuppliedShares(t *testing.T) {
	s, token, _ := openbaoRestoreServer(t)
	backup := domain.Backup{Cluster: "prod", Name: "old", Provider: "openbao", Type: "raft-snapshot", Phase: domain.PhaseSucceeded, Location: "s3://archive/deleted/raft/old.snap"}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"restored","source":{"backup":"old"}}`, token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "earlier incarnation") {
		t.Fatal(w.Code, w.Body.String())
	}
}
