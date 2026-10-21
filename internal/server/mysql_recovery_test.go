// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/pxc"
)

func TestMySQLQuorumRecoveryAcceptanceAndReplay(t *testing.T) {
	s, token := namespaceTestServer(t)
	provider := pxc.New(mysql.Options{})
	var err error
	s.Registry, err = engine.NewRegistry(provider, mysql.New(mysql.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pxcone", "sqlone"} {
		c := domain.Cluster{Name: name}
		if name == "pxcone" {
			provider.ApplyDefaults(&c.Spec)
		} else {
			mysql.New(mysql.Options{}).ApplyDefaults(&c.Spec)
		}
		if err := s.Store.CreateCluster(t.Context(), &c); err != nil {
			t.Fatal(err)
		}
	}
	path := "/api/v1/clusters/pxcone/mysql/recover-quorum"
	for _, body := range []string{`{}`, `{"confirmation":"yes"}`} {
		w := call(s, http.MethodPost, path, body, token)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("missing explicit confirmation accepted: %d %s", w.Code, w.Body.String())
		}
	}
	body := `{"confirmation":"all-absent-members-fenced"}`
	first := idempotentCall(s, http.MethodPost, path, body, token, "pxc-quorum-one")
	second := idempotentCall(s, http.MethodPost, path, body, token, "pxc-quorum-one")
	assertReplay(t, first, second, http.StatusAccepted)
	var op api.Operation
	if err := json.Unmarshal(first.Body.Bytes(), &op); err != nil {
		t.Fatal(err)
	}
	if op.Id == "" || string(op.Type) != "recover-quorum" || first.Header().Get("Karakoram-Operation-Id") != op.Id {
		t.Fatalf("invalid queued recovery: %+v", op)
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "pxcone"})
	if err != nil || len(ops) != 1 {
		t.Fatalf("recovery replay queued duplicates: %v %v", ops, err)
	}
	if w := call(s, http.MethodPost, "/api/v1/clusters/sqlone/mysql/recover-quorum", body, token); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"field":"spec.provider"`) || !strings.Contains(w.Body.String(), "pxc provider") {
		t.Fatalf("Percona Server quorum rejection lacks provider guidance: %d %s", w.Code, w.Body.String())
	}
	rule, ok := api.OperationRBAC["recoverMySQLQuorum"]
	if !ok || rule.Resource != "clusters" || rule.Action != "update" || rule.ObjectParam != "cluster" || rule.Public {
		t.Fatalf("incorrect quorum recovery RBAC: %+v", rule)
	}
}
