// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestDeleteUnstartedBackupWithoutStorage(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "unstarted", Storage: "removed-storage", Provider: "neo4j", Type: "dump", Phase: domain.PhaseFailed}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "delete-unstarted", Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"unstarted"}`)}
	runDumpOperation(t, p, rc, op)
	if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
		t.Fatal("unstarted backup inventory was not deleted", err)
	}
}
func TestStoredPrefixRejectsForeignProvenance(t *testing.T) {
	b := domain.Backup{Namespace: "default", Cluster: "demo", Name: "snapshot", Provider: "neo4j", Type: "dump", PBMPrefix: "production/neo4j/default/demo-incarnation/snapshot"}
	if _, err := storedPrefix(b); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "production/neo4j/default/other-incarnation/snapshot", "production/neo4j/other/demo-incarnation/snapshot", "production/neo4j/default/demo-incarnation/snapshot-other", "production/neo4j/default/demo-incarnation/../snapshot"} {
		b.PBMPrefix = prefix
		if _, err := storedPrefix(b); err == nil {
			t.Fatal("foreign or malformed prefix accepted", prefix)
		}
	}
}
