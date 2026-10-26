// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestLegacyEventDetailsMigrationAndReplay(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	verifier := "QWxwaGFCZXRhR2FtbWFEZWx0YUVwc2lsb25PbmVUd28="
	details := []byte(`{"credentials":{"SCRAM-SHA-256":{"salt":"fake-salt","storedKey":"fake-stored","serverKey":"fake-server","iterationCount":4096}},"changes":[{"path":"users","before":{"payload":{"salt":"fake-before"},"checkpoint":{"storedKey":"fake-checkpoint"}},"after":{"internal":{"serverKey":"fake-internal"},"password":"fake-after","key":"fake-key"}}],"operationId":"ui9Z3FEPPOSGoLNi7MP2UY9q","sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","number":9007199254740993}`)
	initialID := "tenant:production-mongodb-target-Ab3dE6gH9jK2-initial-restore"
	backupName := "averylongproductionmongodbbackupname20261021"
	data, _ := json.Marshal(map[string]string{"message": "Waiting for initial restore operation " + initialID + " backup " + backupName + " native failure " + verifier})
	if _, err := s.writer.ExecContext(ctx, `INSERT INTO events(cluster,type,data,details,created) VALUES('demo','Warning',?,?,7)`, data, details); err != nil {
		t.Fatal(err)
	}
	check := func(raw []byte) {
		t.Helper()
		if bytes.Contains(raw, []byte("fake-")) || bytes.Contains(raw, []byte(verifier)) {
			t.Fatal("legacy credential material retained")
		}
	}
	events, err := s.ListEvents(ctx, EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatal("missing event")
	}
	check(events[0].Details)
	check([]byte(events[0].Message))
	for iteration := range 2 {
		if _, err = s.writer.ExecContext(ctx, `DELETE FROM schema_migrations WHERE name='012_sanitize_diagnostics.sql'`); err != nil {
			t.Fatal(err)
		}
		if err = s.migrate(ctx); err != nil {
			t.Fatal(err)
		}
		var storedData, storedDetails []byte
		if err = s.writer.QueryRowContext(ctx, `SELECT data,details FROM events WHERE cluster='demo'`).Scan(&storedData, &storedDetails); err != nil {
			t.Fatal(err)
		}
		check(storedData)
		check(storedDetails)
		if !bytes.Contains(storedData, []byte(initialID)) || !bytes.Contains(storedData, []byte(backupName)) {
			t.Fatal("migration destroyed diagnostic resource identity")
		}
		if !bytes.Contains(storedDetails, []byte("ui9Z3FEPPOSGoLNi7MP2UY9q")) || !bytes.Contains(storedDetails, []byte("9007199254740993")) {
			t.Fatal("identity or precision changed")
		}
		if iteration == 0 {
			data = storedData
			details = storedDetails
		} else if !bytes.Equal(data, storedData) || !bytes.Equal(details, storedDetails) {
			t.Fatal("migration replay changed sanitized rows")
		}
	}
}
