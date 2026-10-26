// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStorageUpdateClearsPersistedCheck(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"name":"archive","type":"s3","endpointURL":"https://127.0.0.1","region":"region","bucket":"bucket","credentials":{"accessKeyId":"access-private","secretAccessKey":"secret-private"}}`
	if w := call(s, "POST", "/api/v1/backup-storages", body, token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	b, err := s.Store.GetBackupStorage(t.Context(), "default", "archive")
	if err != nil {
		t.Fatal(err)
	}
	b.Verification = &domain.StorageVerification{Verified: true, CheckedAt: time.Now().UTC(), Message: "Access verified"}
	if err = s.Store.SaveBackupStorageVerification(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	check := func(path string, want bool) {
		t.Helper()
		w := call(s, "GET", path, "", token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var storage api.BackupStorage
		if path == "/api/v1/backup-storages" {
			var list api.BackupStorageList
			if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Items) != 1 {
				t.Fatal(w.Body.String(), err)
			}
			storage = list.Items[0]
		} else if err := json.Unmarshal(w.Body.Bytes(), &storage); err != nil {
			t.Fatal(err)
		}
		if (storage.Verification != nil) != want {
			t.Fatal(w.Body.String())
		}
	}
	check("/api/v1/backup-storages/archive", true)
	check("/api/v1/backup-storages", true)
	update := `{"type":"s3","endpointURL":"https://127.0.0.1","region":"region","bucket":"bucket","forcePathStyle":true,"verifyTLS":true,"credentials":{"accessKeyId":"access-private","secretAccessKey":"replacement-private"}}`
	w := call(s, "PUT", "/api/v1/backup-storages/archive", update, token)
	var saved api.BackupStorage
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &saved) != nil || saved.Verification != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	check("/api/v1/backup-storages/archive", false)
	check("/api/v1/backup-storages", false)
	w = call(s, "POST", "/api/v1/backup-storages/archive/verify", "", token)
	var result api.StorageVerification
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Verified {
		t.Fatal(w.Code, w.Body.String())
	}
	check("/api/v1/backup-storages/archive", true)
	check("/api/v1/backup-storages", true)
}
