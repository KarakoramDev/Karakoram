// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStorageEndpointAdmission(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		valid    bool
	}{
		{"https://s3.example.test", true}, {"http://s3.example.test:9000/", true}, {"http://192.0.2.1:9000", true}, {"https://[2001:db8::1]:9443", true},
		{"s3.example.test", false}, {"s3.example.test:9000", false}, {"https://s3.example.test/s3", false}, {"https://s3.example.test/s3/", false}, {"https://s3.example.test/%2f", false},
		{"https://s3.example.test:0", false}, {"https://s3.example.test:65536", false}, {"https://s3.example.test:", false}, {"https://[fe80::1%25eth0]", false},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			b := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: tc.endpoint, Region: "region", Bucket: "objects", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			err := validateStorage(b, true)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}
			if !tc.valid && strings.Contains(tc.endpoint, ".test/s3") && !strings.Contains(err.Error(), "path prefixes") {
				t.Fatalf("missing shape-specific advice: %v", err)
			}
		})
	}
}

func TestStorageEndpointPrefixRejectedOnCreateAndReplace(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"name":"archive","type":"s3","endpointURL":"https://s3.example.test/s3","region":"region","bucket":"objects","credentials":{"accessKeyId":"access","secretAccessKey":"secret"}}`
	if w := call(s, "POST", "/api/v1/backup-storages", body, token); w.Code != 400 || !strings.Contains(w.Body.String(), "path prefixes") {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	body = strings.Replace(body, ".test/s3", ".test", 1)
	if w := call(s, "POST", "/api/v1/backup-storages", body, token); w.Code != 201 {
		t.Fatalf("create origin: %d %s", w.Code, w.Body.String())
	}
	update := `{"type":"s3","endpointURL":"https://s3.example.test/s3","region":"region","bucket":"objects","forcePathStyle":false,"verifyTLS":true}`
	if w := call(s, "PUT", "/api/v1/backup-storages/archive", update, token); w.Code != 400 || !strings.Contains(w.Body.String(), "path prefixes") {
		t.Fatalf("replace: %d %s", w.Code, w.Body.String())
	}
}

func TestLegacyStorageEndpointStatusAndVerify(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, _, token := testServerWithDatabase(t, path)
	b := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Region: "region", Bucket: "objects", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := s.Store.CreateBackupStorage(t.Context(), &b); err != nil {
		t.Fatal(err)
	}
	// Model a pre-upgrade persisted registration without passing new admission.
	b.EndpointURL += "/s3"
	b.Verification = &domain.StorageVerification{Verified: true, CheckedAt: time.Now().UTC(), Message: "Old successful check"}
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(t.Context(), "UPDATE records SET data=? WHERE kind='storage' AND key='archive'", data); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/api/v1/backup-storages/archive", "/api/v1/backup-storages"} {
		w := call(s, "GET", route, "", token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var projected api.BackupStorage
		if route == "/api/v1/backup-storages" {
			var list api.BackupStorageList
			if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Items) != 1 {
				t.Fatal(w.Body.String(), err)
			}
			projected = list.Items[0]
		} else if err := json.Unmarshal(w.Body.Bytes(), &projected); err != nil {
			t.Fatal(err)
		}
		if projected.Verification == nil || projected.Verification.Verified || !strings.Contains(projected.Verification.Message, "path prefixes") {
			t.Fatalf("legacy status: %+v", projected.Verification)
		}
	}
	w := call(s, "POST", "/api/v1/backup-storages/archive/verify", "", token)
	var result api.StorageVerification
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Verified || !strings.Contains(result.Message, "path prefixes") || result.FailureReason == nil || *result.FailureReason != "endpoint_not_allowed" {
		t.Fatalf("verify: %d %s", w.Code, w.Body.String())
	}
	persisted, err := s.Store.GetBackupStorage(t.Context(), "default", "archive")
	if err != nil || persisted.Verification == nil || persisted.Verification.Verified {
		t.Fatalf("persisted check: %+v %v", persisted, err)
	}
}
