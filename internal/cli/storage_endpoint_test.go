// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStorageVerifyUnsupportedEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/backup-storages/archive/verify" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"verified":false,"checkedAt":"2026-01-01T00:00:00Z","failureReason":"endpoint_not_allowed","message":"S3 endpoint path prefixes are unsupported; use an HTTP(S) origin and put object key prefixes in prefix"}`))
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "storages", "verify", "archive", "-o", "json")
	if code == 0 || !strings.Contains(out, "endpoint_not_allowed") || !strings.Contains(out, "path prefixes") || !strings.Contains(stderr, "path prefixes") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, stderr)
	}
}
