// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"
)

func TestSlowRequestDiagnostics(t *testing.T) {
	var out bytes.Buffer
	s := &Server{Options: Options{Logger: slog.New(slog.NewJSONHandler(&out, nil))}}
	s.logRequest("request-test", "GET", "GET /api/v1/clusters/{cluster}", 3*time.Second)
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "WARN" || record["msg"] != "Slow HTTP request" || record["route"] != "GET /api/v1/clusters/{cluster}" || record["duration"] != float64(3*time.Second) || record["request_id"] != "request-test" {
		t.Fatalf("missing slow-request diagnostic fields: %v", record)
	}
	out.Reset()
	s.logRequest("request-test", "GET", "GET /healthz", time.Millisecond)
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "INFO" || record["msg"] != "HTTP request" {
		t.Fatalf("ordinary request changed severity: %v", record)
	}
}
