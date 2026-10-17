// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package logship

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type source struct{ lines []agentapi.LogLine }

func (s source) LogTail(string, int) []agentapi.LogLine { return s.lines }
func TestBoundedBatchesAndRedaction(t *testing.T) {
	batches := make(chan []event, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ingest-token" {
			t.Error("missing ingestion authentication")
		}
		var events []event
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			t.Error(err)
		}
		batches <- events
		w.WriteHeader(200)
	}))
	defer server.Close()
	lines := make([]agentapi.LogLine, 205)
	now := time.Now()
	for i := range lines {
		lines[i] = agentapi.LogLine{Timestamp: now.Add(time.Duration(i) * time.Nanosecond), Process: "postgres", Message: "password secret-value " + strings.Repeat("x", 9000)}
	}
	cancel := Start(context.Background(), source{lines}, Options{URL: server.URL, Token: "ingest-token", Member: "member", Processes: []string{"postgres"}, Secrets: []string{"secret-value"}})
	defer cancel()
	count := 0
	for count < 205 {
		select {
		case batch := <-batches:
			if len(batch) > 100 {
				t.Fatal("batch exceeds cap")
			}
			for _, e := range batch {
				if e.Member != "member" || e.Process != "postgres" || len(e.Message) > 8192 || strings.Contains(e.Message, "secret-value") {
					t.Fatal("invalid bounded/redacted event")
				}
			}
			count += len(batch)
		case <-time.After(5 * time.Second):
			t.Fatal("log batches not delivered")
		}
	}
	select {
	case <-batches:
		t.Fatal("duplicate log batch")
	case <-time.After(1100 * time.Millisecond):
	}
}
