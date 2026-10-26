// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestEventDetailsRedactedByAPIAndSSE(t *testing.T) {
	s, _, token := testServer(t)
	details := json.RawMessage(`{"changes":[{"path":"native.users","before":{"payload":{"salt":"fake-before"},"checkpoint":{"storedKey":"fake-stored"}},"after":{"internal":{"serverKey":"fake-after"},"password":"fake-password"}}]}`)
	event := domain.Event{Cluster: "prod", Type: "Warning", Reason: "legacy-details", Message: "legacy diagnostic", Details: details}
	check := func(text string) {
		t.Helper()
		if strings.Contains(text, "fake-") {
			t.Fatal("event API leaked legacy credential")
		}
	}
	converted, err := json.Marshal(eventAPI(event))
	if err != nil {
		t.Fatal(err)
	}
	check(string(converted))
	if !strings.Contains(string(converted), "[redacted]") {
		t.Fatal("missing sanitized event details")
	}
	if err = s.Store.AppendEvent(context.Background(), &event); err != nil {
		t.Fatal(err)
	}
	response := call(s, "GET", "/api/v1/events?cluster=prod", "", token)
	if response.Code != 200 {
		t.Fatal(response.Code)
	}
	check(response.Body.String())
	ts := httptest.NewServer(testHandler(s))
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", ts.URL+"/api/v1/events/stream?cluster=prod", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	streamed, err := ts.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer streamed.Body.Close()
	if streamed.StatusCode != 200 {
		t.Fatal(streamed.StatusCode)
	}
	scanner := bufio.NewScanner(streamed.Body)
	for scanner.Scan() {
		line := scanner.Text()
		check(line)
		if strings.HasPrefix(line, "data:") && strings.Contains(line, "legacy-details") {
			return
		}
	}
	t.Fatal("legacy event not received through SSE")
}
