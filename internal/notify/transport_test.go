// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package notify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/netguard"
)

func transportSender(value string) *Sender {
	u, _ := url.Parse(value)
	s := NewSender()
	s.policy = &netguard.Policy{Allowlist: netguard.ParseAllowlist(u.Host)}
	s.now = func() time.Time { return time.Unix(1700000000, 0) }
	return s
}
func TestTransportWebhookSignature(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Karakoram-Event-Id") != "42" {
			t.Error("missing headers")
		}
		if r.Header.Get("X-Karakoram-Signature") != "t=1700000000,v1=757ee5e4d398b824a3af0f1c4b273c275a5b99807af01e76f464b7e157fffd45" {
			t.Error("incorrect signature vector")
		}
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Error(err)
		}
		if got["id"] != float64(42) || got["category"] != "backup-failed" || got["severity"] != "warning" || got["source"] != "karakoram" || got["namespace"] != "ns" || got["cluster"] != "db" || got["reason"] != "Failed" || got["message"] != "backup failed" || got["createdAt"] != "2026-01-02T03:04:05Z" {
			t.Errorf("wrong payload: %v", got)
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	channel := domain.NotificationChannel{Type: "webhook", Secrets: domain.NotificationChannelSecrets{WebhookURL: server.URL, WebhookSigningSecret: "test-secret"}}
	event := domain.Event{ID: 42, Type: "Warning", Namespace: "ns", Cluster: "db", Reason: "Failed", Message: "backup failed", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	if err := transportSender(server.URL).Send(t.Context(), channel, event, domain.NotificationBackupFailed); err != nil {
		t.Fatal(err)
	}
	if err := NewSender().Send(t.Context(), channel, event, domain.NotificationBackupFailed); err == nil {
		t.Fatal("loopback admitted without allowlist")
	}
}
func TestTransportSlack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got map[string]string
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		if len(got) != 1 || got["text"] != "info ns/db Recovered: back online" {
			t.Errorf("payload %v", got)
		}
	}))
	defer server.Close()
	channel := domain.NotificationChannel{Type: "slack", Secrets: domain.NotificationChannelSecrets{SlackWebhookURL: server.URL}}
	if err := transportSender(server.URL).Send(t.Context(), channel, domain.Event{Namespace: "ns", Cluster: "db", Reason: "Recovered", Message: "back\nonline"}, domain.NotificationClusterRecovered); err != nil {
		t.Fatal(err)
	}
}
func TestTransportHTTPFailuresAndBodyCap(t *testing.T) {
	for _, status := range []int{302, 403, 503, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			targetCalled := false
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalled = true }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target.URL)
				w.WriteHeader(status)
				if status == 200 {
					io.WriteString(w, strings.Repeat("x", 8192))
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				} else {
					io.WriteString(w, "secret-response-token")
				}
			}))
			defer server.Close()
			channel := domain.NotificationChannel{Type: "webhook", Secrets: domain.NotificationChannelSecrets{WebhookURL: server.URL + "/?secret-url-token"}}
			started := time.Now()
			err := transportSender(server.URL).Send(t.Context(), channel, domain.Event{}, "")
			if status == 200 {
				if err != nil {
					t.Fatal(err)
				}
				if time.Since(started) > time.Second*2 {
					t.Fatal("read beyond body cap")
				}
			} else if err == nil || strings.Contains(err.Error(), "token") {
				t.Fatalf("unsafe or missing error %v", err)
			}
			if status == 302 && err.Error() != "notification HTTP status 302" {
				t.Fatal(err)
			}
			if status == 403 && err.Error() != "notification HTTP status 403" {
				t.Fatal(err)
			}
			if status == 503 && err.Error() != "notification HTTP status 503" {
				t.Fatal(err)
			}
			if targetCalled {
				t.Fatal("followed redirect")
			}
		})
	}
}
