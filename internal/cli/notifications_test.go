// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNotificationChannelCommands(t *testing.T) {
	for _, tc := range []struct {
		args                []string
		input, method, path string
		check               func(*testing.T, map[string]any)
	}{
		{args: []string{"list"}, method: "GET", path: ""},
		{args: []string{"get", "alerts"}, method: "GET", path: "/alerts"},
		{args: []string{"create", "--name", "alerts", "--type", "webhook", "--url", "https://alerts.example.com/hook", "--enabled=false", "--category", "backup-failed", "--category", "cluster-degraded", "--namespace", "one", "--namespace", "two", "--signing-secret-stdin"}, input: "signing-key\n", method: "POST", check: func(t *testing.T, b map[string]any) {
			if b["name"] != "alerts" || b["enabled"] != false || len(b["categories"].([]any)) != 2 || len(b["namespaces"].([]any)) != 2 || nested(b, "webhook", "signingSecret") != "signing-key" {
				t.Fatal(b)
			}
		}},
		{args: []string{"replace", "alerts", "--type", "slack", "--webhook-url-stdin"}, input: "https://hooks.slack.com/secret\n", method: "PUT", path: "/alerts", check: func(t *testing.T, b map[string]any) {
			if nested(b, "slack", "webhookUrl") != "https://hooks.slack.com/secret" {
				t.Fatal(b)
			}
		}},
		{args: []string{"create", "--name", "alerts", "--type", "email", "--smtp-host", "mail.example.com", "--smtp-port", "465", "--security", "tls", "--username", "sender", "--password-stdin", "--from", "alerts@example.com", "--to", "a@example.com", "--to", "b@example.com"}, input: "mail-secret\n", method: "POST", check: func(t *testing.T, b map[string]any) {
			if nested(b, "email", "password") != "mail-secret" || nested(b, "email", "port") != float64(465) || len(nested(b, "email", "to").([]any)) != 2 {
				t.Fatal(b)
			}
		}},
		{args: []string{"replace", "alerts", "--type", "webhook"}, method: "PUT", path: "/alerts", check: func(t *testing.T, b map[string]any) {
			if _, exists := b["webhook"].(map[string]any)["signingSecret"]; exists {
				t.Fatal("omitted secret was sent")
			}
		}},
		{args: []string{"delete", "alerts", "--yes"}, method: "DELETE", path: "/alerts"},
		{args: []string{"test", "alerts"}, method: "POST", path: "/alerts/test"},
	} {
		t.Run(strings.Join(tc.args[:1], "")+tc.path+tc.input, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != tc.method || r.URL.Path != "/api/v1/notification-channels"+tc.path || r.URL.RawQuery != "" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				if tc.check != nil {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					tc.check(t, body)
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.args[0] == "test" {
					_, _ = w.Write([]byte(`{"delivered":true,"error":""}`))
				} else {
					_, _ = w.Write([]byte(`{"name":"alerts","type":"webhook","enabled":true}`))
				}
			}))
			defer srv.Close()
			args := append([]string{"notification-channels"}, tc.args...)
			args = append(args, "-o", "json")
			_, stderr, err := execute(t, srv.URL, filepath.Join(t.TempDir(), "config"), tc.input, args...)
			if err != nil || stderr != "" || calls != 1 {
				t.Fatalf("err=%v stderr=%q calls=%d", err, stderr, calls)
			}
		})
	}
}

func TestNotificationChannelListPagination(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/notification-channels" || r.URL.Query().Get("limit") != "17" || r.URL.Query().Get("cursor") != "next-page" {
			t.Errorf("unexpected paginated request %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[],"nextCursor":"later-page"}`))
	}))
	defer srv.Close()
	out, stderr, err := execute(t, srv.URL, filepath.Join(t.TempDir(), "config"), "", "notification-channels", "list", "--limit", "17", "--cursor", "next-page", "-o", "json")
	if err != nil || stderr != "" || !strings.Contains(out, "later-page") {
		t.Fatalf("out=%q err=%v stderr=%q", out, err, stderr)
	}
}

func TestNotificationFilesAndOutput(t *testing.T) {
	file := filepath.Join(t.TempDir(), "channel.yaml")
	if err := os.WriteFile(file, []byte("name: alerts\ntype: slack\nenabled: true\nslack:\n  webhookUrl: https://hooks.slack.com/private\n"), 0600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" && r.URL.Path == "/api/v1/notification-channels" {
			var b map[string]any
			_ = json.NewDecoder(r.Body).Decode(&b)
			if nested(b, "slack", "webhookUrl") != "https://hooks.slack.com/private" {
				t.Error(b)
			}
		}
		if strings.HasSuffix(r.URL.Path, "/test") {
			_, _ = w.Write([]byte(`{"delivered":false,"error":"delivery failed"}`))
			return
		}
		channel := `{"name":"alerts","type":"webhook","enabled":true,"categories":["backup-failed"],"webhook":{"host":"alerts.example.com"},"status":{"pending":2,"failed":1,"lastError":"delivery failed"}}`
		if r.Method == "GET" && r.URL.Path == "/api/v1/notification-channels" {
			_, _ = w.Write([]byte(`{"items":[` + channel + `]}`))
		} else {
			_, _ = w.Write([]byte(channel))
		}
	}))
	defer srv.Close()
	for _, args := range [][]string{{"create", "-f", file}, {"replace", "alerts", "-f", "-"}, {"list"}, {"get", "alerts"}, {"get", "alerts", "-o", "yaml"}, {"test", "alerts"}} {
		command := append([]string{"notification-channels"}, args...)
		out, stderr, err := execute(t, srv.URL, filepath.Join(t.TempDir(), "config"), "name: alerts\ntype: webhook\nenabled: true\n", command...)
		if err != nil || stderr != "" {
			t.Fatalf("%v: %v %s", args, err, stderr)
		}
		if args[0] == "test" {
			if !strings.Contains(out, "Delivered: false") || !strings.Contains(out, "delivery failed") {
				t.Fatal(out)
			}
		} else if !strings.Contains(out, "alerts") || strings.Contains(out, "private") {
			t.Fatal(out)
		}
	}
}

func TestNotificationInvalidFlags(t *testing.T) {
	for _, args := range [][]string{{"create", "--type", "slack"}, {"create", "--name", "alerts", "--type", "invalid"}, {"replace", "alerts", "--name", "other", "--type", "slack"}, {"create", "--name", "alerts", "--type", "slack", "--password-stdin"}, {"create", "--name", "alerts", "--type", "slack", "--webhook-url-stdin", "--password-stdin"}, {"create", "--name", "alerts", "--type", "email", "--security", "none"}, {"create", "--name", "alerts", "--type", "email", "--smtp-port", "0"}, {"create", "--name", "alerts", "--type", "slack", "--webhook-url-stdin"}} {
		command := append([]string{"notification-channels"}, args...)
		_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config"), "", command...)
		if err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
