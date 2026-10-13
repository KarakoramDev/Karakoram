// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T, key string) json.RawMessage {
	t.Helper()
	data, err := os.ReadFile("testdata/api_responses.json")
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]json.RawMessage
	if err = json.Unmarshal(data, &all); err != nil {
		t.Fatal(err)
	}
	v, ok := all[key]
	if !ok {
		t.Fatalf("missing fixture %s", key)
	}
	return v
}
func reply(t *testing.T, w http.ResponseWriter, key string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(fixture(t, key))
}
func newTestBackend(t *testing.T, h http.HandlerFunc) *Backend {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	b, err := New(Options{BaseURL: s.URL, Token: "test-token", ProjectID: "ExampleID0000000000077", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestClientExactResponses(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing bearer")
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/service-stack/import"):
			reply(t, w, "import")
		case strings.HasSuffix(r.URL.Path, "/app-version"):
			reply(t, w, "appVersion")
		case strings.HasSuffix(r.URL.Path, "/upload"):
			data, _ := io.ReadAll(r.Body)
			if string(data) != "gzip-tar" || r.ContentLength != 8 || r.Header.Get("Content-Type") != "application/octet-stream" {
				t.Error("incorrect raw upload")
			}
			reply(t, w, "upload")
		case strings.HasSuffix(r.URL.Path, "/deploy"):
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["zeropsYaml"] != "zerops: []" || body["zeropsYamlSetup"] != "test" {
				t.Error("incorrect deploy body")
			}
			reply(t, w, "deploy")
		case strings.HasSuffix(r.URL.Path, "/env"):
			reply(t, w, "env")
		case strings.HasSuffix(r.URL.Path, "/user-data"):
			reply(t, w, "createSecret")
		case strings.HasPrefix(r.URL.Path, "/user-data/"):
			reply(t, w, "updateSecret")
		case strings.HasSuffix(r.URL.Path, "/autoscaling"):
			reply(t, w, "autoscaling")
		case strings.HasSuffix(r.URL.Path, "/container"):
			reply(t, w, "containers")
		case strings.HasPrefix(r.URL.Path, "/process/"):
			reply(t, w, "finished")
		default:
			reply(t, w, "project")
		}
	})
	ctx := context.Background()
	c := b.client
	imp, err := c.ImportServices(ctx, "services: []")
	if err != nil || len(imp.ServiceStacks) == 0 || len(imp.ServiceStacks[0].Processes) == 0 {
		t.Fatalf("import: %v", err)
	}
	version, err := c.CreateAppVersion(ctx, "sid", "test")
	if err != nil || version.ID == "" {
		t.Fatalf("app version: %v", err)
	}
	if err = c.Upload(ctx, version.ID, strings.NewReader("gzip-tar"), 8); err != nil {
		t.Fatal(err)
	}
	if p, e := c.Deploy(ctx, version.ID, "zerops: []", "test"); e != nil || p.ID == "" {
		t.Fatalf("deploy: %v", e)
	}
	if env, e := c.Env(ctx, "sid"); e != nil || len(env) == 0 {
		t.Fatalf("env: %v", e)
	}
	for _, id := range []string{"", "existing"} {
		if p, e := c.SetUserData(ctx, "sid", id, "key", "value"); e != nil || p.ID == "" {
			t.Fatalf("secret: %v", e)
		}
	}
	if p, e := c.Autoscaling(ctx, "sid", map[string]json.RawMessage{}); e != nil || p == nil || p.ID == "" {
		t.Fatalf("autoscale: %v", e)
	}
	if v, e := c.Containers(ctx, "sid"); e != nil || len(v) != 1 || v[0].Hostname == "" {
		t.Fatalf("containers: %v", e)
	}
	if v, e := c.ProjectInfo(ctx); e != nil || v.PublicIPv6 == "" || v.PublicIPv4 != "" {
		t.Fatalf("project: %v", e)
	}
}
func TestPaginationFiltersCore(t *testing.T) {
	var raw page[json.RawMessage]
	_ = json.Unmarshal(fixture(t, "list"), &raw)
	var offsets []string
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		offsets = append(offsets, r.URL.Query().Get("offset"))
		p := page[json.RawMessage]{Total: len(raw.List), Limit: 2}
		switch r.URL.Query().Get("offset") {
		case "0":
			p.List = raw.List[:2]
		case "2":
			p.List = raw.List[2:4]
		case "4":
			p.List = raw.List[4:]
		}
		_ = json.NewEncoder(w).Encode(p)
	})
	list, err := b.client.ListServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 3 || offsets[1] != "2" || offsets[2] != "4" {
		t.Fatalf("pagination: %v", offsets)
	}
	for _, s := range list {
		if s.protected() {
			t.Fatal("protected service leaked")
		}
	}
}
func TestTypedErrorAndRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
		retry  bool
		want   int
	}{{"GET", 500, true, 3}, {"PUT", 429, true, 3}, {"POST", 500, true, 1}, {"DELETE", 500, true, 1}, {"PUT", 500, false, 1}, {"GET", 400, true, 1}} {
		t.Run(tc.method+http.StatusText(tc.status), func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				reply(t, w, "error")
			}))
			defer s.Close()
			c, err := NewClient(Options{BaseURL: s.URL, Token: "secret-token", ProjectID: "p", MaxRetries: 2, RetryBase: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			err = c.json(context.Background(), tc.method, "/test", struct{}{}, nil, tc.retry)
			var api *APIError
			if !errors.As(err, &api) || api.Code != "serviceStackNameInvalid" || api.Message == "" || len(api.Meta) == 0 {
				t.Fatalf("typed error: %v", err)
			}
			if int(calls.Load()) != tc.want {
				t.Fatalf("calls %d want %d", calls.Load(), tc.want)
			}
			if strings.Contains(err.Error(), "secret-token") {
				t.Fatal("credential leaked")
			}
		})
	}
}

type brokenTransport struct{ calls atomic.Int32 }

func (b *brokenTransport) RoundTrip(*http.Request) (*http.Response, error) {
	b.calls.Add(1)
	return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("network with secret URL")}
}
func TestNetworkRetryCancellationAndRedaction(t *testing.T) {
	tr := &brokenTransport{}
	c, err := NewClient(Options{Token: "token", ProjectID: "p", HTTPClient: &http.Client{Transport: tr}, MaxRetries: 2, RetryBase: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = c.json(context.Background(), "GET", "/x", nil, nil, true)
	if tr.calls.Load() != 3 || strings.Contains(err.Error(), "secret URL") {
		t.Fatalf("network error %v calls %d", err, tr.calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = c.json(ctx, "GET", "/x", nil, nil, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation %v", err)
	}
}
func TestSignedLogsNoBearerOrCredentialErrors(t *testing.T) {
	logFixture, err := os.ReadFile("testdata/logs.json")
	if err != nil {
		t.Fatal(err)
	}
	var s *httptest.Server
	s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/log") {
			_ = json.NewEncoder(w).Encode(map[string]string{"url": s.URL + "/signed?token=secret"})
			return
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("bearer sent to log host")
		}
		if r.URL.Query().Get("facility") != "16" || r.URL.Query().Get("serviceStackId") != "sid" || r.URL.Query().Get("limit") != "10" {
			t.Error("bad logging query")
		}
		_, _ = w.Write(logFixture)
	}))
	defer s.Close()
	c, _ := NewClient(Options{BaseURL: s.URL, Token: "test-token", ProjectID: "p"})
	logs, err := c.Logs(context.Background(), "sid", 10)
	if err != nil || len(logs) != 10 || logs[0].Content != "healthcheck is enabled" {
		t.Fatalf("logs: %v", err)
	}
}
func TestRedirectDoesNotExposeCredentials(t *testing.T) {
	var touched bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { touched = true }))
	defer destination.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer s.Close()
	c, _ := NewClient(Options{BaseURL: s.URL, Token: "test-token", ProjectID: "p"})
	_, err := c.ProjectInfo(context.Background())
	if err == nil || touched {
		t.Fatal("redirect followed")
	}
}
