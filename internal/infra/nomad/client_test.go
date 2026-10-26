// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestClientRequests(t *testing.T) {
	type observation struct{ method, token, namespace, region, index, wait, contentType, body string }
	observations := make(chan observation, 8)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observations <- observation{r.Method, r.Header.Get("X-Nomad-Token"), r.URL.Query().Get("namespace"), r.URL.Query().Get("region"), r.URL.Query().Get("index"), r.URL.Query().Get("wait"), r.Header.Get("Content-Type"), string(body)}
		w.Header().Set("X-Nomad-Index", "42")
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	config := server.Client().Transport.(*http.Transport).TLSClientConfig
	c := newClient(server.URL, "global", "tenant", "first-secret", config)
	ctx := context.Background()
	var out struct {
		OK bool `json:"ok"`
	}
	index, err := c.get(ctx, "/v1/jobs", nil, &out)
	if err != nil || index != 42 || !out.OK {
		t.Fatalf("get = %d, %+v, %v", index, out, err)
	}
	got := <-observations
	if got.token != "first-secret" || got.namespace != "tenant" || got.region != "global" {
		t.Fatalf("request: %+v", got)
	}
	c.setToken("new-secret")
	query := url.Values{"namespace": {"other"}, "region": {"other-region"}}
	index, err = c.getBlocking(ctx, "/v1/jobs", query, 31, 2*time.Second, &out)
	if err != nil || index != 42 {
		t.Fatalf("blocking = %d, %v", index, err)
	}
	got = <-observations
	if got.token != "new-secret" || got.namespace != "other" || got.region != "other-region" || got.index != "31" || got.wait != "2s" {
		t.Fatalf("blocking request: %+v", got)
	}
	if query.Get("index") != "" || query.Get("wait") != "" {
		t.Fatal("caller query mutated")
	}
	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete} {
		switch method {
		case http.MethodPut:
			err = c.put(ctx, "/v1/jobs", nil, map[string]int{"value": 1}, &out)
		case http.MethodPost:
			err = c.post(ctx, "/v1/jobs", nil, map[string]int{"value": 1}, &out)
		case http.MethodDelete:
			err = c.delete(ctx, "/v1/jobs", nil, &out)
		}
		if err != nil {
			t.Fatal(err)
		}
		got = <-observations
		if got.method != method {
			t.Fatalf("method %q", got.method)
		}
		if method != http.MethodDelete && (got.body != `{"value":1}` || got.contentType != "application/json") {
			t.Fatalf("JSON request: %+v", got)
		}
	}
	c.setToken("")
	data, err := c.getRaw(ctx, "/v1/logs", nil, 100)
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("raw = %q, %v", data, err)
	}
	if got = <-observations; got.token != "" {
		t.Fatal("empty token was sent")
	}
}

func TestClientErrorMapping(t *testing.T) {
	tests := []struct {
		status int
		body   string
		class  error
	}{
		{401, "bad token", infra.ErrUnauthorized}, {403, "Permission denied", infra.ErrUnauthorized},
		{400, "ACL token not found", infra.ErrUnauthorized}, {400, "invalid job", infra.ErrRejected},
		{422, "invalid job", infra.ErrRejected}, {404, "not found", infra.ErrNotFound}, {409, "conflict", infra.ErrConflict},
		{429, "slow down", infra.ErrUnavailable}, {500, "failed", infra.ErrUnavailable}, {503, "failed", infra.ErrUnavailable},
		{302, "redirect", infra.ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d-%s", tt.status, tt.body), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				fmt.Fprintf(w, "%s\n secret-token https://private.example.com/secret\t%s", tt.body, strings.Repeat("z", 500))
			}))
			defer server.Close()
			c := newClient(server.URL, "", "tenant", "secret-token", nil)
			_, err := c.get(context.Background(), "/v1/jobs", nil, nil)
			if !errors.Is(err, tt.class) {
				t.Fatalf("error = %v; want %v", err, tt.class)
			}
			var api *APIError
			if !errors.As(err, &api) || api.Status != tt.status {
				t.Fatalf("not APIError: %v", err)
			}
			for _, detail := range []string{err.Error(), infra.SafeDetail(err), api.Message} {
				if len(detail) > 300 || strings.Contains(detail, "secret-token") || strings.Contains(detail, "https://") || strings.Contains(detail, "private.example.com") || strings.ContainsAny(detail, "\n\t") {
					t.Fatalf("unsafe detail: %q", detail)
				}
			}
			if tt.class == infra.ErrRejected {
				var rejected *infra.RejectedError
				if !errors.As(err, &rejected) {
					t.Fatal("missing neutral rejection details")
				}
			}
		})
	}
}

func TestClientNoRedirect(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true; fmt.Fprint(w, `{}`) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	c := newClient(server.URL, "", "tenant", "secret", nil)
	c.http = server.Client() // Replacing the transport must not bypass redirect policy.
	_, err := c.get(context.Background(), "/v1/jobs", nil, nil)
	if err == nil || followed {
		t.Fatalf("redirect followed=%v, error=%v", followed, err)
	}
}

func TestClientTransportFailures(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer server.Close()
	c := newClient(server.URL, "", "tenant", "secret-token", &tls.Config{MinVersion: tls.VersionTLS12})
	_, err := c.get(context.Background(), "/v1/jobs", nil, nil)
	if !errors.Is(err, infra.ErrConfiguration) || !strings.Contains(infra.SafeDetail(err), "KARAKORAM_NOMAD_CACERT") {
		t.Fatalf("untrusted certificate = %v", err)
	}
	server.Close()
	_, err = c.get(context.Background(), "/v1/jobs", nil, nil)
	assertSilentUnavailable(t, err, server.URL)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	c = newClient(slow.URL, "", "tenant", "secret-token", nil)
	c.http.Timeout = 10 * time.Millisecond
	_, err = c.get(context.Background(), "/v1/jobs", nil, nil)
	assertSilentUnavailable(t, err, slow.URL)
}

func TestClientTLSHostnameConfigurationError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request reached server despite invalid TLS identity")
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	c := newClient(server.URL, "", "tenant", "secret-token", &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "secret-requested-name.example.test",
	})
	_, err := c.get(t.Context(), "/v1/jobs", url.Values{"private": {"secret-query"}}, nil)
	if !errors.Is(err, infra.ErrConfiguration) || errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("TLS error classification = %v", err)
	}
	detail := infra.SafeDetail(err)
	if !strings.Contains(detail, "TLS verification failed") || !strings.Contains(detail, "KARAKORAM_NOMAD_TLS_SERVER_NAME") || !strings.Contains(detail, "SANs") {
		t.Fatalf("missing TLS remediation: %q", detail)
	}
	for _, text := range []string{detail, err.Error()} {
		if len(text) > 300 {
			t.Fatalf("unbounded diagnostic: %q", text)
		}
		for _, secret := range []string{"secret-token", "secret-query", "secret-requested-name", server.URL, "BEGIN CERTIFICATE"} {
			if strings.Contains(text, secret) {
				t.Fatalf("unsafe TLS diagnostic: %q", text)
			}
		}
	}
}

func TestNomadTLSTransportErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"hostname", x509.HostnameError{}, "KARAKORAM_NOMAD_TLS_SERVER_NAME"},
		{"authority", x509.UnknownAuthorityError{}, "KARAKORAM_NOMAD_CACERT"},
		{"expired", x509.CertificateInvalidError{Reason: x509.Expired}, "system clock"},
		{"invalid", x509.CertificateInvalidError{Reason: x509.IncompatibleUsage}, "KARAKORAM_NOMAD_CACERT"},
		{"verification", &tls.CertificateVerificationError{Err: errors.New("private certificate details")}, "TLS verification failed"},
		{"bad certificate alert", tls.AlertError(42), "KARAKORAM_NOMAD_CLIENT_CERT"},
		{"unknown CA alert", tls.AlertError(48), "KARAKORAM_NOMAD_CLIENT_KEY"},
		{"remote bad certificate", errors.New("remote error: tls: bad certificate"), "KARAKORAM_NOMAD_CLIENT_CERT"},
		{"remote unknown CA", errors.New("remote error: tls: unknown certificate authority"), "KARAKORAM_NOMAD_CLIENT_KEY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := transportError(&url.Error{Op: "Get", URL: "https://private.example.test/secret-token", Err: tc.err})
			if !errors.Is(err, infra.ErrConfiguration) || errors.Is(err, infra.ErrRejected) || !strings.Contains(infra.SafeDetail(err), tc.want) {
				t.Fatalf("TLS classification/remediation = %v", err)
			}
			for _, text := range []string{err.Error(), infra.SafeDetail(err)} {
				if len(text) > 300 || strings.Contains(text, "private") || strings.Contains(text, "secret-token") || strings.Contains(text, "https://") {
					t.Fatalf("unsafe diagnostic: %q", text)
				}
			}
		})
	}
	for _, cause := range []error{errors.New("connection refused"), tls.AlertError(80), errors.New("remote error: tls: internal error")} {
		err := transportError(cause)
		if !errors.Is(err, infra.ErrUnavailable) || infra.SafeDetail(err) != "nomad: network request failed" {
			t.Fatalf("ordinary network error changed classification: %v", err)
		}
	}
}

func assertSilentUnavailable(t *testing.T, err error, origin string) {
	t.Helper()
	if !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	if strings.Contains(err.Error(), origin) || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("unsafe transport error: %v", err)
	}
}

func TestClientResponseLimitsAndJSON(t *testing.T) {
	for _, tt := range []struct {
		name, body, index string
		raw               bool
	}{
		{name: "raw oversized", body: "12345", raw: true},
		{name: "invalid JSON", body: "not JSON"},
		{name: "invalid index", body: `{}`, index: "invalid"},
		{name: "JSON oversized", body: strings.Repeat("x", int(jsonResponseLimit)+1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Nomad-Index", tt.index)
				fmt.Fprint(w, tt.body)
			}))
			defer server.Close()
			c := newClient(server.URL, "", "tenant", "", nil)
			var err error
			if tt.raw {
				var data []byte
				data, err = c.getRaw(context.Background(), "/v1/logs", nil, 4)
				if err != nil || string(data) != "1234" {
					t.Fatalf("bounded raw response = %q, %v", data, err)
				}
				return
			} else {
				var out map[string]any
				_, err = c.get(context.Background(), "/v1/jobs", nil, &out)
			}
			if !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("want unavailable, got %v", err)
			}
		})
	}
}

func TestClientVariableErrorsNeverExposeBodies(t *testing.T) {
	const secret = "private-variable-value"
	for _, path := range []string{"/v1/var/nomad/jobs/runtime", "/v1/vars"} {
		for _, status := range []int{400, 403, 409, 500} {
			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
				t.Run(fmt.Sprintf("%s-%s-%d", method, path, status), func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(status)
						fmt.Fprintf(w, `{"Items":{"PASSWORD":%q}}`, secret)
					}))
					t.Cleanup(server.Close)
					c := newClient(server.URL, "", "tenant", "", nil)
					_, err := c.requestJSON(t.Context(), method, path, nil, nil, nil, 0)
					var api *APIError
					if !errors.As(err, &api) || api.Status != status {
						t.Fatalf("variable failure = %v", err)
					}
					if status == 409 && (!errors.Is(err, infra.ErrConflict) || api.Message != "stale variable index") {
						t.Fatalf("CAS conflict = %v", err)
					}
					for _, detail := range []string{err.Error(), infra.SafeDetail(err), api.Message} {
						if strings.Contains(detail, secret) || strings.Contains(detail, "Items") || strings.Contains(detail, "PASSWORD") {
							t.Fatalf("variable response reached diagnostic: %q", detail)
						}
					}
				})
			}
		}
	}
}
