// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestConnectionFailureDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		response *http.Response
		want     string
	}{
		{"untrusted", &url.Error{Op: "Get", URL: "secret", Err: x509.UnknownAuthorityError{}}, nil, "authority is not trusted"},
		{"hostname", x509.HostnameError{Host: "secret"}, nil, "does not match the dial hostname"},
		{"validity", x509.CertificateInvalidError{}, nil, "outside its validity period"},
		{"dns", &net.DNSError{Err: "secret", Name: "secret"}, nil, "DNS resolution failed"},
		{"dns timeout", &url.Error{Op: "Get", URL: "secret", Err: &net.DNSError{Err: "secret", Name: "secret", IsTimeout: true}}, nil, "timed out"},
		{"refused", &url.Error{Op: "Get", URL: "secret", Err: &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("secret: %w", syscall.ECONNREFUSED)}}, nil, "TCP connection refused"},
		{"unauthorized", errors.New("secret"), &http.Response{StatusCode: 401}, "authentication/authorization rejected (HTTP 401)"},
		{"forbidden", errors.New("secret"), &http.Response{StatusCode: 403}, "authentication/authorization rejected (HTTP 403)"},
		{"timeout", fmt.Errorf("secret: %w", context.DeadlineExceeded), nil, "timed out"},
		{"upgrade", errors.New("secret"), &http.Response{StatusCode: 502}, "WebSocket upgrade rejected (HTTP 502)"},
		{"unknown", errors.New("secret"), nil, "connection failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := connectionFailure("outer HTTPS/WebSocket", "wss://user:secret@cp.example:8443/secret?secret=secret", tc.err, tc.response).Error()
			if !strings.Contains(got, tc.want) || !strings.Contains(got, "cp.example:8443") || strings.Contains(got, "secret") {
				t.Fatalf("unsafe or unhelpful diagnostic: %s", got)
			}
		})
	}
}

func TestConnectReportsUntrustedOuterTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("untrusted TLS must fail before HTTP or inner enrollment")
	}))
	defer server.Close()
	endpoint := "wss" + strings.TrimPrefix(server.URL, "https") + "/relay/v1/connect"
	r := runtime{o: defaults(Options{HandshakeTimeout: time.Second})}
	err := r.connect(t.Context(), endpoint, nil)
	if err == nil || !strings.Contains(err.Error(), "outer HTTPS/WebSocket") || !strings.Contains(err.Error(), "authority is not trusted") {
		t.Fatalf("missing TLS failure diagnosis: %v", err)
	}
}

func TestInnerTLSAuthorityDiagnostics(t *testing.T) {
	err := connectionFailure("inner mTLS", "wss://cp.example/relay/v1/connect", &url.Error{Op: "Get", URL: "secret", Err: x509.UnknownAuthorityError{}}, nil)
	if !strings.Contains(err.Error(), "KARAKORAM_RELAY_CA") || !strings.Contains(err.Error(), "hub enrollment") || strings.Contains(err.Error(), "runtime trust store") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("incorrect inner TLS remediation: %v", err)
	}
}

func TestConnectionFailureDiagnosticBounds(t *testing.T) {
	err := connectionFailure("outer HTTPS/WebSocket", "wss://user:secret@"+strings.Repeat("a", 10000)+".example/secret?secret=secret", x509.UnknownAuthorityError{}, nil)
	if len(err.Error()) > 512 || !strings.Contains(err.Error(), "...: TLS certificate authority") || !strings.Contains(err.Error(), "runtime trust store") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unbounded or incomplete diagnostic (%d bytes): %v", len(err.Error()), err)
	}
}
