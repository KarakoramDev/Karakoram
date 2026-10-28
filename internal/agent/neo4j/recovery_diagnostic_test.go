// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestRecoveryNativeDiagnosticsNeverExposeQueryInputs(t *testing.T) {
	const secret = "private-password-and-statement"
	for _, tc := range []struct {
		code, class string
	}{
		{"Neo.ClientError.Statement.ArgumentError", "invalid-argument"},
		{"Neo.ClientError.Security.UserNotFound", "user-missing"},
		{"Neo.ClientError.Security.CredentialsExpired", "password-change-required"},
		{"Neo.ClientError.Cluster.NotALeader", "not-leader"},
		{"Neo.TransientError.General.DatabaseUnavailable", "database-unavailable"},
		{"Neo.DatabaseError.General.UnknownError", "native-error"},
		{"Neo.ClientError." + secret, "native-error"},
		{"Neo.ClientError.Statement.ArgumentError\n" + secret, "native-error"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			s := coreTestSpec(t)
			pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Statement  string            `json:"statement"`
					Parameters map[string]string `json:"parameters"`
				}
				if r.URL.Path != "/db/system/query/v2" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Statement != "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED" || body.Parameters["p"] != secret {
					t.Error("unexpected recovery query")
				}
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": tc.code, "message": secret + " ALTER USER neo4j SET PASSWORD '" + secret + "'"}}})
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			_, portText, _ := net.SplitHostPort(server.Listener.Addr().String())
			port, _ := strconv.Atoi(portText)
			err = recoverAdminPassword(t.Context(), s, port, secret)
			if err == nil {
				t.Fatal("failed native ALTER was accepted")
			}
			diagnostic := err.Error()
			if strings.Contains(diagnostic, secret) || strings.Contains(diagnostic, "ALTER") || strings.ContainsAny(diagnostic, "\r\n") {
				t.Fatal("recovery diagnostic leaked native inputs")
			}
			if !strings.Contains(diagnostic, "category=native code="+nativeStatusCode(tc.code)) || !strings.Contains(diagnostic, "class="+tc.class) {
				t.Fatalf("missing safe native diagnostic: %s", diagnostic)
			}
		})
	}
}

func TestRecoveryRequestDiagnosticsNeverExposeTransportErrors(t *testing.T) {
	const secret = "https://user:private-password@private-host/query"
	for _, tc := range []struct {
		err   error
		class string
	}{
		{errors.New(secret), "transport"},
		{fmt.Errorf("%s: %w", secret, context.Canceled), "canceled"},
		{fmt.Errorf("%s: %w", secret, context.DeadlineExceeded), "timeout"},
	} {
		diagnostic := recoveryFailureDiagnostic(classifyTransportError(tc.err)).Error()
		if !strings.Contains(diagnostic, "category=request class="+tc.class) || strings.Contains(diagnostic, secret) {
			t.Fatalf("unsafe or missing request category: %s", diagnostic)
		}
	}
	if got := recoveryFailureDiagnostic(errors.New(secret)).Error(); got != "Neo4j credential recovery failed: category=unknown" {
		t.Fatal("unknown recovery error was not sanitized")
	}
	if got := recoveryFailureDiagnostic(errCredentials).Error(); !strings.Contains(got, "class=authentication httpStatus=401") {
		t.Fatal("missing authentication category")
	}
	for _, tc := range []struct {
		status int
		body   string
		class  string
	}{
		{http.StatusServiceUnavailable, secret, "http-rejected"},
		{http.StatusAccepted, secret, "invalid-response"},
	} {
		s := coreTestSpec(t)
		pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
		server.StartTLS()
		_, portText, _ := net.SplitHostPort(server.Listener.Addr().String())
		port, _ := strconv.Atoi(portText)
		err = recoverAdminPassword(t.Context(), s, port, secret)
		server.Close()
		if err == nil || !strings.Contains(err.Error(), "category=request class="+tc.class+" httpStatus="+strconv.Itoa(tc.status)) || strings.Contains(err.Error(), secret) {
			t.Fatal("missing or unsafe response diagnostic")
		}
	}
}
