// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func TestUserNameLengthMatchesPublicContract(t *testing.T) {
	for _, length := range []int{1, 63, 64, 65} {
		_, _, err := userStatement(neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: strings.Repeat("a", length), Password: "password-value"})
		if (err == nil) != (length <= 64) {
			t.Fatalf("%d-character user validation mismatch", length)
		}
	}
}
func TestSanitizedNativeUserErrors(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		status        int
	}{
		{"Neo.ClientError.General.InvalidArguments", "User already exists: private-query-input", http.StatusConflict},
		{"Neo.ClientError.General.InvalidArguments", "User does not exist: private-query-input", http.StatusNotFound},
		{"Neo.ClientError.General.InvalidArguments", "Password is too short: private-query-input", http.StatusBadRequest},
		{"Neo.ClientError.Statement.ArgumentError", "User already exists: private-query-input", http.StatusConflict},
		{"Neo.ClientError.Statement.SyntaxError", "private-query-input", http.StatusBadRequest},
		{"Neo.DatabaseError.General.UnknownError", "private-query-input", http.StatusServiceUnavailable},
	} {
		err := classifyQueryError(tc.code, tc.message)
		status, message := userErrorResponse(err)
		if status != tc.status {
			t.Fatalf("native %s status %d, want %d", tc.code, status, tc.status)
		}
		if strings.Contains(err.Error(), "private-query-input") || strings.Contains(message, "private-query-input") {
			t.Fatal("native error exposed query inputs")
		}
	}
	if status, _ := userErrorResponse(errors.New("private-query-input")); status != http.StatusServiceUnavailable {
		t.Fatal("unexpected transport error mapping")
	}
}
func TestQueryClassifiesHTTPAndQueryAPIErrorEnvelopes(t *testing.T) {
	s := coreTestSpec(t)
	pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusAccepted} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": "Neo.ClientError.General.InvalidArguments", "message": "User already exists: secret-password-value"}}})
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			_, portText, _ := net.SplitHostPort(server.Listener.Addr().String())
			port, _ := strconv.Atoi(portText)
			result, err := query(t.Context(), s, port, s.AdminPassword, "system", "CREATE USER `application` SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": "secret-password-value"})
			var failure *queryFailure
			if !errors.As(err, &failure) || failure.code != "Neo.ClientError.General.InvalidArguments" {
				t.Fatal("native query code was not preserved safely")
			}
			responseStatus, message := userErrorResponse(err)
			if responseStatus != http.StatusConflict || len(result.Errors) != 0 || strings.Contains(message, "secret-password-value") || strings.Contains(err.Error(), "secret-password-value") {
				t.Fatal("query error classification leaked native diagnostics or lost status")
			}
		})
	}
}
