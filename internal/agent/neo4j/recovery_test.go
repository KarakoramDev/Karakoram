// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestRecoverAdminPasswordAlreadyRestored(t *testing.T) {
	const unchanged = "Failed to alter the specified user 'neo4j': Old password and new password cannot be the same."
	for _, tc := range []struct {
		name, code, message string
		status              int
		wantOK              bool
	}{
		{"changed", "", "", http.StatusAccepted, true},
		{"same password", "Neo.ClientError.Statement.ArgumentError", unchanged, http.StatusAccepted, true},
		{"same password HTTP error", "Neo.ClientError.Statement.ArgumentError", unchanged, http.StatusBadRequest, true},
		{"other argument", "Neo.ClientError.Statement.ArgumentError", "Password rejected: private-password", http.StatusBadRequest, false},
		{"other user", "Neo.ClientError.Statement.ArgumentError", strings.Replace(unchanged, "'neo4j'", "'application'", 1), http.StatusBadRequest, false},
		{"unrecognized code", "Neo.DatabaseError.General.UnknownError", unchanged, http.StatusBadRequest, false},
		{"unrecognized diagnostic", "Neo.ClientError.Statement.ArgumentError", unchanged + " private-password", http.StatusBadRequest, false},
		{"transport rejection", "", "", http.StatusServiceUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				if r.URL.Path != "/db/system/query/v2" || r.Header.Get("Authorization") != "" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Statement != "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED" || body.Parameters["p"] != "private-password" {
					t.Error("unexpected recovery request")
				}
				w.WriteHeader(tc.status)
				if tc.code == "" {
					_, _ = w.Write([]byte(`{"data":{"fields":[],"values":[]}}`))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": tc.code, "message": tc.message}}})
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			_, portText, _ := net.SplitHostPort(server.Listener.Addr().String())
			port, _ := strconv.Atoi(portText)
			err = recoverAdminPassword(t.Context(), s, port, "private-password")
			if (err == nil) != tc.wantOK {
				t.Fatalf("recovery result %v, want success %v", err, tc.wantOK)
			}
			if err != nil && strings.Contains(err.Error(), "private-password") {
				t.Fatal("recovery exposed native query input")
			}
		})
	}
}
