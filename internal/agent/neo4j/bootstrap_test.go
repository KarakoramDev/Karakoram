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

func TestInterruptedBootstrapPasswordRecovery(t *testing.T) {
	for _, mode := range []string{"initial store", "same password change required", "transport before commit", "committed response lost", "clear interrupted", "missing user", "unverified result", "change still required"} {
		t.Run(mode, func(t *testing.T) {
			s := coreTestSpec(t)
			pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
			if err != nil {
				t.Fatal(err)
			}
			current := "initial-password"
			required := true
			if mode == "same password change required" || mode == "clear interrupted" {
				current = s.AdminPassword
			}
			interrupted := false
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Statement  string            `json:"statement"`
					Parameters map[string]string `json:"parameters"`
				}
				if r.URL.Path != "/db/system/query/v2" || r.Header.Get("Authorization") != "" || json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("unexpected recovery request")
					w.WriteHeader(400)
					return
				}
				switch body.Statement {
				case "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED":
					if mode == "transport before commit" && !interrupted {
						interrupted = true
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
					if mode == "missing user" {
						_, _ = w.Write([]byte(`{"errors":[{"code":"Neo.ClientError.Security.UserNotFound","message":"private native text"}]}`))
						return
					}
					if body.Parameters["p"] != s.AdminPassword {
						t.Error("wrong managed password")
					}
					if current == s.AdminPassword {
						_, _ = w.Write([]byte(`{"errors":[{"code":"Neo.ClientError.Statement.ArgumentError","message":"Failed to alter the specified user 'neo4j': Old password and new password cannot be the same."}]}`))
						return
					}
					current, required = s.AdminPassword, false
					if mode == "committed response lost" && !interrupted {
						interrupted = true
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
				case "ALTER USER neo4j SET PASSWORD CHANGE NOT REQUIRED":
					if mode == "clear interrupted" && !interrupted {
						interrupted = true
						w.WriteHeader(503)
						return
					}
					required = false
				case "SHOW USERS YIELD user, passwordChangeRequired WHERE user = 'neo4j' RETURN passwordChangeRequired":
					if mode == "unverified result" {
						_, _ = w.Write([]byte(`{"data":{"fields":[],"values":[]}}`))
						return
					}
					if mode == "change still required" {
						required = true
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"fields": []string{"passwordChangeRequired"}, "values": [][]any{{required}}}})
					return
				default:
					t.Error("unexpected statement")
				}
				_, _ = w.Write([]byte(`{"data":{"fields":[],"values":[]}}`))
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()
			_, text, _ := net.SplitHostPort(server.Listener.Addr().String())
			port, _ := strconv.Atoi(text)
			err = recoverAdminPassword(t.Context(), s, port, s.AdminPassword)
			if mode == "committed response lost" || mode == "clear interrupted" || mode == "transport before commit" {
				if err == nil {
					t.Fatal("ambiguous request must not publish bootstrap success")
				}
				err = recoverAdminPassword(t.Context(), s, port, s.AdminPassword)
			}
			if mode == "missing user" || mode == "unverified result" || mode == "change still required" {
				if err == nil {
					t.Fatal("unverified administrator must not publish bootstrap success")
				}
				if strings.Contains(err.Error(), "private native text") {
					t.Fatal("native text escaped")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if current != s.AdminPassword || required {
				t.Fatal("recovery succeeded without the complete administrator postcondition")
			}
		})
	}
}
