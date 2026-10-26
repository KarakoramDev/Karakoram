// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestKeycloakPublicHTTPSAdmission(t *testing.T) {
	for _, supported := range []bool{false, true} {
		t.Run(fmt.Sprintf("publicHttps=%t", supported), func(t *testing.T) {
			s, _, token := testServer(t)
			var err error
			s.Registry, err = engine.NewRegistry(keycloak.New(keycloak.Options{}))
			if err != nil {
				t.Fatal(err)
			}
			s.namespaces = engine.StaticNamespaces{DefaultBackend: infrafake.New(infrafake.Options{
				Capabilities: infra.Capabilities{PublicHTTPS: supported, Logs: true},
			})}
			publicSpec := `{"provider":"keycloak","engineOptions":{"database":{"cluster":"appdb"},"publicHttps":true,"hostname":"https://login.example.com","proxyHeaders":"xforwarded"}}`
			assertAdmission := func(method, path, body string, accepted int) {
				t.Helper()
				w := call(s, method, path, body, token)
				if supported {
					if w.Code != accepted {
						t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
					}
				} else if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_request") || !strings.Contains(w.Body.String(), "does not provide publicHttps") {
					t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
				}
			}
			create := `{"name":"identity","spec":` + publicSpec + `}`
			assertAdmission("POST", "/api/v1/dry-run/clusters", create, 200)
			assertAdmission("POST", "/api/v1/clusters", create, 201)
			private := `{"name":"private","spec":{"provider":"keycloak","engineOptions":{"database":{"cluster":"appdb"}}}}`
			if w := call(s, "POST", "/api/v1/clusters", private, token); w.Code != 201 {
				t.Fatal(w.Code, w.Body.String())
			}
			for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
				assertAdmission("PUT", prefix+"private", `{"spec":`+publicSpec+`}`, 200)
				assertAdmission("PATCH", prefix+"private", `{"spec":{"engineOptions":{"publicHttps":true,"hostname":"https://login.example.com","proxyHeaders":"xforwarded"}}}`, 200)
			}
			if !supported {
				c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "private"})
				if err != nil || c.Generation != 1 {
					t.Fatalf("rejected update changed state: %+v %v", c, err)
				}
			}
		})
	}
}
