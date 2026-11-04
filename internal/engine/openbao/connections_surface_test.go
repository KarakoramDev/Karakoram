// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
)

func TestConnectionHyphenatedNamesDoNotCollide(t *testing.T) {
	for _, provider := range []string{"postgres", "psmdb", "rabbitmq"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			target, _ := connectionTargetFixture(rc)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			target.Context.Cluster.Spec.Provider = provider
			target.Context.Cluster.Spec.Topology.Type = "replicaSet"
			target.Context.Cluster.Status.Connection.Hosts = []string{"target"}
			if provider == "psmdb" {
				target.Provider = &connectionMongo{}
			}
			if provider == "rabbitmq" {
				target.Provider = &connectionRabbit{}
			}
			if err := target.Context.Secrets.SetClusterSecret(t.Context(), target.Context.Cluster.Ref(), "tls/ca.crt", []byte("ca")); err != nil {
				t.Fatal(err)
			}
			var rolePaths []string
			allowed := map[string]string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/sys/mounts" {
					_, _ = w.Write([]byte(`{"data":{}}`))
					return
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if raw := body["allowed_roles"]; raw != nil {
					var roles []string
					if err := json.Unmarshal(raw, &roles); err != nil || len(roles) != 1 {
						t.Errorf("allowed roles: %s", raw)
					} else {
						allowed[r.URL.Path] = roles[0]
					}
				}
				if body["creation_statements"] != nil || body["vhosts"] != nil {
					rolePaths = append(rolePaths, r.URL.Path)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client, err := baoapi.New(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			var paths []string
			for i, name := range []string{"app", "app-prod"} {
				role := []string{"prod-reader", "reader"}[i]
				request := DatabaseConnectionWrite{Name: name, TargetCluster: "target", Roles: []DatabaseRoleWrite{{Name: role, Access: AccessReadOnly}}}
				if err := saveConnectionCredential(t.Context(), rc, name, connectionCredential{Username: name, Step: "identity"}); err != nil {
					t.Fatal(err)
				}
				connection, err := p.CreateDatabaseConnection(t.Context(), rc, target, request)
				if err != nil {
					t.Fatal(err)
				}
				expected := "database/creds/" + name + "_" + role
				if provider == "rabbitmq" {
					expected = "rabbitmq-" + name + "/creds/" + role
				}
				if connection.Roles[0].CredentialsPath != expected {
					t.Fatalf("credentials path: %s, want %s", connection.Roles[0].CredentialsPath, expected)
				}
				paths = append(paths, expected)
				if err := p.configureConnection(t.Context(), rc, target, client, connection, connectionCredential{}); err != nil {
					t.Fatal(err)
				}
				if provider != "rabbitmq" && allowed["/v1/database/config/"+name] != name+"_*" {
					t.Fatalf("allowed_roles not isolated: %v", allowed)
				}
			}
			if paths[0] == paths[1] || len(rolePaths) != 2 || rolePaths[0] == rolePaths[1] {
				t.Fatalf("colliding role paths: %v / %v", paths, rolePaths)
			}
		})
	}
}
