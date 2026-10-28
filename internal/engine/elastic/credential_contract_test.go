// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

var generatedPasswordLimit = enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}
var generatedKibanaKeyLimit = enginetest.CredentialLimit{Min: 32, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 1)
	recorded := enginetest.RecordCredentials(rc)
	s := &state{Members: []*member{{Name: "demon0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Native password >=6, product >=8; native username <=507 printable ASCII.
	// https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-user
	pw := generatedPasswordLimit
	// Kibana keys >=32: https://www.elastic.co/docs/reference/kibana/configuration-reference/security-settings
	key := generatedKibanaKeyLimit
	// Service-account tokens/API keys are opaque native engine outputs, excluded.
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/karakoram": pw, "users/karakoram-monitor": pw, "users/elastic": pw,
		"users/karakoram_logstash": pw, "users/karakoram_apm": pw, "apm/secret-token": pw,
		"kibana/keys/security": key, "kibana/keys/savedObjects": key, "kibana/keys/reporting": key,
	}, 507)
}

func TestProviderGeneratedDataCredentialContracts(t *testing.T) {
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		opsJSON(w, map[string]any{})
	})
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	recorded := enginetest.RecordCredentials(rc)
	client, err := p.es(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ensureDataAccess(t.Context(), rc, client); err != nil {
		t.Fatal(err)
	}
	// Same native user API contract as the mandatory system users above.
	pw := generatedPasswordLimit
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
		"users/karakoram-mcp-ro": pw, "users/karakoram-mcp-rw": pw,
	}, 507)
}

func TestRuntimeRotationCredentialContracts(t *testing.T) {
	for _, user := range []string{"elastic", "karakoram-monitor", "logstash", "apm", "apm-secret-token", engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		t.Run(user, func(t *testing.T) {
			native := map[string]string{}
			p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut || r.Method == http.MethodPost {
					if strings.HasPrefix(r.URL.Path, "/_security/user/") {
						var body struct {
							Password string `json:"password"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Error(err)
						}
						if body.Password != "" {
							native[r.URL.Path] = body.Password
						}
					}
				}
				if r.URL.Path == "/_security/_authenticate" {
					opsJSON(w, map[string]string{"username": "elastic/kibana"})
				} else if r.URL.Path == "/v1/status" {
					opsJSON(w, agentapi.Status{ConfigVersion: 10000})
				} else {
					opsJSON(w, map[string]any{})
				}
			})
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
			// opsFixture supplies the TLS server trust but no signing key; only
			// prepare the additional secrets consumed by these runtime paths.
			for _, key := range []string{secretMonitor, secretAPMToken} {
				if err := ensureSecret(t.Context(), rc, key, secrets.Password); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.ensureSecurity(t.Context(), rc, s, true); err != nil {
				t.Fatal(err)
			}
			key, _ := rotationTarget(user)
			old, err := secret(t.Context(), rc, key)
			if err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "contract-" + user, Kind: domain.OperationRotateSystemUser}
			cp := operationState{}
			done := false
			var staged string
			for range 30 {
				result, err := p.runRotateUser(t.Context(), rc, s, op, cp, user)
				if err != nil {
					t.Fatal(err)
				}
				cp = opsCheckpoint(t, result)
				if cp.Stage == "install" {
					value, err := secret(t.Context(), rc, "operations/"+op.ID+"/rotation")
					if err != nil {
						t.Fatal(err)
					}
					staged = string(value)
					generatedPasswordLimit.Assert(t, "staged "+user, staged)
				}
				if result.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("credential rotation did not complete")
			}
			value, err := secret(t.Context(), rc, key)
			if err != nil {
				t.Fatal(err)
			}
			generatedPasswordLimit.Assert(t, key, string(value))
			if string(value) != staged || string(value) == string(old) {
				t.Fatal("rotation did not install the generated credential")
			}
			nativePath := map[string]string{
				"elastic":                      "/_security/user/elastic/_password",
				"logstash":                     "/_security/user/karakoram_logstash",
				"apm":                          "/_security/user/karakoram_apm",
				engine.DataAccessReadUser:      "/_security/user/" + engine.DataAccessReadUser,
				engine.DataAccessReadWriteUser: "/_security/user/" + engine.DataAccessReadWriteUser,
			}[user]
			if nativePath != "" {
				generatedPasswordLimit.Assert(t, nativePath, native[nativePath])
				if native[nativePath] != string(value) {
					t.Fatal("native password differs from persisted credential")
				}
			}
			if user == "karakoram-monitor" {
				m := s.Members[0]
				// MemberConfig projects PEM bytes; this test does not start agents.
				for _, kind := range []string{"agents/" + m.Name, "nodes/" + m.Name + "/http", "nodes/" + m.Name + "/transport"} {
					for _, suffix := range []string{".crt", ".key"} {
						if err := putSecret(t.Context(), rc, "tls/"+kind+suffix, []byte("fixture-pem")); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := p.publishConfig(t.Context(), rc, s, m, rc.Cluster.Spec.Version, "contract"); err != nil {
					t.Fatal(err)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				config, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec agentspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				generatedPasswordLimit.Assert(t, "native monitor password", spec.Elasticsearch.Monitor.Password)
				if spec.Elasticsearch.Monitor.Password != string(value) {
					t.Fatal("native monitor password differs from persisted password")
				}
			}
			if user == "apm" || user == "apm-secret-token" {
				spec, err := p.desiredSpec(t.Context(), rc, s, &member{Name: "testapm", Kind: kindAPM}, rc.Cluster.Spec.Version)
				if err != nil {
					t.Fatal(err)
				}
				projected := spec.APM.Writer.Password
				if user == "apm-secret-token" {
					projected = spec.APM.SecretToken
				}
				generatedPasswordLimit.Assert(t, "native APM credential", projected)
				if projected != string(value) {
					t.Fatal("native APM credential differs from persisted credential")
				}
			}
			if user == engine.DataAccessReadUser || user == engine.DataAccessReadWriteUser {
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				level := engine.DataAccessRead
				if user == engine.DataAccessReadWriteUser {
					level = engine.DataAccessReadWrite
				}
				returned, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				generatedPasswordLimit.Assert(t, "returned data password", returned.Password)
				if returned.Password != string(value) {
					t.Fatal("returned data password differs from native password")
				}
			}
		})
	}
}

func TestKibanaGeneratedKeyProjectionContracts(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 1)
	s := &state{Members: []*member{{Name: "demon0", Kind: kindNode}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// ES service tokens are native outputs, not Karakoram-generated credentials.
	if err := putSecret(t.Context(), rc, secretKibanaToken, []byte("native-token")); err != nil {
		t.Fatal(err)
	}
	spec, err := p.desiredSpec(t.Context(), rc, s, &member{Name: "demokb", Kind: kindKibana}, rc.Cluster.Spec.Version)
	if err != nil {
		t.Fatal(err)
	}
	for key, projected := range map[string]string{secretKibanaSecurity: spec.Kibana.SecurityKey, secretKibanaSavedObjects: spec.Kibana.SavedObjectsKey, secretKibanaReporting: spec.Kibana.ReportingKey} {
		generatedKibanaKeyLimit.Assert(t, key, projected)
		persisted, err := secret(t.Context(), rc, key)
		if err != nil {
			t.Fatal(err)
		}
		generatedKibanaKeyLimit.Assert(t, key, string(persisted))
		if projected != string(persisted) {
			t.Fatal("native Kibana key differs from persisted key")
		}
	}
}
