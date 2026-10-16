// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestSpecChangeHandlersMaskTLSAndOpaqueConfig(t *testing.T) {
	for _, method := range []string{"PATCH", "PUT"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			for i := range 2 {
				ca, err := pki.NewCA("test-certificate", time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
				if err != nil {
					t.Fatal(err)
				}
				spec := map[string]any{}
				if method == "PUT" {
					data, err := json.Marshal(specAPI(c.Spec))
					if err != nil {
						t.Fatal(err)
					}
					if err = json.Unmarshal(data, &spec); err != nil {
						t.Fatal(err)
					}
				}
				spec["engineConfig"] = "credential = confidential-config-value"
				tls, ok := spec["tls"].(map[string]any)
				if !ok {
					tls = map[string]any{}
				}
				tls["certificate"], tls["privateKey"] = string(ca.CertPEM), string(ca.KeyPEM)
				spec["tls"] = tls
				body, err := json.Marshal(map[string]any{"spec": spec})
				if err != nil {
					t.Fatal(err)
				}
				w := call(s, method, "/api/v1/clusters/prod", string(body), token)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				events := auditEvents(t, s, "ClusterSpecChangeRequested")
				if len(events) != i+1 {
					t.Fatal("missing audit", len(events))
				}
				event := events[i]
				data, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				for _, value := range []string{"BEGIN CERTIFICATE", "PRIVATE KEY", "confidential-config-value"} {
					if strings.Contains(string(data), value) {
						t.Fatal("handler persisted secret material", value)
					}
				}
				var details specDetails
				if err = json.Unmarshal(event.Details, &details); err != nil {
					t.Fatal(err)
				}
				expected := 2
				if i == 0 {
					expected = 3
				}
				if c.Spec.Backup.Schedules == nil {
					expected++
				}
				if c.Spec.Exposure.Allowlist == nil {
					expected++
				}
				if len(details.Changes) != expected {
					t.Fatal("incorrect secret diff", string(event.Details))
				}
				for _, change := range details.Changes {
					if !change.Sensitive && change.Path != "backup.schedules" && change.Path != "exposure.allowlist" {
						t.Fatal("secret change not marked sensitive", change)
					}
				}
				if i == 1 {
					w = call(s, method, "/api/v1/clusters/prod", string(body), token)
					if w.Code != 200 {
						t.Fatal(w.Code, w.Body.String())
					}
					events = auditEvents(t, s, "ClusterSpecChangeRequested")
					if len(events) != 3 || events[2].Message != "Cluster spec change requested for prod: no changes" {
						t.Fatal("identical secret inputs were not idempotent", events)
					}
				}
			}
		})
	}
}

func TestSpecDiffSensitiveFreeFormKeys(t *testing.T) {
	for _, key := range []string{"password", "passwd", "pass", "dbPass", "passphrase", "pwd", "webhook", "auth", "authHeader", "clientAuth", "clientAuthHeader", "authentication", "authorization", "server.ssl.keyPassphrase", "xpack.security.transport.ssl.key_passphrase", "secretAccessKey", "access_key_id", "client-secret", "privateKey", "private_key", "apiToken", "credential.connection", "nested.password.value", "key material", "signingKey", "encryptionKey", "keyFile", "keyStore.password", "trustStore", "key", "pem", "config", "pipelineConfig", "pipelineSource", "pipelineSources"} {
		t.Run(key, func(t *testing.T) {
			before := map[string]any{"engineOptions": map[string]any{key: "old-confidential-value"}}
			after := map[string]any{"engineOptions": map[string]any{key: "new-confidential-value"}}
			var changes []specChange
			diffSpecValues("", before, after, &changes)
			data, err := json.Marshal(changes)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != 1 || !changes[0].Sensitive || strings.Contains(string(data), "confidential-value") || strings.Contains(specChangeText(changes[0]), "confidential-value") {
				t.Fatal("sensitive free-form key leaked", string(data))
			}
		})
	}
	for _, key := range []string{"author", "AUTHOR", "pipelineAuthor", "compass"} {
		if sensitiveSpecPath("engineOptions." + key) {
			t.Errorf("non-sensitive key %q was masked", key)
		}
	}
	for _, tc := range []struct{ name, before, after, path string }{
		{"id pipeline", `{"logstash":{"pipelines":[{"id":"main","config":"old-confidential-value"}]}}`, `{"logstash":{"pipelines":[{"id":"main","config":"input { kafka { sasl_jaas_config => 'password=new-confidential-value' } }"}]}}`, "engineOptions.logstash.pipelines[main].config"},
		{"pipeline map source", `{"logstash":{"pipelines":{"main":"old-confidential-value"}}}`, `{"logstash":{"pipelines":{"main":"new-confidential-value"}}}`, "engineOptions.logstash.pipelines.main"},
		{"pipeline scalar array", `{"logstash":{"pipelines":["old-confidential-value"]}}`, `{"logstash":{"pipelines":["new-confidential-value"]}}`, "engineOptions.logstash.pipelines"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := domain.ClusterSpec{EngineOptions: json.RawMessage(tc.before)}
			after := domain.ClusterSpec{EngineOptions: json.RawMessage(tc.after)}
			event := clusterSpecAuditEvent("demo", 2, before, after, nil)
			data, err := json.Marshal(event)
			if err != nil || strings.Contains(string(data), "confidential-value") || strings.Contains(string(data), "sasl_jaas_config") {
				t.Fatal("pipeline credentials leaked", err, string(data))
			}
			var details specDetails
			if err = json.Unmarshal(event.Details, &details); err != nil {
				t.Fatal(err)
			}
			if len(details.Changes) != 1 || details.Changes[0].Path != tc.path || !details.Changes[0].Sensitive || details.Changes[0].Before != "(changed)" || details.Changes[0].After != "(changed)" {
				t.Fatal("pipeline source was not a masked field-level change", string(event.Details))
			}
		})
	}
}
