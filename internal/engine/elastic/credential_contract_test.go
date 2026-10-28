// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 1)
	recorded := enginetest.RecordCredentials(rc)
	s := &state{Members: []*member{{Name: "demon0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Native password >=6, product >=8; native username <=507 printable ASCII.
	// https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-user
	pw := enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}
	// Kibana keys >=32: https://www.elastic.co/docs/reference/kibana/configuration-reference/security-settings
	key := enginetest.CredentialLimit{Min: 32, Alphabet: enginetest.Alphanumeric}
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
	pw := enginetest.CredentialLimit{Min: 8, Alphabet: enginetest.Alphanumeric}
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
		"users/karakoram-mcp-ro": pw, "users/karakoram-mcp-rw": pw,
	}, 507)
}
