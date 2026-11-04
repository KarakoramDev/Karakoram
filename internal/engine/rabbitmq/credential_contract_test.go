// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

// RabbitMQ's default validator requires nonempty passwords, with no native maximum.
var generatedPasswordLimit = enginetest.CredentialLimit{Min: 1, Alphabet: enginetest.Alphanumeric}
var generatedCookieLimit = enginetest.CredentialLimit{Min: 1, Max: 255, Alphabet: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	recorded := enginetest.RecordCredentials(rc)
	s := &internalState{Members: []*member{{Name: "demom0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Default validator accepts nonempty passwords; no native maximum is specified.
	// https://www.rabbitmq.com/docs/passwords
	pw := generatedPasswordLimit
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/karakoram": pw, "users/admin": pw, "users/monitor": pw,
		"users/karakoram-mcp-ro": pw, "users/karakoram-mcp-rw": pw,
		// RabbitMQ cookie <=255 alphanumeric; generated unpadded RFC4648 base32 subset.
		// https://www.rabbitmq.com/docs/clustering#erlang-cookie
		"erlang-cookie": generatedCookieLimit,
	}, 0)
}
