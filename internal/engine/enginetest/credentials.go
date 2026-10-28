// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const Alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// CredentialLimit describes an output contract. Max == 0 means no documented
// native maximum, not a claim that every possible input is accepted.
type CredentialLimit struct {
	Min, Max         int
	Alphabet, Prefix string
}

func (l CredentialLimit) Assert(t *testing.T, name, value string) {
	t.Helper()
	if len(value) < l.Min || l.Max > 0 && len(value) > l.Max || strings.Trim(value, l.Alphabet) != "" || !strings.HasPrefix(value, l.Prefix) {
		t.Errorf("%s violates length/charset contract (length %d, want %d..%d)", name, len(value), l.Min, l.Max)
	}
}

// AgentCredential is Karakoram's bearer format, not a native engine password.
var AgentCredential = CredentialLimit{51, 51, Alphanumeric + "_", "kk_"}

// CredentialStore observes real provider writes while retaining the fixture's
// persistence behavior. TLS material is transport identity, not a password.
// Native OpenBao /sys/init root tokens/shares and Elasticsearch service-account
// tokens/API keys are engine-generated opaque outputs: no bounds are invented.
type CredentialStore struct {
	engine.SecretStore
	Values map[string][]byte
	Tokens map[string]string
}

func RecordCredentials(rc *engine.Context) *CredentialStore {
	s := &CredentialStore{SecretStore: rc.Secrets, Values: map[string][]byte{}, Tokens: map[string]string{}}
	rc.Secrets = s
	return s
}

func (s *CredentialStore) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if err := s.SecretStore.SetClusterSecret(ctx, ref, key, value); err != nil {
		return err
	}
	if !strings.HasPrefix(key, "tls/") {
		s.Values[key] = append([]byte(nil), value...)
	}
	return nil
}

func (s *CredentialStore) SetAgentToken(ctx context.Context, token *domain.AgentToken) error {
	if err := s.SecretStore.SetAgentToken(ctx, token); err != nil {
		return err
	}
	s.Tokens[token.Member] = token.Token
	return nil
}

// AssertValues rejects every unlisted persisted credential, including future
// keys. Every declared key must be written, preventing vacuous seeding tests.
func (s *CredentialStore) AssertValues(t *testing.T, limits map[string]CredentialLimit, usernameMax int) {
	t.Helper()
	for key, value := range s.Values {
		limit, ok := limits[key]
		if !ok {
			t.Errorf("persisted credential %q has no declared contract", key)
			continue
		}
		limit.Assert(t, key, string(value))
		if usernameMax > 0 && strings.HasPrefix(key, "users/") {
			name := strings.TrimPrefix(key, "users/")
			if len(name) == 0 || len(name) > usernameMax || strings.Trim(name, Alphanumeric+"_-") != "" {
				t.Errorf("system username %q violates identifier contract (maximum %d bytes)", name, usernameMax)
			}
		}
	}
	for key := range limits {
		if _, ok := s.Values[key]; !ok {
			t.Errorf("credential %q was not generated", key)
		}
	}
}

// Assert also requires and checks credentials persisted in the agent-token store.
func (s *CredentialStore) Assert(t *testing.T, limits map[string]CredentialLimit, usernameMax int) {
	t.Helper()
	s.AssertValues(t, limits, usernameMax)
	if len(s.Tokens) == 0 {
		t.Error("provider did not persist an agent credential")
	}
	for member, token := range s.Tokens {
		AgentCredential.Assert(t, "agent/"+member, token)
		CredentialLimit{48, 48, Alphanumeric, ""}.Assert(t, "agent body/"+member, strings.TrimPrefix(token, "kk_"))
	}
}
