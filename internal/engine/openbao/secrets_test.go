// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"testing"
)

func (s *edgeSecrets) DeleteAgentToken(_ context.Context, _ domain.ClusterRef, name string) error {
	if s.tokens[name] == nil {
		return engine.ErrNotFound
	}
	delete(s.tokens, name)
	return nil
}
func (s *edgeSecrets) DeleteClusterSecret(_ context.Context, _ domain.ClusterRef, key string) error {
	if _, ok := s.values[key]; !ok {
		return engine.ErrNotFound
	}
	delete(s.values, key)
	return nil
}
func TestEdgeDeleteMemberSecrets(t *testing.T) {
	_, rc, _ := edgeFixture(t)
	for _, kind := range []string{"members", "agents"} {
		if err := putSecret(t.Context(), rc, "tls/"+kind+"/baom0.pending.pem", []byte("pending")); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := deleteMemberSecrets(t.Context(), rc, "baom0"); err != nil {
			t.Fatal(err)
		}
	}
	s := rc.Secrets.(*edgeSecrets)
	if s.tokens["baom0"] != nil {
		t.Fatal("agent token retained")
	}
	for _, kind := range []string{"members", "agents"} {
		for _, suffix := range []string{".pem", ".pending.pem"} {
			if _, ok := s.values["tls/"+kind+"/baom0"+suffix]; ok {
				t.Fatal("member TLS retained")
			}
		}
	}
	if len(s.values["tls/ca.crt"]) == 0 || s.tokens["baom1"] == nil || len(s.values["tls/members/baom1.pem"]) == 0 {
		t.Fatal("cleanup removed cluster or other member material")
	}
}
