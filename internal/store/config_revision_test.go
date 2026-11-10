// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestConfigSecretRevision(t *testing.T) {
	s, _ := testStore(t)
	ref := domain.ClusterRef{Name: "demo"}
	read := func() [32]byte {
		t.Helper()
		v, err := s.ConfigSecretRevision(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	empty := read()
	if err := s.SetClusterSecret(t.Context(), ref, "config", []byte("one")); err != nil {
		t.Fatal(err)
	}
	first := read()
	if first == empty || read() != first {
		t.Fatal("secret insert or stable revision lost")
	}
	if err := s.SetClusterSecret(t.Context(), ref, "config", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if read() == first {
		t.Fatal("secret replacement did not invalidate revision")
	}
	if err := s.DeleteClusterSecret(t.Context(), ref, "config"); err != nil {
		t.Fatal(err)
	}
	if read() != empty {
		t.Fatal("secret deletion did not invalidate revision")
	}
	if err := s.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: ref.Name, Member: "member", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	if read() == empty {
		t.Fatal("token change did not invalidate revision")
	}
}
