// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestOpenBaoConnectionTargetResolver(t *testing.T) {
	s, _, _ := openbaoTestServer(t)
	target, e := s.openbaoConnectionTarget(t.Context(), domain.ClusterRef{Name: "database"})
	if e != nil {
		t.Fatal(e)
	}
	if target.Provider.Name() != "postgres" || target.Context.Cluster.Name != "database" || target.Context.Backend == nil || target.Context.Namespace != domain.DefaultNamespace {
		t.Fatalf("unresolved target: %+v", target)
	}
	if _, e = s.openbaoConnectionTarget(t.Context(), domain.ClusterRef{Name: "missing"}); e == nil {
		t.Fatal("missing target resolved")
	}
}
