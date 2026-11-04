// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"slices"
	"testing"
)

func TestEdgeRelayTargets(t *testing.T) {
	if RelayTargets(nil) != nil {
		t.Fatal("nil cluster has targets")
	}
	c := &domain.Cluster{Name: "bao"}
	c.Spec.Components.Engine.Replicas = 1
	want := []string{"baom0:8200", "baom0:8202", "baom0:9443"}
	if got := RelayTargets(c); !slices.Equal(got, want) {
		t.Fatalf("targets %v", got)
	}
	c.Status.Members = []domain.MemberStatus{{Name: "baom2"}}
	want = append(want, "baom2:8200", "baom2:8202", "baom2:9443")
	slices.Sort(want)
	if got := RelayTargets(c); !slices.Equal(got, want) {
		t.Fatalf("targets %v", got)
	}
}
func TestEdgeCertificateRotationKeepsRestartEpoch(t *testing.T) {
	_, rc, s := edgeFixture(t)
	m := s.Members[0]
	before, _ := secret(t.Context(), rc, "tls/members/"+m.Name+".pem")
	generation := m.ProcessGeneration
	if err := rotateCertificate(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	after, _ := secret(t.Context(), rc, "tls/members/"+m.Name+".pem")
	if string(before) == string(after) {
		t.Fatal("certificate did not rotate")
	}
	if m.ProcessGeneration != generation {
		t.Fatal("certificate rotation changed restart epoch")
	}
}
