// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"crypto/tls"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestInternalTLSPreservesPersistedRelaySANs(t *testing.T) {
	s, sealer, _ := testServer(t)
	initial, ca, err := internalTLS(t.Context(), s.Store, sealer, "control", "oldrelay")
	if err != nil {
		t.Fatal(err)
	}
	before, err := initial.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"development", "staging"} {
		hostname := "oldrelay"
		if name == "staging" {
			hostname = "stagerelay"
		}
		if err = s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: name, Relay: domain.NamespaceRelayStatus{Hostname: hostname}}); err != nil {
			t.Fatal(err)
		}
	}
	config, again, err := internalTLS(t.Context(), s.Store, sealer, "control", "newrelay")
	if err != nil {
		t.Fatal(err)
	}
	after, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if ca != again {
		t.Fatal("SAN repair changed installation CA")
	}
	if before.Leaf.SerialNumber.Cmp(after.Leaf.SerialNumber) == 0 {
		t.Fatal("missing persisted SAN did not reissue leaf")
	}
	for _, name := range []string{"control", "oldrelay", "stagerelay", "newrelay"} {
		if err = after.Leaf.VerifyHostname(name); err != nil {
			t.Fatal("required relay SAN missing", name, err)
		}
	}
	restarted, _, err := internalTLS(t.Context(), s.Store, sealer, "control", "newrelay")
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := restarted.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || unchanged.Leaf.SerialNumber.Cmp(after.Leaf.SerialNumber) != 0 {
		t.Fatal("complete persisted SAN leaf was unnecessarily replaced", err)
	}
}
