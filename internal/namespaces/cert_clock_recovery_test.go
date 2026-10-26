// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package namespaces

import (
	"bytes"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/relay"
)

func TestRelayFutureLeafRenewsAfterClockCorrection(t *testing.T) {
	r, backend, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	cert, key, err := relay.IssueClientCertificate(r.pki.CACert, r.pki.CAKey, n.Name, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := relay.ParseCertificatePEM(cert)
	if err != nil {
		t.Fatal(err)
	}
	n.ClientCert, n.ClientSerial, n.Relay.CertNotAfter = cert, leaf.SerialNumber.String(), leaf.NotAfter
	token, err := r.o.Store.GetNamespaceTokenForUpdate(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.o.Store.UpdateNamespaceCredentials(t.Context(), n, token, key); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(t.Context(), n, backend); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(n.ClientCert, cert) {
		t.Fatal("future relay identity was not repaired")
	}
	renewed, err := relay.ParseCertificatePEM(n.ClientCert)
	if err != nil || time.Now().Before(renewed.NotBefore) {
		t.Fatal("renewed relay identity is not currently valid", err)
	}
	if backend.imports != 0 || backend.restarts != 0 {
		t.Fatal("credential renewal unexpectedly changed runtime")
	}
}
