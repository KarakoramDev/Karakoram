// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

// RestorePreparedCPUpgrade preserves the cluster address captured by existing
// native/agent test-server closures while using the canonical fixture loader.
func RestorePreparedCPUpgrade(t *testing.T, rc *engine.Context, path string) *CPUpgradeFixture {
	t.Helper()
	original := rc.Cluster
	fixture := RestoreCPUpgrade(t, rc, path)
	*original = *rc.Cluster
	rc.Cluster = original
	return fixture
}

// PreparedServices projects frozen service observations into map-backed fakes.
func PreparedServices(fixture *CPUpgradeFixture) map[string]*infra.Service {
	services := make(map[string]*infra.Service, len(fixture.Services))
	for i := range fixture.Services {
		service := &fixture.Services[i]
		if service.Ownership == nil {
			service.Ownership = map[string]string{}
		}
		services[service.Hostname] = service
	}
	return services
}

// RequirePreparedConfig compares JSON values, ignoring fixture indentation.
func RequirePreparedConfig(t *testing.T, name string, before, after json.RawMessage) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(before, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(after, &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("CP-only upgrade changed prepared engine config for %s", name)
	}
}

// LegacyNativeServer connects the fake native API to the historical test CA.
func LegacyNativeServer(t *testing.T, cert, key []byte, names []string, handler http.Handler) string {
	t.Helper()
	ca, err := pki.LoadCA(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	crt, priv, err := ca.Issue(pki.CertRequest{DNSNames: names, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(crt, priv)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}
