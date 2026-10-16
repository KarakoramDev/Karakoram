// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"bytes"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
)

func fakeBackend(t *testing.T, f *fakeAPI, config map[string]string) *Backend {
	t.Helper()
	settings := map[string]string{"url": f.server.URL, "registryId": "registry", "publicIPv4": "192.0.2.1", "publicIPv6": "2001:db8::1", "domainSuffix": "apps.example.test"}
	for key, value := range config {
		if value == "" {
			delete(settings, key)
		} else {
			settings[key] = value
		}
	}
	factory := newFactory(func(key string) string {
		if key == "KARAKORAM_DOKPLOY_ALLOWED_URLS" {
			return f.server.URL
		}
		return ""
	})
	backend, err := factory.New(infra.Target{Kind: "dokploy", ID: "environment", Token: fakeAPIKey, Config: settings})
	if err != nil {
		t.Fatal(err)
	}
	return backend.(*Backend)
}

func testDeployment(t *testing.T, volume string) infra.Deployment {
	t.Helper()
	return infra.Deployment{
		Artifact: infra.Artifact{Reader: bytes.NewReader(renderTestArtifact(t)), Name: "karakoram.tar.gz", Binary: "karakoram"},
		Runtime:  infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Volume: volume, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}, StopBeforeStart: true},
	}
}

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Harness {
		f := newFakeAPI(t)
		return contracttest.Harness{
			Backend:      fakeBackend(t, f, nil),
			Hostname:     func(s string) string { return "kkcontract" + s },
			Artifact:     func() infra.Artifact { return testDeployment(t, "").Artifact },
			PollInterval: time.Millisecond,
			Timeout:      3 * time.Second,
		}
	})
}
