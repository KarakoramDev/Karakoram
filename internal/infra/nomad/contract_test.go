// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"archive/tar"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
)

func fakeArchive(t *testing.T) []byte {
	t.Helper()
	return testArchive(t, "karakoram", "#!/bin/sh\nexit 0\n", tar.TypeReg, 1)
}

func fakeBackend(t *testing.T, f *fakeNomad, config map[string]string) *Backend {
	t.Helper()
	archive := fakeArchive(t)
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(archive) }))
	t.Cleanup(mirror.Close)
	settings := map[string]string{"address": f.URL, "publicIPv4": "192.0.2.1", "publicIPv6": "2001:db8::1"}
	for key, value := range config {
		settings[key] = value
	}
	b, err := newFactory(testEnv(nil), WithHTTPClient(f.Client()), WithAllowedAddrs(f.URL), WithArtifactURL(mirror.URL)).New(infra.Target{Kind: "nomad", ID: "default", Token: fakeNomadToken, Config: settings})
	if err != nil {
		t.Fatal(err)
	}
	return b.(*Backend)
}

func testDeployment(t *testing.T, volume string) infra.Deployment {
	t.Helper()
	return infra.Deployment{Artifact: testArtifact(fakeArchive(t)), Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Volume: volume, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}, StopBeforeStart: true}}
}

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Harness {
		f := newFakeNomad(t)
		return contracttest.Harness{Backend: fakeBackend(t, f, nil), Hostname: func(s string) string { return "kkcontract" + s }, Artifact: func() infra.Artifact { return testArtifact(fakeArchive(t)) }, PollInterval: time.Millisecond, Timeout: 3 * time.Second}
	})
}
