// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestNativeCertificateRecoveryBypassesWriterlessProxy(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	// A desired options revision was already hashed, but a failed proxy kept
	// it from reaching data members. Identity recovery must not acknowledge it.
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"log_min_duration_statement": "250"})
	rc.Cluster.Generation++
	b.statusOverrides = map[string]pgspec.StatusResponse{"demopx": {Role: "proxy", Running: false}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	crt, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := secret(t.Context(), rc, "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		t.Fatal(err)
	}
	old := map[string][]byte{}
	b.appliedVersions = map[string]int64{}
	for _, m := range s.data() {
		base := "tls/servers/" + m.Name
		active, err := secret(t.Context(), rc, base+".crt")
		if err != nil {
			t.Fatal(err)
		}
		old[m.Name] = invalidTransportLeaf(t, ca, active, false)
		if err := putSecret(t.Context(), rc, base+".crt", old[m.Name]); err != nil {
			t.Fatal(err)
		}
		b.appliedVersions[m.Name] = m.ConfigVersion
	}
	// Model a proxy whose verified monitor cannot recover until all native
	// identities have actually been published and acknowledged by their agents.
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current, _ := load(rc.Cluster)
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: current.find("demopx").ConfigVersion})
		case "/v1/postgres/status":
			running := true
			for _, m := range current.data() {
				active, err := secret(r.Context(), rc, "tls/servers/"+m.Name+".crt")
				if err != nil {
					running = false
					continue
				}
				unusable, err := pki.NeedsRenewal(active, 0, now(rc))
				running = running && err == nil && !unusable && !m.TLSReloadPending
			}
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "proxy", Running: running,
				ProxyBackends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}})
		case "/v1/reload":
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(proxy.Close)
	proxyDialer := fakeAgents{url: proxy.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.Certificate().Raw})}
	client, err := proxyDialer.Dial(t.Context(), rc.Cluster.Ref(), "demopx")
	if err != nil {
		t.Fatal(err)
	}
	rc.Agents = relayProxyAgents{AgentDialer: rc.Agents, proxy: client}
	b.actions, b.reloads = nil, nil
	b.reloadFailures = 1 // Publication survived; the pull request response did not.
	for range 30 {
		_, _ = p.Sync(t.Context(), rc)
		all := true
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range current.data() {
			active, err := secret(t.Context(), rc, "tls/servers/"+m.Name+".crt")
			if err != nil {
				t.Fatal(err)
			}
			all = all && !bytes.Equal(active, old[m.Name]) && !m.TLSReloadPending && !m.TLSPending && b.appliedVersions[m.Name] >= m.ConfigVersion
		}
		if all {
			for _, m := range current.data() {
				if !strings.Contains(strings.Join(b.reloads, ","), m.Name) {
					t.Fatalf("native identity activated without agent pull: %v", b.reloads)
				}
			}
			if len(b.actions) != 0 {
				t.Fatalf("certificate recovery disrupted a process: %v", b.actions)
			}
			finished := converge(t, p, rc)
			for _, m := range finished.data() {
				options, err := p.parseOptions(m.Options)
				if err != nil || options.Parameters["log_min_duration_statement"] != "250" {
					t.Fatalf("identity-only reload falsely acknowledged desired options: %s %v", m.Options, err)
				}
			}
			return
		}
	}
	t.Fatalf("writerless proxy fenced native certificate publication: reloads=%v", b.reloads)
}

func TestNativeCertificateRecoveryPreservesOperationOwners(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[0]
	m.TLSReloadPending = true
	for _, hold := range []func(){func() { s.Upgrade = &upgradeState{} }, func() { s.Upgrade = nil; s.Rolling = &rollingState{} }, func() { s.Rolling = nil; m.Offline = true }} {
		hold()
		if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); handled || err != nil {
			t.Fatalf("operation/stop fence bypassed: %t %v", handled, err)
		}
	}
}

func TestNativeCertificateRecoveryPreservesRemoteMaintenance(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	target := s.data()[0]
	target.TLSReloadPending = true
	target.TLSPending = true
	reloads := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{Maintenance: name == "demop1"})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary"})
		case "/v1/reload":
			reloads++
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		}
	})
	if _, err := p.recoverNativeCertificates(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if reloads != 0 || !target.TLSPending || !target.TLSReloadPending {
		t.Fatal("a different member's operator maintenance was bypassed")
	}
}
