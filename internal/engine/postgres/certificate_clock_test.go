// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestConversionCertificateClockCorrection(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	corrected := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	clock := &backupCleanupClock{at: corrected.Add(8 * time.Hour)}
	rc.Clock = clock
	rc.Cluster.Spec.Topology.Type = "ha"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	rc.Cluster.Generation++
	var s *state
	deployed := false
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(rc.Cluster)
		deployed = len(s.data()) == 3 && !s.Formed
		for _, m := range s.data() {
			deployed = deployed && (m.Stage == "deployed" || m.Stage == "joined")
		}
		if deployed {
			break
		}
	}
	if !deployed {
		t.Fatal("conversion never reached unformed deployed membership")
	}
	old := map[string][]byte{}
	for _, name := range []string{"demop1", "demop2"} {
		crt, err := secret(t.Context(), rc, "tls/servers/"+name+".crt")
		if err != nil {
			t.Fatal(err)
		}
		block, _ := pem.Decode(crt)
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !leaf.NotBefore.Equal(clock.at.Add(-pki.LeafClockSkew)) {
			t.Fatal("skewed mint did not backdate by the shared allowance", err)
		}
		old[name] = crt
	}
	clock.at = corrected
	condition, err := memberCertificateCondition(t.Context(), rc, s)
	if err != nil || condition == nil || condition.Reason != "CertificateNotYetValid" {
		t.Fatalf("missing truthful certificate condition: %+v %v", condition, err)
	}
	// Generic transport is available, but native observations fail because the
	// database/Patroni/etcd identity is not yet valid. Neither is a repair gate.
	b.statusFailures = map[string]int{"demop0": 1000, "demop1": 1000, "demop2": 1000, "demopx": 1000}
	b.genericOverrides = map[string]agentapi.Status{}
	b.reloadFailures = 1
	for range 30 {
		_, _ = p.Sync(t.Context(), rc)
		s, _ = load(rc.Cluster)
		recovered := true
		for name, before := range old {
			crt, err := secret(t.Context(), rc, "tls/servers/"+name+".crt")
			if err != nil {
				t.Fatal(err)
			}
			invalid, err := pki.NeedsRenewal(crt, 0, corrected)
			recovered = recovered && err == nil && !invalid && !bytes.Equal(crt, before) && !s.find(name).TLSReloadPending
		}
		if recovered {
			break
		}
	}
	for name, before := range old {
		crt, _ := secret(t.Context(), rc, "tls/servers/"+name+".crt")
		if invalid, err := pki.NeedsRenewal(crt, 0, corrected); err != nil || invalid || bytes.Equal(crt, before) || s.find(name).TLSReloadPending {
			t.Fatalf("member %s failed durable identity recovery: invalid=%v err=%v", name, invalid, err)
		}
		old[name] = crt
	}
	// Valid persisted replacements must not generate another serial/revision.
	version := s.ConfigVersion
	if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
		t.Fatal(err)
	}
	if s.ConfigVersion != version {
		t.Fatal("valid certificates caused repeated publication")
	}
	for name, before := range old {
		crt, _ := secret(t.Context(), rc, "tls/servers/"+name+".crt")
		if !bytes.Equal(crt, before) {
			t.Fatal("valid certificate was reissued")
		}
	}
	b.statusFailures = nil
	finished := converge(t, p, rc)
	if !finished.Formed || finished.Topology != "ha" || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatal("original conversion did not complete after clock correction")
	}
}

func TestSingleCertificateRecoveryRequiresSafeReload(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	m := s.data()[0]
	caCert, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := secret(t.Context(), rc, "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	base := "tls/servers/" + m.Name
	active, err := secret(t.Context(), rc, base+".crt")
	if err != nil {
		t.Fatal(err)
	}
	bad := invalidTransportLeaf(t, ca, active, true)
	if err := putSecret(t.Context(), rc, base+".crt", bad); err != nil {
		t.Fatal(err)
	}
	if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
		t.Fatal(err)
	}
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: {Running: true, PendingRestart: true}}
	b.reloads = nil
	if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); err != nil || !handled {
		t.Fatal("old agent safety hold did not handle invalid identity", err)
	}
	stored, _ := secret(t.Context(), rc, base+".crt")
	if !bytes.Equal(stored, bad) || len(b.reloads) != 0 || m.TLSReloadPending {
		t.Fatal("old single-member agent was allowed to activate a restart-capable pull")
	}
	condition, err := memberCertificateCondition(t.Context(), rc, s)
	if err != nil || condition == nil || condition.Reason != "CertificateRecoveryBlocked" {
		t.Fatal("missing truthful old-agent recovery hold condition", err)
	}
	// A new agent can safely publish the replacement without any native probe.
	b.statusFailures = map[string]int{m.Name: 100}
	b.genericOverrides = map[string]agentapi.Status{m.Name: {ConfigVersion: m.ConfigVersion, CertificateReloadOnly: true}}
	if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); err != nil || !handled {
		t.Fatal("reload-only capable agent could not recover invalid identity", err)
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil || !config.CertificateReloadOnly || !m.TLSReloadOnly || !m.TLSReloadPending {
		t.Fatal("certificate-only request or durable receipt was not published", err)
	}
	if b.statusFailures[m.Name] != 100 {
		t.Fatal("new agent identity recovery depended on native health")
	}
	if m.TLSReloadBlocked != "" {
		t.Fatal("upgraded agent retained obsolete recovery hold")
	}
	b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.TLSReloadVersion, CertificateReloadOnly: true}
	if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); err != nil || !handled {
		t.Fatal("certificate-only revision was not acknowledged", err)
	}
	if m.TLSReloadPending || m.TLSReloadOnly {
		t.Fatal("acknowledged certificate-only receipt was not cleared")
	}
}

func TestCertificateRecoveryBlockClearsWhenValid(t *testing.T) {
	for _, activate := range []bool{false, true} {
		name := "clock-passes-validity-boundary"
		if activate {
			name = "ordinary-activation"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, b := setup(t, "single")
			s := converge(t, p, rc)
			m := s.data()[0]
			clock := &backupCleanupClock{at: time.Now().UTC().Add(2 * time.Hour)}
			rc.Clock = clock
			caCert, err := secret(t.Context(), rc, "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			caKey, err := secret(t.Context(), rc, "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caCert, caKey)
			if err != nil {
				t.Fatal(err)
			}
			base := "tls/servers/" + m.Name
			active, err := secret(t.Context(), rc, base+".crt")
			if err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, base+".crt", invalidTransportLeaf(t, ca, active, true)); err != nil {
				t.Fatal(err)
			}
			if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
				t.Fatal(err)
			}
			b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: {Role: "standalone", Running: true, PendingRestart: true}}
			if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); err != nil || !handled || m.TLSReloadBlocked == "" {
				t.Fatal("legacy member did not establish its certificate recovery hold", err)
			}
			blocked := m.TLSReloadBlocked
			clock.at = clock.at.Add(25 * time.Hour)
			if activate {
				if err := activateCertificates(t.Context(), rc, m); err != nil {
					t.Fatal(err)
				}
				if m.TLSReloadBlocked != "" {
					t.Fatal("ordinary activation retained obsolete certificate recovery hold")
				}
				// An older checkpoint can contain the hold without staged work.
				m.TLSReloadBlocked = blocked
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.find(m.Name).TLSReloadBlocked != "" {
				t.Fatal("valid active certificate retained obsolete recovery hold")
			}
			condition, err := memberCertificateCondition(t.Context(), rc, s)
			if err != nil || (condition != nil && condition.Reason == "CertificateRecoveryBlocked") {
				t.Fatalf("ordinary reconciliation retained blocked condition without an agent upgrade: %+v %v", condition, err)
			}
		})
	}
}

func TestProxyCertificateRecoveryIndependentOfWriter(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	caCert, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := secret(t.Context(), rc, "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	base := "tls/servers/demopx"
	active, err := secret(t.Context(), rc, base+".crt")
	if err != nil {
		t.Fatal(err)
	}
	bad := invalidTransportLeaf(t, ca, active, true)
	if err := putSecret(t.Context(), rc, base+".crt", bad); err != nil {
		t.Fatal(err)
	}
	b.statusOverrides = map[string]pgspec.StatusResponse{"demopx": {Role: "proxy", Running: false}}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if condition := healthCondition(status, "MemberTLSReady"); condition.Status != "False" || condition.Reason != "CertificateNotYetValid" {
		t.Fatalf("proxy TLS failure was hidden by writer unavailability: %+v", condition)
	}
	for range 8 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ := load(rc.Cluster)
		current, err := secret(t.Context(), rc, base+".crt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(current, bad) && !s.find("demopx").TLSReloadPending {
			if invalid, err := pki.NeedsRenewal(current, 0, now(rc)); err != nil || invalid {
				t.Fatal("replacement proxy identity is invalid", err)
			}
			config, err := p.MemberConfig(t.Context(), rc, "demopx")
			if err != nil || config.CertificateReloadOnly {
				t.Fatal("proxy incorrectly used the data-member reload-only path", err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if condition := healthCondition(status, "MemberTLSReady"); condition.Status == "False" {
				t.Fatalf("valid proxy identity retained a TLS condition: %+v", condition)
			}
			return
		}
	}
	t.Fatal("writer unavailability blocked proxy certificate recovery")
}
