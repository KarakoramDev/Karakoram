// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestMaintenanceUrgentLeavesWaitForActiveRetirement(t *testing.T) {
	f := coreSetup(t, 4)
	coreConverge(t, f)
	removed := false
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/remove" {
			t.Error("unexpected retirement route", r.URL.Path)
		}
		s, err := load(f.rc.Cluster)
		if err != nil || s.ScaleDown == nil || s.find(s.ScaleDown.Member).CleanupStep != "stopped" {
			t.Error("native removal crossed the stop fence", err)
		}
		removed = true
	}))
	defer internal.Close()
	f.rc.Dialer = retirementDialer{f.rc.Dialer, strings.TrimPrefix(internal.URL, "http://")}
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	var s *state
	admitted := false
	for range 30 {
		if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.ScaleDown != nil && s.find(s.ScaleDown.Member).Removing {
			admitted = true
			break
		}
	}
	if !admitted {
		t.Fatal("scale-down did not acquire retirement ownership")
	}
	caPEM, err := secret(t.Context(), f.rc, secretCA)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := secret(t.Context(), f.rc, secretCAKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	leaves := map[string]string{}
	transportMember := ""
	for _, m := range s.Members {
		if m.Removing {
			continue
		}
		config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		leaf, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, Server: true, Validity: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		config.TLS.CertPEM, config.TLS.KeyPEM = string(leaf), string(key)
		if transportMember == "" {
			transportMember = m.Name
			config.AgentTLS = agentapi.Certificate{}
		}
		if err = activateConfig(t.Context(), f.rc, s, m, config); err != nil {
			t.Fatal(err)
		}
		leaves[m.Name] = string(leaf)
	}
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	retired := false
	for range 120 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.ScaleDown != nil && s.Rolling != nil {
			t.Fatal("native leaf renewal displaced active retirement")
		}
		for _, m := range s.Members {
			if m.Removing {
				continue
			}
			config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
			if err != nil || config.TLS.CertPEM != leaves[m.Name] {
				t.Fatal("native leaf changed before retirement completed", err)
			}
		}
		if len(s.Members) == 3 && s.ScaleDown == nil {
			retired = true
			break
		}
	}
	if !retired || !removed {
		t.Fatal("urgent retained-member leaves blocked admitted retirement")
	}
	transport, err := f.p.MemberConfig(t.Context(), f.rc, transportMember)
	if err != nil || transport.AgentTLS.CertPEM == "" {
		t.Fatal("retirement blocked independent transport renewal", err)
	}
	started, completed := false, false
	for range 150 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		started = started || s.Rolling != nil
		if started && s.Rolling == nil {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("urgent leaf renewal did not resume after retirement")
	}
	if allowed, _ := f.rc.DisruptionAllowed(); allowed {
		t.Fatal("proof unexpectedly opened the maintenance window")
	}
	for _, m := range s.Members {
		config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
		if err != nil || config.TLS.CertPEM == leaves[m.Name] {
			t.Fatal("retained urgent leaf was not renewed", err)
		}
	}
}
