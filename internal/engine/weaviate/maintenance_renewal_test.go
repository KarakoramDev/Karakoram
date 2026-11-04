// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"testing"
	"time"
)

func TestMaintenanceUrgentLeavesWaitForPreviousVoterAcknowledgement(t *testing.T) {
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
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
	versions := map[string]int64{}
	leaves := map[string]string{}
	resources := s.Members[0].Resources
	for _, m := range s.Members {
		config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		leaf, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, Server: true, Validity: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		config.TLS.CertPEM, config.TLS.KeyPEM = string(leaf), string(key)
		if err = activateConfig(t.Context(), f.rc, s, m, config); err != nil {
			t.Fatal(err)
		}
		versions[m.Name], leaves[m.Name] = m.ConfigVersion, string(leaf)
	}
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"queryDefaultsLimit":99}`)
	first := ""
	for range 30 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		for _, m := range s.Members {
			if m.ConfigVersion > versions[m.Name] {
				first = m.Name
				break
			}
		}
		if first != "" {
			break
		}
	}
	if first == "" || s.Rolling == nil || s.Rolling.Reason != "renew-tls" || len(s.Rolling.Order) != 3 {
		t.Fatal("urgent leaves did not enter accepted-intent rolling renewal")
	}
	f.readinessStale = true
	for range 20 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.Rolling == nil || s.Rolling.Position != 0 {
			t.Fatal("renewal advanced before restarted voter acknowledged readiness")
		}
		for _, m := range s.Members {
			if m.Name != first && m.ConfigVersion != versions[m.Name] {
				t.Fatal("another voter restarted while first was unready")
			}
		}
	}
	f.readinessStale = false
	complete := false
	for range 150 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
		if s.Rolling == nil {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("serialized emergency renewal did not complete")
	}
	for _, m := range s.Members {
		config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec wvspec.MemberSpec
		if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if config.TLS.CertPEM == leaves[m.Name] || m.Resources != resources || spec.QueryDefaultsLimit == 99 {
			t.Fatal("renewal failed or applied deferred resource/application settings")
		}
	}
}
