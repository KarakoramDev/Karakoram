// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestBackendPlacementOverridesAgentFingerprint(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authoritative bool
		group         string
		warning       string
	}{
		{"backend", true, "node-a", "are co-located"},
		{"backend unknown", true, "", "backend placement is unknown"},
		{"agent fallback", false, "node-a", "Best-effort"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			s := state(t, rc)
			for range 3 {
				s.add(rc.Cluster.Name)
			}
			// This fixture exposes running agent endpoints below. Mark the
			// deployment observed; planned members intentionally skip probes.
			for _, m := range ordered(s) {
				m.Stage = "deployed"
			}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			backend := fake.New(fake.Options{Capabilities: infra.Capabilities{Placement: tc.authoritative, VolumeQuota: true}})
			rc.Backend = backend
			for _, m := range ordered(s) {
				if _, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: m.Name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}}); err != nil {
					t.Fatal(err)
				}
				if _, err := backend.Start(t.Context(), m.Name); err != nil {
					t.Fatal(err)
				}
				if err := backend.SetPlacementGroup(m.Name, tc.group); err != nil {
					t.Fatal(err)
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range status.Members {
				want := tc.group
				if !tc.authoritative {
					want = "host-" + member.Name
				}
				if member.PlacementGroup != want {
					t.Fatalf("member %s placement = %q, want %q", member.Name, member.PlacementGroup, want)
				}
				group := want
				if group == "" {
					group = "unknown"
				}
				found := false
				for _, name := range status.Placement.Groups[group] {
					found = found || name == member.Name
				}
				if !found {
					t.Fatalf("member %s missing from placement group %s: %+v", member.Name, group, status.Placement)
				}
			}
			if !strings.Contains(status.Placement.Warning, tc.warning) {
				t.Fatalf("placement = %+v", status.Placement)
			}
			if tc.authoritative && strings.Contains(status.Placement.Warning, "fingerprint") {
				t.Fatalf("authoritative placement described as fingerprints: %+v", status.Placement)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "Placement" && tc.authoritative && condition.Reason == "BestEffortOnly" {
					t.Fatalf("backend placement is not best effort: %+v", condition)
				}
			}
			placement, err := p.Placement(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if tc.authoritative && placement.Warning != status.Placement.Warning {
				t.Fatalf("placement endpoint and status disagree: %+v / %+v", placement, status.Placement)
			}
		})
	}
}
