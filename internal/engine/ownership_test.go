// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestOwnerMarkerBindsEveryIdentityField(t *testing.T) {
	token := "private-agent-token"
	marker := OwnerMarker(token, "cluster", "member", "hostname")
	if marker != OwnerMarker(token, "cluster", "member", "hostname") || !strings.HasPrefix(marker, "v1.") || len(marker) != 67 || strings.Contains(marker, token) {
		t.Fatal("invalid deterministic marker")
	}
	for _, args := range [][4]string{{"other-token", "cluster", "member", "hostname"}, {token, "other-cluster", "member", "hostname"}, {token, "cluster", "other-member", "hostname"}, {token, "cluster", "member", "other-hostname"}} {
		if got := OwnerMarker(args[0], args[1], args[2], args[3]); got == marker {
			t.Fatal("marker does not bind every identity field")
		}
	}
	if got := OwnerVars(token, "cluster", "member", "hostname"); !reflect.DeepEqual(got, map[string]string{OwnerVariable: marker}) {
		t.Fatalf("owner variables: %v", got)
	}
}

func TestHasOwnershipMarkerLegacyAndMaskedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		service *infra.Service
		token   string
		want    bool
	}{
		{"marker", &infra.Service{Hostname: "hostname", Masked: true, Ownership: OwnerVars("token", "cluster", "member", "hostname")}, "token", true},
		{"legacy", &infra.Service{Hostname: "hostname", Ownership: map[string]string{"KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "member", "KARAKORAM_AGENT_TOKEN": "token"}}, "token", true},
		{"masked", &infra.Service{Hostname: "hostname", Masked: true}, "token", false},
		{"wrong-marker", &infra.Service{Hostname: "hostname", Ownership: OwnerVars("token", "cluster", "member", "other-hostname")}, "token", false},
		{"wrong-token", &infra.Service{Hostname: "hostname", Ownership: map[string]string{"KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "member", "KARAKORAM_AGENT_TOKEN": "other"}}, "token", false},
		{"wrong-cluster", &infra.Service{Hostname: "hostname", Ownership: map[string]string{"KARAKORAM_CLUSTER": "other", "KARAKORAM_MEMBER": "member", "KARAKORAM_AGENT_TOKEN": "token"}}, "token", false},
		{"wrong-member", &infra.Service{Hostname: "hostname", Ownership: map[string]string{"KARAKORAM_CLUSTER": "cluster", "KARAKORAM_MEMBER": "other", "KARAKORAM_AGENT_TOKEN": "token"}}, "token", false},
		{"nil", nil, "token", false},
		{"empty-token", &infra.Service{Hostname: "hostname", Ownership: OwnerVars("", "cluster", "member", "hostname")}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasOwnership(tc.service, tc.token, "cluster", "member"); got != tc.want {
				t.Fatalf("ownership=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestUnverifiedOwnershipHintsOnlyForMaskedLegacyEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		masked, marker, hint bool
	}{{"unmasked", false, false, false}, {"masked-legacy", true, false, true}, {"masked-with-marker", true, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &infra.Service{Hostname: "hostname", Masked: tc.masked, Ownership: map[string]string{}}
			if tc.marker {
				s.Ownership[OwnerVariable] = "wrong-marker"
			}
			err := UnverifiedOwnership(s)
			if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "service hostname has no verified cluster ownership") || strings.Contains(err.Error(), "sudo") != tc.hint {
				t.Fatalf("conflict: %v", err)
			}
			if tc.hint && (!strings.Contains(err.Error(), "delete the service") || !strings.Contains(err.Error(), "one reconcile")) {
				t.Fatalf("non-actionable conflict: %v", err)
			}
		})
	}
}

type markerBackend struct {
	infra.Backend
	writes   int
	hostname string
	vars     map[string]string
	err      error
}

func (b *markerBackend) SetEnvVars(_ context.Context, hostname string, vars map[string]string) error {
	b.writes++
	b.hostname = hostname
	b.vars = vars
	return b.err
}

func TestEnsureOwnerMarkerWritesOnlyWhenNeeded(t *testing.T) {
	for _, status := range []string{"", "ACTIVE", "READY_TO_DEPLOY", "STOPPED", "DELETING", "CREATING", "UPDATING"} {
		t.Run(status, func(t *testing.T) {
			b := &markerBackend{}
			s := &infra.Service{Hostname: "hostname", Status: status}
			if err := EnsureOwnerMarker(t.Context(), b, s, "token", "cluster", "member"); err != nil {
				t.Fatal(err)
			}
			want := 1
			if status == "DELETING" || status == "CREATING" || status == "UPDATING" {
				want = 0
			}
			if b.writes != want {
				t.Fatalf("writes=%d want=%d", b.writes, want)
			}
			if want == 0 {
				return
			}
			if b.hostname != "hostname" || !reflect.DeepEqual(b.vars, OwnerVars("token", "cluster", "member", "hostname")) || s.Ownership[OwnerVariable] != b.vars[OwnerVariable] {
				t.Fatal("marker not recorded")
			}
			if err := EnsureOwnerMarker(t.Context(), b, s, "token", "cluster", "member"); err != nil || b.writes != 1 {
				t.Fatal("identical marker rewritten", err)
			}
			s.Ownership[OwnerVariable] = "wrong-marker"
			if err := EnsureOwnerMarker(t.Context(), b, s, "token", "cluster", "member"); err != nil || b.writes != 2 {
				t.Fatal("wrong marker not replaced", err)
			}
		})
	}
	b := &markerBackend{err: errors.New("write failed")}
	s := &infra.Service{Hostname: "hostname"}
	if err := EnsureOwnerMarker(t.Context(), b, s, "token", "cluster", "member"); !errors.Is(err, b.err) || s.Ownership[OwnerVariable] != "" {
		t.Fatal("failed marker write recorded", err)
	}
	before := b.writes
	if err := EnsureOwnerMarker(t.Context(), b, nil, "token", "cluster", "member"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureOwnerMarker(t.Context(), b, s, "", "cluster", "member"); err != nil || b.writes != before {
		t.Fatal("missing identity caused write", err)
	}
}

func TestImportedServiceIDUsesImportEvidenceNotHostname(t *testing.T) {
	service := &infra.Service{ID: "service-id", Hostname: "hostname"}
	for _, tc := range []struct {
		name    string
		op      infra.Op
		state   infra.OpState
		service *infra.Service
		want    string
	}{
		{"import-response", infra.Op{Services: map[string]string{"hostname": "service-id"}}, infra.OpState{}, service, "service-id"},
		{"response-mismatch-wins", infra.Op{Services: map[string]string{"hostname": "other-id"}}, infra.OpState{ServiceIDs: []string{"service-id"}}, service, ""},
		{"legacy-process", infra.Op{}, infra.OpState{ServiceIDs: []string{"other-id", "service-id"}}, service, "service-id"},
		{"other-response-hostname", infra.Op{Services: map[string]string{"other": "other-id"}}, infra.OpState{ServiceIDs: []string{"service-id"}}, service, "service-id"},
		{"hostname-only", infra.Op{}, infra.OpState{}, service, ""},
		{"wrong-process", infra.Op{}, infra.OpState{ServiceIDs: []string{"other-id"}}, service, ""},
		{"nil", infra.Op{}, infra.OpState{}, nil, ""},
		{"empty-id", infra.Op{}, infra.OpState{ServiceIDs: []string{""}}, &infra.Service{Hostname: "hostname"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ImportedServiceID(tc.op, tc.state, tc.service); got != tc.want {
				t.Fatalf("imported ID=%q want=%q", got, tc.want)
			}
		})
	}
}
