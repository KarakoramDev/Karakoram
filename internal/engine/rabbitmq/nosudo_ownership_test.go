// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func pendingMemberImport(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *internalState, *member, string) {
	t.Helper()
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s := state(t, rc)
	if s.Pending == nil || s.Pending.Action != "import" || len(s.Members) != 1 {
		t.Fatalf("missing pending member import: %+v", s)
	}
	m := s.Members[0]
	tok, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil || tok == nil || tok.Token == "" {
		t.Fatalf("missing member token: %v", err)
	}
	return p, rc, f, s, m, tok.Token
}

func TestMaskedOwnershipRequiresMarker(t *testing.T) {
	for _, marker := range []bool{false, true} {
		name := "legacy-masked"
		if marker {
			name = "non-sensitive-marker"
		}
		t.Run(name, func(t *testing.T) {
			_, rc, f, _, m, token := pendingMemberImport(t)
			m.ServiceID, m.VolumeID = "", ""
			for _, name := range []string{m.Name, m.Volume} {
				service := f.services[name]
				service.Masked = true
				service.Ownership = nil // Sensitive legacy ownership values are absent when masked.
				if marker {
					service.Ownership = engine.OwnerVars(token, rc.Cluster.Name, m.Name, name)
				}
			}
			err := verifyMemberServices(t.Context(), rc, m)
			if !marker {
				if !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "sudo mode") {
					t.Fatalf("expected actionable masked ownership conflict: %v", err)
				}
				if m.ServiceID != "" || m.VolumeID != "" || len(f.varWrites) != 0 {
					t.Fatal("unverified services were adopted or modified")
				}
				return
			}
			if err != nil || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatalf("markers did not prove both services: member=%+v err=%v", m, err)
			}
		})
	}
}

func TestLegacyOwnershipBackfillsMarkers(t *testing.T) {
	_, rc, f, _, m, token := pendingMemberImport(t)
	m.ServiceID, m.VolumeID = "", ""
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Ownership = ownershipEnv(rc.Cluster.Name, m.Name, token)
	}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		service, err := f.GetService(t.Context(), name)
		want := engine.OwnerMarker(token, rc.Cluster.Name, m.Name, name)
		if err != nil || service.Ownership[engine.OwnerVariable] != want || f.varWrites[name][engine.OwnerVariable] != want {
			t.Fatalf("legacy service %s did not receive its marker through SetEnvVars: %v", name, err)
		}
	}
	if m.ServiceID == "" || m.VolumeID == "" {
		t.Fatal("legacy service IDs were not recorded")
	}
}

func TestOwnershipRejectsWrongMarkers(t *testing.T) {
	for _, volume := range []bool{false, true} {
		for _, binding := range []string{"token", "cluster", "member", "hostname"} {
			name := "runtime/" + binding
			if volume {
				name = "volume/" + binding
			}
			t.Run(name, func(t *testing.T) {
				_, rc, f, _, m, token := pendingMemberImport(t)
				m.ServiceID, m.VolumeID = "", ""
				for _, name := range []string{m.Name, m.Volume} {
					f.services[name].Ownership = engine.OwnerVars(token, rc.Cluster.Name, m.Name, name)
				}
				host := m.Name
				if volume {
					host = m.Volume
				}
				cluster, member, hostname := rc.Cluster.Name, m.Name, host
				switch binding {
				case "token":
					token = "different-token"
				case "cluster":
					cluster = "different-cluster"
				case "member":
					member = "different-member"
				case "hostname":
					hostname = "different-hostname"
				}
				f.services[host].Ownership = engine.OwnerVars(token, cluster, member, hostname)
				if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("wrong marker was accepted: %v", err)
				}
				if len(f.varWrites) != 0 {
					t.Fatal("failed verification backfilled markers")
				}
			})
		}
	}
}

func TestRecordedIDsVerifyWithoutEnvironment(t *testing.T) {
	p, rc, f, s, m, _ := pendingMemberImport(t)
	if m.ServiceID != "" || m.VolumeID != "" {
		t.Fatal("pending import recorded service identities before completion")
	}
	if _, err := p.observePending(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Ownership = nil
	}
	if m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
		t.Fatal("import response IDs were not recorded")
	}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
}

func TestRecordedRuntimeCannotBackfillVolumeEvidence(t *testing.T) {
	_, rc, f, _, m, _ := pendingMemberImport(t)
	m.ServiceID = f.services[m.Name].ID
	m.VolumeID = ""
	f.services[m.Name].Ownership = map[string]string{"KARAKORAM_DATA_DIR": "/mnt/" + m.Volume}
	f.services[m.Volume].Ownership = nil
	if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("recorded runtime ID authorized an unverified volume: %v", err)
	}
	if m.VolumeID != "" || len(f.varWrites) != 0 || f.services[m.Name].Ownership[engine.OwnerVariable] != "" {
		t.Fatal("failed volume verification generated new mount evidence")
	}
}

func TestPendingImportAdoptsOperationServiceIDs(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "import-response"
		if legacy {
			name = "legacy-process-ids"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, f, s, m, _ := pendingMemberImport(t)
			m.ServiceID, m.VolumeID = "", ""
			for _, name := range []string{m.Name, m.Volume} {
				f.services[name].Ownership = nil
				f.services[name].Masked = true
			}
			st := infra.OpState{Done: true}
			if legacy {
				s.Pending.Op.Services = nil
				st.ServiceIDs = []string{f.services[m.Name].ID, f.services[m.Volume].ID}
			}
			f.states[s.Pending.Op.ID] = st
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s = state(t, rc)
			m = s.Members[0]
			if s.Pending != nil || m.Stage != "imported" || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID || len(f.varWrites) != 2 {
				t.Fatalf("pending import evidence was not adopted and backfilled: %+v", s)
			}
		})
	}
}

func TestExpiredImportReimportsMissingServices(t *testing.T) {
	for _, missing := range []string{"both", "runtime", "volume"} {
		t.Run(missing, func(t *testing.T) {
			p, rc, f, s, m, _ := pendingMemberImport(t)
			m.ServiceID, m.VolumeID = "", ""
			s.Pending.Op.Services = nil
			f.states[s.Pending.Op.ID] = infra.OpState{Done: true, Expired: true}
			if missing != "volume" {
				delete(f.services, m.Name)
			}
			if missing != "runtime" {
				delete(f.services, m.Volume)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s = state(t, rc)
			if s.Pending != nil || s.Members[0].Stage != "planned" {
				t.Fatalf("incomplete expired import advanced or remained pending: %+v", s)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s = state(t, rc)
			if s.Pending == nil || s.Pending.Action != "import" || len(f.imports) != 2 {
				t.Fatalf("missing services were not re-imported: %+v", s)
			}
			want := 1
			if missing == "both" {
				want = 2
			}
			if len(f.imports[1]) != want {
				t.Fatalf("imported %d services, want %d", len(f.imports[1]), want)
			}
			for _, spec := range f.imports[1] {
				if missing == "runtime" && spec.Hostname != m.Name || missing == "volume" && spec.Hostname != m.Volume {
					t.Fatalf("re-imported existing service %s", spec.Hostname)
				}
			}
		})
	}
}

func TestExpiredImportMaskedLegacyServicesConflict(t *testing.T) {
	p, rc, f, s, m, _ := pendingMemberImport(t)
	m.ServiceID, m.VolumeID = "", ""
	s.Pending.Op.Services = nil
	f.states[s.Pending.Op.ID] = infra.OpState{Done: true, Expired: true}
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Ownership = nil
		f.services[name].Masked = true
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "sudo mode") {
		t.Fatalf("expired masked import did not report recovery instructions: %v", err)
	}
	if len(f.imports) != 1 || len(f.varWrites) != 0 || state(t, rc).Members[0].Stage != "planned" {
		t.Fatal("expired masked services were modified or incorrectly advanced")
	}
}

func TestImportSpecsCarryServiceBoundMarkers(t *testing.T) {
	_, rc, f, s, m, token := pendingMemberImport(t)
	if len(f.imports) != 1 || len(f.imports[0]) != 2 {
		t.Fatal("expected runtime and volume import")
	}
	for _, spec := range f.imports[0] {
		want := engine.OwnerMarker(token, rc.Cluster.Name, m.Name, spec.Hostname)
		if spec.Vars[engine.OwnerVariable] != want || spec.Env[engine.OwnerVariable] != "" || spec.Env["KARAKORAM_AGENT_TOKEN"] != token || spec.Env["KARAKORAM_CLUSTER"] != rc.Cluster.Name || spec.Env["KARAKORAM_MEMBER"] != m.Name {
			t.Fatalf("missing public marker or changed sensitive ownership environment for %s", spec.Hostname)
		}
		if s.Pending.Op.Services[spec.Hostname] != f.services[spec.Hostname].ID || f.services[spec.Hostname].Ownership[engine.OwnerVariable] != want {
			t.Fatalf("fake import did not preserve marker and returned service ID for %s", spec.Hostname)
		}
	}
}

func TestExpiredPendingOperationsScheduleRetry(t *testing.T) {
	for _, action := range []string{"deploy", "upgrade", "restart", "resources", "disk", "management", "cleanup-management", "delete-runtime", "delete-volume"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, s, m, _ := pendingMemberImport(t)
			before := *m
			s.Pending.Action = action
			s.Pending.Op.Kind = action
			s.Pending.Version = rc.Cluster.Spec.Version
			s.Pending.Resources = rc.Cluster.Spec.Components.Engine.Resources
			f.states[s.Pending.Op.ID] = infra.OpState{Done: true, Failed: true, Expired: true, Status: "EXPIRED", Message: "platform process expired; retrying the action"}
			busy, err := p.observePending(t.Context(), rc, s)
			if !busy || err == nil || s.Pending != nil || s.Retry == nil || s.Retry.Action != action {
				t.Fatalf("expired %s did not schedule retry: busy=%v err=%v state=%+v", action, busy, err, s)
			}
			r := s.PlatformRetries[platformRetryKey(s.Retry)]
			if r == nil || r.Failures != 1 || !r.RetryAfter.After(now(rc)) {
				t.Fatalf("expired action did not record bounded backoff: %+v", r)
			}
			if s.find(m.Name) != m || !reflect.DeepEqual(*m, before) {
				t.Fatalf("expired action advanced member state: before=%+v after=%+v", before, m)
			}
		})
	}
}

// A stale recorded volume ID does not block Karakoram's own replacement: the
// re-imported volume carries its service-bound marker and is adopted.
func TestImportReplacesStaleVolumeIdentityThroughMarker(t *testing.T) {
	p, rc, f, s, m, _ := pendingMemberImport(t)
	m.ServiceID, m.VolumeID = "previous-runtime", "previous-volume"
	s.Pending = nil
	delete(f.services, m.Name)
	delete(f.services, m.Volume)
	if err := p.importMember(t.Context(), rc, s, m); err != nil {
		t.Fatal(err)
	}
	if m.ServiceID != "previous-runtime" || m.VolumeID != "previous-volume" {
		t.Fatalf("pending import replaced recorded identities before completion: %+v", m)
	}
	if _, err := p.observePending(t.Context(), rc, s); err != nil {
		t.Fatalf("own re-imported volume rejected: %v", err)
	}
	if m.Stage != "imported" || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
		t.Fatalf("completed re-import identities not recorded: %+v", m)
	}
	// Without its marker and without the runtime's exact mount binding, the
	// volume has no evidence left besides its hostname.
	f.services[m.Volume].Ownership = nil
	delete(f.services[m.Name].Ownership, "KARAKORAM_DATA_DIR")
	m.VolumeID = "previous-volume"
	if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("unmarked replacement volume accepted: %v", err)
	}
}

func TestRecordedIDsSkipMarkerBackfillDuringDeletionOrWithoutToken(t *testing.T) {
	for _, mode := range []string{"deleting", "missing-token"} {
		t.Run(mode, func(t *testing.T) {
			_, rc, f, _, m, _ := pendingMemberImport(t)
			m.ServiceID, m.VolumeID = f.services[m.Name].ID, f.services[m.Volume].ID
			for _, name := range []string{m.Name, m.Volume} {
				f.services[name].Ownership = nil
				f.services[name].Masked = true
			}
			if mode == "deleting" {
				rc.Cluster.Deleting = true
			} else if err := rc.Secrets.DeleteAgentToken(t.Context(), rc.Cluster.Ref(), m.Name); err != nil {
				t.Fatal(err)
			}
			if err := verifyMemberServices(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if len(f.varWrites) != 0 || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("recorded ownership changed or marker backfill wrote variables")
			}
		})
	}
}
