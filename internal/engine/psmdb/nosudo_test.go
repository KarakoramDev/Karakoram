// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type importStateInfra struct {
	*fakeInfra
	state infra.OpState
}

func (f *importStateInfra) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return f.state, nil
}

func legacyPendingImport(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *internalState) {
	t.Helper()
	p, rc, f, _ := setup(t)
	s := syncToPending(t, p, rc, "import")
	m := s.Members[0]
	m.ServiceID, m.VolumeID = "", ""
	s.Pending.Op.Services = nil
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Ownership = nil
		f.services[name].Masked = true
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	return p, rc, f, s
}

func assertMemberMarkers(t *testing.T, rc *engine.Context, f *fakeInfra, m *member) {
	t.Helper()
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil || token == nil {
		t.Fatalf("agent token: %v", err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		if got := f.services[name].Ownership[engine.OwnerVariable]; got != engine.OwnerMarker(token.Token, rc.Cluster.Name, m.Name, name) {
			t.Errorf("missing bound marker on %s", name)
		}
	}
}

func TestMaskedLegacyMembersWithoutRecordedIDsHaveActionableConflict(t *testing.T) {
	p, rc, f, s := legacyPendingImport(t)
	s.Pending = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before := f.mutations
	_, err := p.Sync(t.Context(), rc)
	if !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "sudo") || !strings.Contains(err.Error(), "delete the service") {
		t.Fatalf("non-actionable ownership conflict: %v", err)
	}
	if f.mutations != before || len(f.envWrites) != 0 {
		t.Fatal("unverified services were mutated")
	}
}

func TestMemberMarkerVerifiesMaskedOwnership(t *testing.T) {
	_, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Ownership = engine.OwnerVars(token.Token, rc.Cluster.Name, m.Name, name)
	}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	if m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID || len(f.envWrites) != 0 {
		t.Fatal("marker verification failed or rewrote markers")
	}
}

func TestLegacyPlaintextOwnershipBackfillsMemberMarkers(t *testing.T) {
	_, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name].Masked = false
		f.services[name].Ownership = ownershipEnv(rc.Cluster.Name, m.Name, token.Token)
	}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	if m.ServiceID == "" || m.VolumeID == "" || len(f.envWrites) != 2 {
		t.Fatal("legacy ownership not recorded and backfilled")
	}
	assertMemberMarkers(t, rc, f, m)
}

func TestMaskedLegacyRecordedIDsBackfillMemberMarkers(t *testing.T) {
	_, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	m.ServiceID, m.VolumeID = f.services[m.Name].ID, f.services[m.Volume].ID
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	if len(f.envWrites) != 2 {
		t.Fatal("recorded ownership not backfilled")
	}
	assertMemberMarkers(t, rc, f, m)
}

func TestLegacyPendingImportAdoptsProcessServiceIDsAndDeploys(t *testing.T) {
	p, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	rc.Backend = &importStateInfra{fakeInfra: f, state: infra.OpState{Done: true, Status: "FINISHED", ServiceIDs: []string{f.services[m.Name].ID, f.services[m.Volume].ID}}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	m = s.Members[0]
	if m.Stage != "imported" || m.ServiceID == "" || m.VolumeID == "" || s.Pending != nil {
		t.Fatalf("legacy import not adopted: %+v", m)
	}
	assertMemberMarkers(t, rc, f, m)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if s.Pending == nil || s.Pending.Action != "deploy" || len(f.deployments) != 1 || len(f.imports) != 1 {
		t.Fatal("adopted member did not deploy without reimport")
	}
}

func TestExpiredPendingImportWithAbsentServicesReimports(t *testing.T) {
	p, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	delete(f.services, m.Name)
	delete(f.services, m.Volume)
	rc.Backend = &importStateInfra{fakeInfra: f, state: infra.OpState{Done: true, Expired: true, Status: "EXPIRED"}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if s.Pending != nil || s.Members[0].Stage != "planned" {
		t.Fatal("expired import did not clear pending checkpoint")
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if len(f.imports) != 2 || len(f.imports[1]) != 2 || s.Pending == nil || s.Pending.Action != "import" {
		t.Fatal("missing services were not reimported")
	}
	if s.Members[0].ServiceID != "" || s.Members[0].VolumeID != "" {
		t.Fatal("pending import recorded service identities before completion")
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if s.Pending != nil || s.Members[0].Stage != "imported" || s.Members[0].ServiceID != f.services[m.Name].ID || s.Members[0].VolumeID != f.services[m.Volume].ID {
		t.Fatal("completed import response IDs not recorded")
	}
}

func TestExpiredPendingImportWithMaskedLegacyServicesConflicts(t *testing.T) {
	p, rc, f, _ := legacyPendingImport(t)
	rc.Backend = &importStateInfra{fakeInfra: f, state: infra.OpState{Done: true, Expired: true, Status: "EXPIRED"}}
	before := f.mutations
	_, err := p.Sync(t.Context(), rc)
	if !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "sudo") || !strings.Contains(err.Error(), "delete the service") {
		t.Fatalf("non-actionable expired import conflict: %v", err)
	}
	if f.mutations != before || len(f.envWrites) != 0 || len(f.imports) != 1 {
		t.Fatal("expired process authorized masked services")
	}
}

func TestFreshImportCarriesRuntimeAndVolumeOwnerVariables(t *testing.T) {
	p, rc, f, _ := setup(t)
	s := syncToPending(t, p, rc, "import")
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.imports) != 1 || len(f.imports[0]) != 2 {
		t.Fatal("missing runtime or volume import")
	}
	for _, spec := range f.imports[0] {
		if got := spec.Vars[engine.OwnerVariable]; got != engine.OwnerMarker(token.Token, rc.Cluster.Name, m.Name, spec.Hostname) {
			t.Errorf("missing non-sensitive marker for %s", spec.Hostname)
		}
		if _, ok := spec.Env[engine.OwnerVariable]; ok {
			t.Error("ownership marker imported as secret")
		}
		if spec.Env["KARAKORAM_AGENT_TOKEN"] != token.Token {
			t.Error("agent credential missing from secrets")
		}
	}
	if m.ServiceID != "" || m.VolumeID != "" || s.Pending.Op.Services[m.Name] != f.services[m.Name].ID || s.Pending.Op.Services[m.Volume] != f.services[m.Volume].ID {
		t.Fatal("import response evidence not checkpointed separately from member identities")
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	m = s.Members[0]
	if s.Pending != nil || m.Stage != "imported" || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
		t.Fatal("completed import identities not adopted")
	}
}

func TestWrongMemberMarkerRejected(t *testing.T) {
	_, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	f.services[m.Name].Ownership = engine.OwnerVars(token.Token, rc.Cluster.Name, m.Name, m.Volume)
	if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("wrong-hostname marker accepted: %v", err)
	}
	if len(f.envWrites) != 0 || m.ServiceID != "" || m.VolumeID != "" {
		t.Fatal("wrong marker adopted or rewritten")
	}
}

func TestRecordedRuntimeIDCannotBackfillEvidenceForUntaggedVolume(t *testing.T) {
	for _, legacy := range []string{"masked", "wrong-token"} {
		t.Run(legacy, func(t *testing.T) {
			_, rc, f, s := legacyPendingImport(t)
			m := s.Members[0]
			runtime := f.services[m.Name]
			m.ServiceID = runtime.ID
			runtime.Ownership = map[string]string{"KARAKORAM_DATA_DIR": "/mnt/" + m.Volume}
			if legacy == "wrong-token" {
				runtime.Masked = false
				runtime.Ownership["KARAKORAM_CLUSTER"] = rc.Cluster.Name
				runtime.Ownership["KARAKORAM_MEMBER"] = m.Name
				runtime.Ownership["KARAKORAM_AGENT_TOKEN"] = "foreign-token"
			}
			if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("recorded runtime alone authorized volume: %v", err)
			}
			if len(f.envWrites) != 0 || runtime.Ownership[engine.OwnerVariable] != "" || m.VolumeID != "" {
				t.Fatal("recorded runtime produced marker evidence before volume verification")
			}
		})
	}
}

func TestRecordedVolumeMismatchRejectsMatchingOwnerMarker(t *testing.T) {
	_, rc, f, s := legacyPendingImport(t)
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	m.ServiceID = f.services[m.Name].ID
	m.VolumeID = "original-volume-id"
	f.services[m.Volume].Ownership = engine.OwnerVars(token.Token, rc.Cluster.Name, m.Name, m.Volume)
	if err := verifyMemberServices(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("marker overrode recorded volume ID: %v", err)
	}
	if m.VolumeID != "original-volume-id" || len(f.envWrites) != 0 {
		t.Fatal("recorded volume identity changed or services mutated")
	}
}

func TestRecordedIDsSkipMarkerBackfillDuringDeletionOrWithoutToken(t *testing.T) {
	for _, mode := range []string{"deleting", "missing-token"} {
		t.Run(mode, func(t *testing.T) {
			_, rc, f, s := legacyPendingImport(t)
			m := s.Members[0]
			m.ServiceID, m.VolumeID = f.services[m.Name].ID, f.services[m.Volume].ID
			if mode == "deleting" {
				rc.Cluster.Deleting = true
			} else if err := rc.Secrets.DeleteAgentToken(t.Context(), rc.Cluster.Ref(), m.Name); err != nil {
				t.Fatal(err)
			}
			if err := verifyMemberServices(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if len(f.envWrites) != 0 || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("recorded ownership changed or marker backfill wrote variables")
			}
		})
	}
}

func TestFailedImportDeletedServicesRetryAdoptsNewIDs(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	s := syncToPending(t, p, rc, "import")
	m := s.Members[0]
	oldRuntime, oldVolume := s.Pending.Op.Services[m.Name], s.Pending.Op.Services[m.Volume]
	if oldRuntime == "" || oldVolume == "" || m.ServiceID != "" || m.VolumeID != "" {
		t.Fatal("pending import identities were not isolated from member identities")
	}
	f.failed = true
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed import was not reported")
	}
	s = retryState(t, rc)
	if s.Pending != nil || s.Retry == nil || s.Members[0].ServiceID != "" || s.Members[0].VolumeID != "" {
		t.Fatal("failed import adopted half-created service identities")
	}
	delete(f.services, m.Name)
	delete(f.services, m.Volume)
	f.failed = false
	clock.at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatalf("retry with deleted half-created services failed: %v", err)
	}
	s = retryState(t, rc)
	if len(f.imports) != 2 || len(f.imports[1]) != 2 || s.Pending == nil || s.Pending.Action != "import" {
		t.Fatal("retry did not reimport both services")
	}
	// This fake normally reuses hostnames as IDs; model newly created identities.
	for _, name := range []string{m.Name, m.Volume} {
		id := "replacement-" + name
		f.services[name].ID = id
		s.Pending.Op.Services[name] = id
		f.services[name].Ownership = nil
		f.services[name].Masked = true
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatalf("replacement import conflicted with failed import identities: %v", err)
	}
	s = retryState(t, rc)
	m = s.Members[0]
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 || m.Stage != "imported" || m.ServiceID != "replacement-"+m.Name || m.VolumeID != "replacement-"+m.Volume || m.ServiceID == oldRuntime || m.VolumeID == oldVolume {
		t.Fatalf("replacement import did not adopt new identities: %+v", m)
	}
}

func TestExpiredPendingActionsScheduleRetry(t *testing.T) {
	for _, action := range []string{"deploy", "upgrade", "resources", "disk", "containers", "delete-runtime", "delete-volume", "cleanup-runtime", "cleanup-volume"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, s := legacyPendingImport(t)
			m := s.Members[0]
			before := *m
			s.Pending.Action = action
			s.Pending.Op.Kind = action
			s.Pending.Version = rc.Cluster.Spec.Version
			s.Pending.Resources = rc.Cluster.Spec.Components.Engine.Resources
			s.Pending.Containers = 2
			rc.Backend = &importStateInfra{fakeInfra: f, state: infra.OpState{Done: true, Failed: true, Expired: true, Status: "EXPIRED", Message: "platform process expired; retrying the action"}}
			busy, err := p.observePending(t.Context(), rc, s)
			if !busy || err == nil || s.Pending != nil || s.Retry == nil || s.Retry.Action != action {
				t.Fatalf("expired action did not schedule retry: busy=%v err=%v state=%+v", busy, err, s)
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
