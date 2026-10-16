// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const ownershipTestToken = "member-ownership-token"

func ownershipFixture(t *testing.T) (*Provider, *engine.Context, *coreInfra, *state, *member) {
	t.Helper()
	p, rc, f, _ := coreSetup(t, 1)
	m := &member{Name: "demon0", Volume: "demon0v", Kind: kindNode, Stage: stagePlanned}
	s := &state{UID: "ownership-test", Phase: "provisioning", Members: []*member{m}, NextIndex: map[string]int{kindNode: 1}}
	if err := rc.Secrets.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: ownershipTestToken}); err != nil {
		t.Fatal(err)
	}
	return p, rc, f, s, m
}

func TestMaskedMemberOwnershipEvidence(t *testing.T) {
	for _, evidence := range []string{"none", "marker", "wrong-marker", "recorded-id"} {
		t.Run(evidence, func(t *testing.T) {
			_, rc, f, _, m := ownershipFixture(t)
			for _, name := range []string{m.Name, m.Volume} {
				service := &infra.Service{ID: "id-" + name, Hostname: name, Masked: true, Ownership: map[string]string{}}
				switch evidence {
				case "marker":
					service.Ownership = engine.OwnerVars(ownershipTestToken, rc.Cluster.Name, m.Name, name)
				case "wrong-marker", "recorded-id":
					service.Ownership = engine.OwnerVars("foreign-token", rc.Cluster.Name, m.Name, name)
				}
				f.services[name] = service
			}
			if evidence == "recorded-id" {
				m.ServiceID, m.VolumeID = f.services[m.Name].ID, f.services[m.Volume].ID
			}
			err := verifyMemberServices(t.Context(), rc, m)
			if evidence == "none" || evidence == "wrong-marker" {
				if !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("unverified services were accepted: %v", err)
				}
				if evidence == "none" && !strings.Contains(err.Error(), "sudo mode") {
					t.Fatalf("masked ownership conflict has no recovery hint: %v", err)
				}
				if m.ServiceID != "" || m.VolumeID != "" {
					t.Fatal("unverified identities were recorded", m)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("verified identities were not recorded", m)
			}
			for _, name := range []string{m.Name, m.Volume} {
				if !engine.HasOwnership(f.services[name], ownershipTestToken, rc.Cluster.Name, m.Name) {
					t.Fatal("verified service marker was not backfilled", name)
				}
			}
		})
	}
}

func TestLegacyMemberOwnershipBackfillsMarkers(t *testing.T) {
	_, rc, f, _, m := ownershipFixture(t)
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name] = &infra.Service{ID: "id-" + name, Hostname: name, Ownership: engine.OwnershipEnv(rc.Cluster.Name, m.Name, ownershipTestToken)}
	}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{m.Name, m.Volume} {
		service := f.services[name]
		want := engine.OwnerMarker(ownershipTestToken, rc.Cluster.Name, m.Name, name)
		if service.Ownership[engine.OwnerVariable] != want {
			t.Fatal("legacy service marker was not backfilled", name)
		}
		service.Masked = true
		delete(service.Ownership, "KARAKORAM_AGENT_TOKEN")
	}
	m.ServiceID, m.VolumeID = "", ""
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal("backfilled markers did not survive sensitive-value masking", err)
	}
}

func TestMaskedRuntimeMarkerVerifiesExactVolumeMount(t *testing.T) {
	_, rc, f, _, m := ownershipFixture(t)
	env := engine.OwnerVars(ownershipTestToken, rc.Cluster.Name, m.Name, m.Name)
	env["KARAKORAM_DATA_DIR"] = f.Layout().VolumePath(m.Volume)
	f.services[m.Name] = &infra.Service{ID: "runtime", Hostname: m.Name, Masked: true, Ownership: env}
	f.services[m.Volume] = &infra.Service{ID: "volume", Hostname: m.Volume}
	if err := verifyMemberServices(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	if m.ServiceID != "runtime" || m.VolumeID != "volume" || !engine.HasOwnership(f.services[m.Volume], ownershipTestToken, rc.Cluster.Name, m.Name) {
		t.Fatal("runtime mount did not prove and backfill volume ownership", m)
	}
}

func TestComponentImportOwnershipMarkers(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, s, m := ownershipFixture(t)
			m.Kind, m.Volume = kind, ""
			if err := p.importMember(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			if len(f.imports) != 1 || len(f.imports[0]) != 1 {
				t.Fatal("component import did not create one runtime", f.imports)
			}
			spec := f.imports[0][0]
			want := engine.OwnerMarker(ownershipTestToken, rc.Cluster.Name, m.Name, m.Name)
			if spec.Vars[engine.OwnerVariable] != want || spec.Env["KARAKORAM_AGENT_TOKEN"] != ownershipTestToken || spec.Env[engine.OwnerVariable] != "" {
				t.Fatal("component import did not separate marker variables from sensitive secrets")
			}
			if m.ServiceID != "" || s.Pending.Op.Services[m.Name] != f.services[m.Name].ID {
				t.Fatal("component import response evidence not kept separate from member identity", m)
			}
			if _, err := p.observePending(t.Context(), rc, s); err != nil || m.Stage != stageImported || m.ServiceID != f.services[m.Name].ID {
				t.Fatal("completed component import identity not adopted", m, err)
			}
			service := f.services[m.Name]
			service.Masked, service.Ownership = true, map[string]string{engine.OwnerVariable: want}
			m.ServiceID = ""
			if err := verifyMemberServices(t.Context(), rc, m); err != nil {
				t.Fatal("masked component marker was not accepted", err)
			}
			if m.ServiceID != service.ID {
				t.Fatal("verified component identity was not recorded")
			}
		})
	}
}

func TestImportRecordsIDsAndPreservesVolumeIdentity(t *testing.T) {
	for _, recordedVolume := range []string{"", "previous-volume"} {
		t.Run("recorded-volume-"+recordedVolume, func(t *testing.T) {
			p, rc, f, s, m := ownershipFixture(t)
			m.ServiceID, m.VolumeID = "previous-runtime", recordedVolume
			if err := p.importMember(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			if m.ServiceID != "previous-runtime" || m.VolumeID != recordedVolume {
				t.Fatal("pending import replaced recorded identities before completion", m)
			}
			for _, spec := range f.imports[0] {
				if spec.Vars[engine.OwnerVariable] != engine.OwnerMarker(ownershipTestToken, rc.Cluster.Name, m.Name, spec.Hostname) {
					t.Fatal("member import is missing its service-bound marker", spec.Hostname)
				}
			}
			// A stale recorded volume ID does not block Karakoram's own
			// replacement: the new volume carries its service-bound marker.
			if _, err := p.observePending(t.Context(), rc, s); err != nil || m.Stage != stageImported || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("owned import did not complete", m, err)
			}
		})
	}
}

type ownershipOpInfra struct {
	*coreInfra
	status infra.OpState
}

func (f *ownershipOpInfra) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return f.status, nil
}

func TestLegacyPendingImportAdoptsProcessServiceIDs(t *testing.T) {
	p, rc, f, s, m := ownershipFixture(t)
	for _, name := range []string{m.Name, m.Volume} {
		f.services[name] = &infra.Service{ID: "id-" + name, Hostname: name, Masked: true}
	}
	rc.Backend = &ownershipOpInfra{coreInfra: f, status: infra.OpState{Done: true, ServiceIDs: []string{f.services[m.Name].ID, f.services[m.Volume].ID}}}
	s.Pending = &pending{Op: infra.Op{ID: "legacy-import"}, Action: "import", Member: m.Name}
	if busy, err := p.observePending(t.Context(), rc, s); !busy || err != nil {
		t.Fatal("legacy pending import was not adopted", busy, err)
	}
	if s.Pending != nil || m.Stage != stageImported || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
		t.Fatal("legacy import identities or completion were not recorded", m, s.Pending)
	}
	for _, name := range []string{m.Name, m.Volume} {
		if !engine.HasOwnership(f.services[name], ownershipTestToken, rc.Cluster.Name, m.Name) {
			t.Fatal("adopted legacy service marker was not backfilled", name)
		}
	}
}

func TestPendingImportRejectsUnrelatedProcessServiceIDs(t *testing.T) {
	p, rc, f, s, m := ownershipFixture(t)
	f.services[m.Name] = &infra.Service{ID: "foreign-service", Hostname: m.Name, Masked: true}
	rc.Backend = &ownershipOpInfra{coreInfra: f, status: infra.OpState{Done: true, ServiceIDs: []string{"other-service"}}}
	s.Pending = &pending{Op: infra.Op{ID: "legacy-import"}, Action: "import", Member: m.Name}
	if _, err := p.observePending(t.Context(), rc, s); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("unrelated import process authorized a service", err)
	}
	if m.ServiceID != "" || m.Stage != stagePlanned || s.Pending == nil {
		t.Fatal("unverified pending import was recorded as complete", m, s.Pending)
	}
}

func TestExpiredPendingImportReimportsMissingServices(t *testing.T) {
	for _, absent := range []string{"all", "runtime", "volume"} {
		t.Run(absent, func(t *testing.T) {
			p, rc, f, s, m := ownershipFixture(t)
			for _, name := range []string{m.Name, m.Volume} {
				if absent == "all" || absent == "runtime" && name == m.Name || absent == "volume" && name == m.Volume {
					continue
				}
				f.services[name] = &infra.Service{ID: "id-" + name, Hostname: name, Masked: true, Ownership: engine.OwnerVars(ownershipTestToken, rc.Cluster.Name, m.Name, name)}
			}
			rc.Backend = &ownershipOpInfra{coreInfra: f, status: infra.OpState{Done: true, Expired: true}}
			s.Pending = &pending{Op: infra.Op{ID: "expired-import"}, Action: "import", Member: m.Name}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.Pending != nil || s.Members[0].Stage != stagePlanned {
				t.Fatal("expired partial import advanced past missing services", s.Pending, s.Members[0])
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal("normal import step did not recover expired import", err)
			}
			if len(f.imports) != 1 {
				t.Fatal("missing services were not reimported", f.calls)
			}
			want := 1
			if absent == "all" {
				want = 2
			}
			if len(f.imports[0]) != want || f.services[m.Name] == nil || f.services[m.Volume] == nil {
				t.Fatal("reimport did not create exactly the missing services", f.imports)
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal("replacement import did not complete", err)
			}
			s, err = load(rc.Cluster)
			if err != nil || s.Pending != nil || s.Members[0].Stage != stageImported {
				t.Fatal("replacement import is still pending", s, err)
			}
		})
	}
}

func TestExpiredPendingActionsScheduleRetry(t *testing.T) {
	for _, action := range []string{"deploy", "upgrade", "resources", "disk", "containers", "stop-removal", "delete-runtime", "delete-volume", "subdomain-on", "subdomain-off", "stop", "start", "cleanup-runtime", "cleanup-volume"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, s, m := ownershipFixture(t)
			before := *m
			rc.Backend = &ownershipOpInfra{coreInfra: f, status: infra.OpState{Done: true, Failed: true, Expired: true, Status: "EXPIRED", Message: "platform process expired; retrying the action"}}
			s.Pending = &pending{Op: infra.Op{ID: "expired-process", Kind: action}, Action: action, Member: m.Name, Version: rc.Cluster.Spec.Version, Resources: rc.Cluster.Spec.Components.Engine.Resources, Containers: 2}
			busy, err := p.observePending(t.Context(), rc, s)
			if !busy || err == nil || s.Pending != nil || s.Retry == nil || s.Retry.Action != action {
				t.Fatal("expired action did not schedule retry", busy, err, s.Pending, s.Retry)
			}
			r := s.PlatformRetries[retryKey(s.Retry)]
			if r == nil || r.Failures != 1 || !r.RetryAfter.After(now(rc)) {
				t.Fatalf("expired action did not record bounded backoff: %+v", r)
			}
			if s.find(m.Name) != m || !reflect.DeepEqual(*m, before) {
				t.Fatalf("expired action advanced member state: before=%+v after=%+v", before, m)
			}
		})
	}
}

type markerWriteInfra struct {
	*coreInfra
	writes []string
}

func (f *markerWriteInfra) SetEnvVars(ctx context.Context, name string, vars map[string]string) error {
	f.writes = append(f.writes, name)
	return f.coreInfra.SetEnvVars(ctx, name, vars)
}

func TestRecordedIDsSkipMarkerBackfillDuringDeletionOrWithoutToken(t *testing.T) {
	for _, mode := range []string{"deleting", "missing-token"} {
		t.Run(mode, func(t *testing.T) {
			_, rc, f, _, m := ownershipFixture(t)
			backend := &markerWriteInfra{coreInfra: f}
			rc.Backend = backend
			for _, name := range []string{m.Name, m.Volume} {
				f.services[name] = &infra.Service{ID: "id-" + name, Hostname: name, Masked: true}
			}
			m.ServiceID, m.VolumeID = f.services[m.Name].ID, f.services[m.Volume].ID
			if mode == "deleting" {
				rc.Cluster.Deleting = true
			} else if err := rc.Secrets.DeleteAgentToken(t.Context(), rc.Cluster.Ref(), m.Name); err != nil {
				t.Fatal(err)
			}
			if err := verifyMemberServices(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if len(backend.writes) != 0 || m.ServiceID != f.services[m.Name].ID || m.VolumeID != f.services[m.Volume].ID {
				t.Fatal("recorded ownership changed or marker backfill wrote variables")
			}
		})
	}
}
