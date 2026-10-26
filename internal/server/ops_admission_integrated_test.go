// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/drills"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestOpsIntegratedDrillDataMcpAdmissionMatchesHTTPCreate(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *domain.DataMcpSpec
	}{
		{"unsupported", &domain.DataMcpSpec{Enabled: true}},
		{"invalid-access", &domain.DataMcpSpec{Enabled: true, Access: "invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			// Use an allowlisted drill engine with the existing minimal provider
			// fixture, which deliberately does not implement DataMcpProvider.
			if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: "psmdb"}); err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			source, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			source.Spec.Provider = "psmdb"
			if err := s.Store.UpdateClusterSpec(ctx, source.Ref(), source.Spec); err != nil {
				t.Fatal(err)
			}
			backup := domain.Backup{Name: "saved", Cluster: source.Name, Provider: "psmdb", Type: "logical", Phase: domain.PhaseSucceeded}
			if err := s.Store.CreateBackup(ctx, &backup); err != nil {
				t.Fatal(err)
			}
			source.Spec.DataMcp = tc.spec
			body, err := json.Marshal(map[string]any{"name": "http-copy", "spec": specAPI(source.Spec)})
			if err != nil {
				t.Fatal(err)
			}
			w := call(s, "POST", "/api/v1/clusters", string(body), token)
			if w.Code != 400 {
				t.Fatalf("HTTP create admitted invalid data MCP: %d %s", w.Code, w.Body.String())
			}
			d := domain.BackupDrill{ID: "data-mcp-" + tc.name, Namespace: source.Namespace, Cluster: source.Name, Backup: backup.Name, Phase: "Provisioning", CreatedAt: time.Now().UTC()}
			if err := s.Store.CreateBackupDrill(ctx, d); err != nil {
				t.Fatal(err)
			}
			if err := s.createDrillTarget(ctx, *source, d); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", w.Code)) {
				t.Fatalf("drill create did not preserve HTTP admission rejection: %v", err)
			}
			for _, name := range []string{"http-copy", drills.TargetName(d.ID)} {
				ref := domain.ClusterRef{Namespace: source.Namespace, Name: name}
				if _, err := s.Store.GetCluster(ctx, ref); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("rejected create persisted cluster %s: %v", name, err)
				}
				operations, err := s.Store.ListOperations(ctx, ref)
				if err != nil || len(operations) != 0 {
					t.Fatalf("rejected create enqueued %s operations: %+v %v", name, operations, err)
				}
			}
		})
	}
}

func opsIntegratedBoundPostgresSource(t *testing.T) (*Server, string, *domain.Cluster) {
	t.Helper()
	s, _, token := testServer(t)
	if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: "postgres"}); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	source, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	source.Spec.Provider = "postgres"
	source.Spec.Version = "17.11.1"
	source.Spec.Topology = domain.Topology{Type: "single"}
	source.Spec.Components.Engine.Replicas = 1
	source.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Cron: "0 2 * * *", Type: "full", RetainCount: 3}}
	source.Spec.Backup.Drill = &domain.BackupDrillPolicy{Enabled: true, Schedule: "0 3 * * *", Timeout: "6h"}
	if err := s.Store.UpdateClusterSpec(ctx, source.Ref(), source.Spec); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: source.Name, Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	binding := domain.PostgresBinding{ID: "identity-binding", SourceCluster: source.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, MinMajor: 16, MaxMajor: 18, State: "ready", CredentialGeneration: 7}
	if _, err := s.Store.EnsurePostgresBinding(ctx, &binding); err != nil {
		t.Fatal(err)
	}
	source, err = s.Store.GetCluster(ctx, source.Ref())
	if err != nil {
		t.Fatal(err)
	}
	return s, token, source
}

func TestOpsIntegratedDrillPreservesBoundPostgresSource(t *testing.T) {
	s, _, source := opsIntegratedBoundPostgresSource(t)
	ctx := t.Context()
	before, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	bindingsBefore, err := s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(bindingsBefore) != 1 {
		t.Fatalf("source binding fixture: %+v %v", bindingsBefore, err)
	}
	d := domain.BackupDrill{ID: "bound-source-copy", Namespace: source.Namespace, Cluster: source.Name, Backup: "saved", Phase: "Provisioning", CreatedAt: time.Now().UTC()}
	if err := s.Store.CreateBackupDrill(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := s.createDrillTarget(ctx, *source, d); err != nil {
		t.Fatal(err)
	}
	targetRef := domain.ClusterRef{Namespace: source.Namespace, Name: drills.TargetName(d.ID)}
	target, err := s.Store.GetCluster(ctx, targetRef)
	if err != nil {
		t.Fatal(err)
	}
	if target.ManagedBy == nil || target.ManagedBy.Kind != "backup-drill" || target.ManagedBy.ID != d.ID || target.Spec.DataSource == nil || target.Spec.DataSource.Cluster != source.Name || target.Spec.DataSource.Backup != "saved" {
		t.Fatalf("throwaway target lost source/ownership: %+v", target)
	}
	if target.Spec.Backup.Drill != nil || len(target.Spec.Backup.Schedules) != 0 {
		t.Fatalf("throwaway target inherited source backup policies: %+v", target.Spec.Backup)
	}
	targetBindings, err := s.Store.ListPostgresBindings(ctx, targetRef)
	if err != nil || len(targetBindings) != 0 {
		t.Fatalf("throwaway target inherited bindings: %+v %v", targetBindings, err)
	}
	after, err := s.Store.GetCluster(ctx, source.Ref())
	if err != nil {
		t.Fatal(err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	inputJSON, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, afterJSON) || !bytes.Equal(before, inputJSON) {
		t.Fatal("drill admission mutated the persisted or in-memory source cluster")
	}
	bindingsAfter, err := s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || !reflect.DeepEqual(bindingsBefore, bindingsAfter) {
		t.Fatalf("drill admission changed source binding rows: before=%+v after=%+v error=%v", bindingsBefore, bindingsAfter, err)
	}
	if bindingsAfter[0].CredentialGeneration != 7 || bindingsAfter[0].State != "ready" {
		t.Fatalf("drill admission rotated or reconciled source credentials: %+v", bindingsAfter[0])
	}
}

func TestOpsIntegratedBoundRestoreAcknowledgementHTTPAndDryRun(t *testing.T) {
	s, token, source := opsIntegratedBoundPostgresSource(t)
	ctx := t.Context()
	for _, path := range []string{"/api/v1/clusters/prod/restores", "/api/v1/dry-run/clusters/prod/restores"} {
		w := call(s, "POST", path, `{"name":"unacknowledged","source":{"backup":"saved"}}`, token)
		// Preview deliberately projects store conflicts through the generic
		// failure response; both routes must refuse before persisting work.
		if w.Code != 409 || (!strings.Contains(path, "/dry-run/") && !strings.Contains(strings.ToLower(w.Body.String()), "bound")) {
			t.Fatalf("%s bypassed bound-consumer acknowledgement: %d %s", path, w.Code, w.Body.String())
		}
	}
	operations, err := s.Store.ListOperations(ctx, source.Ref())
	if err != nil || len(operations) != 0 {
		t.Fatalf("unacknowledged restore enqueued work: %+v %v", operations, err)
	}
	if _, err := s.Store.GetRestore(ctx, source.Ref(), "unacknowledged"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unacknowledged restore persisted a record: %v", err)
	}
	body := `{"name":"acknowledged","acknowledgeBoundConsumers":true,"source":{"backup":"saved"}}`
	if w := call(s, "POST", "/api/v1/dry-run/clusters/prod/restores", body, token); w.Code != 200 {
		t.Fatalf("acknowledged preview rejected: %d %s", w.Code, w.Body.String())
	}
	if _, err := s.Store.GetRestore(ctx, source.Ref(), "acknowledged"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("acknowledged preview persisted a restore: %v", err)
	}
	if w := call(s, "POST", "/api/v1/clusters/prod/restores", body, token); w.Code != 202 {
		t.Fatalf("acknowledged restore rejected: %d %s", w.Code, w.Body.String())
	}
}
