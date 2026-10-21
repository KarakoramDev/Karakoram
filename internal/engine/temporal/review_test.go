// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

func publicUI(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	o.UI.PublicHTTPS = true
	o.UI.OIDC = &OIDCOptions{Issuer: "https://identity.example", ClientID: "temporal"}
	rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
	if err = p.SetUIOIDCClientSecret(t.Context(), rc, "oidc-secret"); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc)
}

func privateUI(t *testing.T, rc *engine.Context) {
	t.Helper()
	o, err := optionsFor(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	o.UI.PublicHTTPS = false
	o.UI.OIDC = nil
	rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
}

func assertSafeUI(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	s, _ := loadState(rc.Cluster)
	m := s.Members[tspec.UI]
	if m == nil || m.ConfigVersion == 0 {
		return
	}
	service, err := rc.Backend.GetService(t.Context(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if service == nil {
		return
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec tspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	access, err := engine.PublicHTTPS(rc.Backend)
	if err != nil {
		t.Fatal(err)
	}
	urls, err := access.SubdomainURLs(t.Context(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) > 0 && service.Status != infra.StatusStopped && (spec.UI == nil || spec.UI.OIDC == nil) {
		t.Fatal("public route serves unauthenticated UI configuration")
	}
}

func TestMaintenanceRemovesPublicRouteBeforeUnauthenticatedPublication(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			publicUI(t, p, rc)
			op := &domain.Operation{ID: "public-maintenance-" + kind, Kind: kind}
			stepOperation(t, p, rc, op)
			privateUI(t, rc)
			done := false
			for range 100 {
				out := stepOperation(t, p, rc, op)
				assertSafeUI(t, p, rc)
				if out.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("maintenance did not complete")
			}
			s, _ := loadState(rc.Cluster)
			if s.Members[tspec.UI].Public {
				t.Fatal("public route remains")
			}
		})
	}
}

func restoreFixture(t *testing.T) (*Provider, *engine.Context, *backupBindings, *domain.Operation) {
	t.Helper()
	p, rc, bindings, _ := fixture(t, "combined")
	converge(t, p, rc)
	binder := &backupBindings{fakeBindings: bindings, restorePhase: pgbinding.PhaseRunning}
	p.options.Bindings = binder
	o, _ := optionsFor(rc.Cluster)
	s, _ := loadState(rc.Cluster)
	s.Backups = map[string]backupManifest{"snapshot": {
		Version: rc.Cluster.Spec.Version, HistoryShards: o.HistoryShards, ClusterName: rc.Cluster.Name,
		Databases: []string{o.Persistence.Postgres.Database, o.Persistence.Postgres.VisibilityDatabase},
		Source:    domain.ClusterRef{Namespace: o.Persistence.Postgres.Namespace, Name: o.Persistence.Postgres.Cluster},
		Complete:  true, SourceID: "source-backup",
	}}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Store = &operationInventory{
		c: rc.Cluster, r: &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snapshot"}},
	}
	op := &domain.Operation{
		ID: "restore-review", Kind: domain.OperationRestore,
		Payload: json.RawMessage(`{"restore":"restore","acknowledgeSharedSource":true}`),
	}
	return p, rc, binder, op
}

func waitForSourceRestore(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation) {
	t.Helper()
	for range 40 {
		out := stepOperation(t, p, rc, op)
		var cp operationCheckpoint
		json.Unmarshal(out.Checkpoint, &cp)
		if cp.Stage == "wait-restore" {
			s, _ := loadState(rc.Cluster)
			for _, m := range s.Members {
				service, err := rc.Backend.GetService(t.Context(), m.Name)
				if err != nil || service.Status != infra.StatusStopped {
					t.Fatal("restore did not stop every component", err)
				}
			}
			return
		}
	}
	t.Fatal("restore did not reach source wait")
}

func TestFailedRestoreRecoversBeforeReturningOriginalFailure(t *testing.T) {
	p, rc, binder, op := restoreFixture(t)
	waitForSourceRestore(t, p, rc, op)
	binder.restorePhase = pgbinding.PhaseFailed
	out := stepOperation(t, p, rc, op)
	var cp operationCheckpoint
	json.Unmarshal(out.Checkpoint, &cp)
	if cp.Stage != "recover" {
		t.Fatal(cp.Stage)
	}
	rc.Backend.(*testBackend).FailNext("Start", engine.ErrUnavailable)
	out, err := p.RunOperation(t.Context(), rc, op)
	if !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	op.Checkpoint = out.Checkpoint
	s, _ := loadState(rc.Cluster)
	if s.RestoreRecovery == nil {
		t.Fatal("start failure lost recovery ownership")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	fenced := false
	for _, condition := range status.Conditions {
		if condition.Type == "TemporalRestoreRecovery" {
			fenced = true
		}
	}
	if !fenced || status.Phase == domain.PhaseReady {
		t.Fatal("recovery fence missing")
	}
	if claim, _ := p.CanClaimOperation(rc.Cluster); claim {
		t.Fatal("new maintenance bypassed restore recovery")
	}
	for range 50 {
		out, err = p.RunOperation(t.Context(), rc, op)
		op.Checkpoint = out.Checkpoint
		if err != nil {
			break
		}
	}
	if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "PostgreSQL source restore failed") {
		t.Fatal("original failure was not returned after recovery", err)
	}
	s, _ = loadState(rc.Cluster)
	if s.RestoreRecovery != nil {
		t.Fatal("completed recovery journal remains")
	}
	for _, m := range s.Members {
		observed, err := componentStatus(t.Context(), rc, m)
		if err != nil || !observed.Ready {
			t.Fatal("failure returned before runtimes were Ready", err)
		}
	}
	if rc.Store.(*operationInventory).r.Phase != domain.PhaseFailed {
		t.Fatal("restore inventory not failed")
	}
}

func TestSyncRecoversInterruptedRestoreAfterSourceTerminates(t *testing.T) {
	p, rc, binder, op := restoreFixture(t)
	waitForSourceRestore(t, p, rc, op)
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := loadState(rc.Cluster)
	service, _ := rc.Backend.GetService(t.Context(), s.Members[tspec.Server].Name)
	if service.Status != infra.StatusStopped {
		t.Fatal("Sync restarted roles while source restoration ran")
	}
	binder.restorePhase = pgbinding.PhaseFailed
	converge(t, p, rc)
	s, _ = loadState(rc.Cluster)
	if s.RestoreRecovery != nil {
		t.Fatal("interrupted recovery did not finish")
	}
}

func TestSyncRecoversStoppedComponentsWithoutRestoreCheckpoint(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	s, _ := loadState(rc.Cluster)
	pending, err := rc.Backend.Stop(t.Context(), s.Members[tspec.Server].Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rc.Backend.OpStatus(t.Context(), pending); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc)
}

func TestRestoreRestartRemovesPublicRouteBeforePublication(t *testing.T) {
	p, rc, binder, op := restoreFixture(t)
	publicUI(t, p, rc)
	waitForSourceRestore(t, p, rc, op)
	privateUI(t, rc)
	binder.restorePhase = pgbinding.PhaseCompleted
	done := false
	for range 70 {
		out := stepOperation(t, p, rc, op)
		assertSafeUI(t, p, rc)
		if out.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("restore did not complete")
	}
}

type failingDeleteSecrets struct {
	engine.SecretStore
	failKey string
}

func (s *failingDeleteSecrets) DeleteClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) error {
	if key == s.failKey {
		s.failKey = ""
		return engine.ErrUnavailable
	}
	return s.SecretStore.DeleteClusterSecret(ctx, ref, key)
}

func TestRotationTrustDeletionFailurePreservesPruneCursor(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	op := &domain.Operation{ID: "rotation-delete-failure", Kind: domain.OperationRotateTLS}
	for range 100 {
		stepOperation(t, p, rc, op)
		var cp operationCheckpoint
		json.Unmarshal(op.Checkpoint, &cp)
		if cp.Stage == "issue" && cp.Index == len(cp.Order) && !cp.Waiting {
			break
		}
	}
	rc.Secrets = &failingDeleteSecrets{SecretStore: rc.Secrets, failKey: rotationTrustKey}
	out, err := p.RunOperation(t.Context(), rc, op)
	if !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	var failed operationCheckpoint
	json.Unmarshal(out.Checkpoint, &failed)
	if failed.Stage != "issue" || failed.Index != len(failed.Order) {
		t.Fatal("stage advanced with exhausted prune cursor", failed)
	}
	op.Checkpoint = out.Checkpoint
	out = stepOperation(t, p, rc, op)
	var prune operationCheckpoint
	json.Unmarshal(out.Checkpoint, &prune)
	if prune.Stage != "prune" || prune.Index != 0 || prune.Waiting {
		t.Fatal("prune did not reset atomically", prune)
	}
	s, _ := loadState(rc.Cluster)
	old := s.Members[tspec.Server].ConfigVersion
	stepOperation(t, p, rc, op)
	s, _ = loadState(rc.Cluster)
	if s.Members[tspec.Server].ConfigVersion <= old {
		t.Fatal("prune member roll was skipped")
	}
	finishOperation(t, p, rc, op)
}

func TestNamespaceAdmissionBoundsAgentRequest(t *testing.T) {
	for _, tc := range []struct {
		name        string
		count       int
		description string
		wantErr     bool
	}{
		{name: "99-declarations", count: 99},
		{name: "100-declarations", count: 100, wantErr: true},
		{name: "escaped-body-too-large", count: 11, description: strings.Repeat("\x01", 1024), wantErr: true},
		{name: "escaped-body-fits", count: 10, description: strings.Repeat("\x01", 1024)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			o, _ := optionsFor(rc.Cluster)
			for i := range tc.count {
				o.Namespaces = append(o.Namespaces, tspec.Namespace{
					Name: fmt.Sprintf("namespace-%d", i), Retention: "72h", Description: tc.description,
				})
			}
			rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
			err := p.Validate(t.Context(), nil, rc.Cluster)
			if (err != nil) != tc.wantErr {
				t.Fatal(err)
			}
			if err == nil {
				request, err := namespaceRequest(o)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(request)
				if len(request) > 100 || len(raw) > 64<<10 {
					t.Fatal("admitted oversized agent request")
				}
			}
		})
	}
}

type releaseBindings struct {
	*fakeBindings
	ids  []string
	keep []bool
}

func (b *releaseBindings) Release(_ context.Context, id string, keep bool) error {
	b.ids = append(b.ids, id)
	b.keep = append(b.keep, keep)
	return nil
}

func TestCleanupReleasesDeterministicBindingWithoutEnsureReceipt(t *testing.T) {
	for _, mode := range []string{"never-ensured", "ensure-failed", "different-receipt"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b, _ := fixture(t, "combined")
			binder := &releaseBindings{fakeBindings: b}
			p.options.Bindings = binder
			if mode == "ensure-failed" {
				binder.unavailable = true
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "different-receipt" {
				s, _ := loadState(rc.Cluster)
				s.BindingID = "different-binding"
				if err := saveState(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
			}
			o, _ := optionsFor(rc.Cluster)
			want := []string{pgbinding.BindingID(bindingRequest(rc.Cluster, o))}
			if mode == "different-receipt" {
				want = append(want, "different-binding")
			}
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
			if err != nil || !done {
				t.Fatal(done, err)
			}
			if !reflect.DeepEqual(binder.ids, want) {
				t.Fatal("binding release identities", binder.ids, want)
			}
			for _, keep := range binder.keep {
				if !keep {
					t.Fatal("retention flag lost")
				}
			}
		})
	}
}

func TestInitialPublicUICreationIsAuthenticatedBeforeReady(t *testing.T) {
	for _, failDeploy := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry-deployment-%t", failDeploy), func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			o, _ := optionsFor(rc.Cluster)
			o.UI.PublicHTTPS = true
			o.UI.OIDC = &OIDCOptions{Issuer: "https://identity.example", ClientID: "temporal"}
			rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
			if err := p.SetUIOIDCClientSecret(t.Context(), rc, "initial-oidc-secret"); err != nil {
				t.Fatal(err)
			}
			injected := false
			for range 100 {
				s, _ := loadState(rc.Cluster)
				ui := s.Members[tspec.UI]
				if failDeploy && !injected && ui != nil && ui.Stage == engine.ComponentImported {
					rc.Backend.(*testBackend).FailNext("Deploy", engine.ErrUnavailable)
					injected = true
				}
				_, err := p.Sync(t.Context(), rc)
				if err != nil && !(failDeploy && injected && errors.Is(err, engine.ErrUnavailable)) {
					t.Fatal(err)
				}
				assertSafeUI(t, p, rc)
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase != domain.PhaseReady {
					continue
				}
				s, _ = loadState(rc.Cluster)
				ui = s.Members[tspec.UI]
				if !ui.Public || !ui.RuntimeDeployed || failDeploy && !injected {
					t.Fatal("initial public UI did not finish its exposure transition")
				}
				config, err := p.MemberConfig(t.Context(), rc, ui.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec tspec.Spec
				json.Unmarshal(config.EngineSpec, &spec)
				if spec.UI == nil || spec.UI.OIDC == nil || spec.UI.OIDC.ClientSecret != "initial-oidc-secret" {
					t.Fatal("initial public UI became Ready without OIDC")
				}
				return
			}
			t.Fatal("initial public creation did not converge")
		})
	}
}
