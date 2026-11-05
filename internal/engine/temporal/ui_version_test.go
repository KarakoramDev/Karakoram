// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLegacyUIVersionWaitsForPreparedRollout(t *testing.T) {
	for _, failure := range []string{"none", "rejected", "lost-receipt", "failed-receipt", "cancel-failed-receipt"} {
		t.Run(failure, func(t *testing.T) { testLegacyUIVersionWaitsForPreparedRollout(t, failure) })
	}
}

func testLegacyUIVersionWaitsForPreparedRollout(t *testing.T, failure string) {
	f := newLegacyUIRolloutFixture(t)
	p, rc, backend, agents := f.provider, f.rc, f.backend, f.agents
	root, name, binary, before := f.root, f.name, f.binary, f.before
	after := before
	var sp tspec.Spec
	var err error
	// Even the explicit rollout must wait for maintenance before activation.
	cp := agentUpgradeCheckpoint{operationCheckpoint: operationCheckpoint{Stage: "roll", Order: []string{tspec.UI}}, Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}}
	raw, _ := json.Marshal(cp)
	operation := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
	stepOperation(t, p, rc, operation)
	after, err = p.memberConfig(t.Context(), rc, name)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("closed window activated the UI", err)
	}
	rc.Cluster.Spec.Maintenance = nil
	agents.builds = map[string]string{name: engine.CurrentAgentBuild().Build}
	backendFailure := failure
	if failure == "cancel-failed-receipt" {
		backendFailure = "failed-receipt"
	}
	rc.Backend = &uiSubmissionBackend{testBackend: backend, root: root, failure: backendFailure, oldBinary: binary}
	failures := 0
	done := false
	for range 16 {
		p = New(p.options)
		var result engine.OpResult
		var err error
		if failure == "cancel-failed-receipt" && failures > 0 {
			result, err = p.CancelOperation(t.Context(), rc, operation)
		} else {
			result, err = p.RunOperation(t.Context(), rc, operation)
		}
		if result.Checkpoint != nil {
			operation.Checkpoint = result.Checkpoint
		}
		if err != nil {
			expected := errors.Is(err, errUISubmission)
			failedReceipt := failure == "failed-receipt" || failure == "cancel-failed-receipt"
			if failedReceipt {
				expected = strings.Contains(err.Error(), "PREPARING_RUNTIME_FAILED")
			}
			if !expected || failures != 0 {
				t.Fatal(err)
			}
			failures++
			after, e := p.memberConfig(t.Context(), rc, name)
			if e != nil {
				t.Fatal(e)
			}
			if failedReceipt {
				var unprepared tspec.Spec
				if err := json.Unmarshal(after.EngineSpec, &unprepared); err != nil {
					t.Fatal(err)
				}
				if agents.componentReady(name, unprepared) {
					t.Fatal("failed preparation fixture unexpectedly installed its target")
				}
			} else if !reflect.DeepEqual(before, after) {
				t.Fatal("failed UI submission published its unprepared target")
			}
			state, e := loadState(rc.Cluster)
			if e != nil || !failedReceipt && state.Members[tspec.UI].Version != "2.56.0" {
				t.Fatal("failed UI submission replaced its installed version", e)
			}
			continue
		}
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("UI rollout did not finish")
	}
	if (failure == "none" && failures != 0) || (failure != "none" && failures != 1) {
		t.Fatalf("UI submission failures = %d for %s", failures, failure)
	}
	a, err := nativeartifacts.Get("temporal-ui-server")
	if err != nil {
		t.Fatal(err)
	}
	after, err = p.memberConfig(t.Context(), rc, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(after.EngineSpec, &sp); err != nil {
		t.Fatal(err)
	}
	runtime, deployed := backend.Deployed(name)
	if sp.Version != a.Version || sp.InstallDir != "/opt/karakoram/"+a.Name+"-"+a.Version || !deployed || !strings.Contains(strings.Join(runtime.Prepare, "\n"), sp.InstallDir) {
		t.Fatal("rollout did not prepare and activate matching UI", sp.Version, sp.InstallDir)
	}
}

var errUISubmission = errors.New("UI deployment submission failed")

type uiSubmissionBackend struct {
	*testBackend
	root        string
	oldBinary   string
	failure     string
	attempts    int
	uiName      string
	names       []string
	prepared    func(string)
	failedPolls int
}

func (b *uiSubmissionBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	b.names = append(b.names, name)
	if b.uiName != "" && name != b.uiName {
		op, err := b.testBackend.Deploy(ctx, name, deployment)
		if err == nil && b.prepared != nil {
			b.prepared(name)
		}
		return op, err
	}
	b.attempts++
	if b.failure == "always-rejected" || b.attempts == 1 && b.failure == "rejected" {
		return infra.Op{}, errUISubmission
	}
	op, err := b.testBackend.Deploy(ctx, name, deployment)
	if err != nil {
		return op, err
	}
	if b.failure == "always-failed-receipt" || b.attempts == 1 && b.failure == "failed-receipt" {
		return infra.Op{ID: fmt.Sprintf("ui-prepare-failed-%d", b.attempts), Kind: "deploy", ServiceID: name}, nil
	}
	if b.failure == "backend-exhausted" {
		return infra.Op{ID: fmt.Sprintf("ui-prepare-exhausted-%d", b.attempts), Kind: "deploy", ServiceID: name}, nil
	}
	a, err := nativeartifacts.Get("temporal-ui-server")
	if err != nil {
		return op, err
	}
	binary := filepath.Join(b.root, "/opt/karakoram/"+a.Name+"-"+a.Version, "ui-server")
	if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
		return op, err
	}
	if err := os.WriteFile(binary, nil, 0755); err != nil {
		return op, err
	}
	if b.oldBinary != binary {
		if err := os.Remove(b.oldBinary); err != nil && !os.IsNotExist(err) {
			return op, err
		}
	}
	if b.prepared != nil {
		b.prepared(name)
	}
	if b.attempts == 1 && b.failure == "lost-receipt" {
		return infra.Op{}, errUISubmission
	}
	return op, nil
}

func (b *uiSubmissionBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if strings.HasPrefix(op.ID, "ui-prepare-failed-") {
		b.failedPolls++
		return infra.OpState{Done: true, Failed: true, Status: "PREPARING_RUNTIME_FAILED", Message: "target UI executable was not installed"}, nil
	}
	if strings.HasPrefix(op.ID, "ui-prepare-exhausted-") {
		b.failedPolls++
		err := &infra.DeploymentExhaustedError{Service: op.ServiceID, Version: op.ID, Attempts: componentDeploymentAttempts, LastError: "target UI executable was not installed"}
		return infra.OpState{Done: true, Failed: true, Status: "DeploymentRetriesExhausted", Message: err.Error()}, err
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestUnknownIncumbentUIVersionFailsClosed(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	m := &componentMember{Name: memberName(rc.Cluster, tspec.UI), Component: tspec.UI, Stage: engine.ComponentJoined}
	if err := p.recoverUIVersion(t.Context(), rc, m); err == nil || m.Version != "" {
		t.Fatal("unknown incumbent UI inferred the current artifact as installed")
	}
}

type legacyUIRolloutFixture struct {
	provider *Provider
	rc       *engine.Context
	backend  *testBackend
	agents   *fakeAgents
	root     string
	name     string
	binary   string
	before   agentapi.MemberConfig
}

func newLegacyUIRolloutFixture(t *testing.T) legacyUIRolloutFixture {
	t.Helper()
	p, rc, bindings, agents := fixture(t, "combined")
	ui := rc.Cluster.Spec.Components.Extra[tspec.UI]
	ui.Replicas = 0
	rc.Cluster.Spec.Components.Extra[tspec.UI] = ui
	converge(t, p, rc) // No current UI has been deployed or prepared.
	ui.Replicas = 1
	rc.Cluster.Spec.Components.Extra[tspec.UI] = ui
	name := memberName(rc.Cluster, tspec.UI)
	token := &domain.AgentToken{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Member: name, Token: "legacy-ui-token"}
	if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	backend := rc.Backend.(*testBackend)
	service := engine.RuntimeServiceSpec(backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: name, Token: token.Token, Resources: ui.Resources, Containers: 1})
	op, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{service})
	if err != nil {
		t.Fatal(err)
	}
	if status, err := backend.OpStatus(t.Context(), op); err != nil || !status.Done {
		t.Fatal(status, err)
	}
	// Represent the incumbent legacy container as running without ever
	// submitting a deployment with the current release's prepare commands.
	op, err = backend.Start(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := backend.OpStatus(t.Context(), op); err != nil || !status.Done {
		t.Fatal(status, err)
	}
	svc, err := backend.GetService(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	// Genuine released-state fields, with no new installed-revision field and
	// no current preparation receipt. The only installed UI is v0.4.5's 2.56.0.
	raw, _ := json.Marshal(map[string]any{"name": name, "component": tspec.UI, "serviceId": svc.ID, "stage": engine.ComponentJoined, "version": "2.56.0", "replicas": 1, "resources": ui.Resources, "runtimeDeployed": true})
	var member componentMember
	if err := json.Unmarshal(raw, &member); err != nil {
		t.Fatal(err)
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Members[tspec.UI] = &member
	ca, err := ensureCA(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := p.desiredSpec(t.Context(), rc, &member, bindings.conn, ca)
	if err != nil {
		t.Fatal(err)
	}
	if sp.Version != "2.56.0" {
		t.Fatal("legacy member selected current UI", sp.Version)
	}
	root := t.TempDir()
	binary := filepath.Join(root, sp.InstallDir, "ui-server")
	if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, nil, 0755); err != nil {
		t.Fatal(err)
	}
	if err := p.publishConfig(t.Context(), rc, &member, sp, ca); err != nil {
		t.Fatal(err)
	}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(rc.Cluster.Internal, &persisted); err != nil {
		t.Fatal(err)
	}
	delete(persisted["temporal"].(map[string]any)["members"].(map[string]any)[tspec.UI].(map[string]any), "version")
	rc.Cluster.Internal, err = json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	agents.componentReady = func(_ string, spec tspec.Spec) bool {
		if spec.Component != tspec.UI {
			return true
		}
		_, err := os.Stat(filepath.Join(root, spec.InstallDir, "ui-server"))
		return err == nil
	}
	before, err := p.memberConfig(t.Context(), rc, name)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	after, err := p.memberConfig(t.Context(), rc, name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("CP-only reconciliation changed the installed UI configuration")
	}
	if _, deployed := backend.Deployed(name); deployed {
		t.Fatal("CP-only reconciliation deployed the current UI")
	}
	if err := json.Unmarshal(after.EngineSpec, &sp); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, sp.InstallDir, "ui-server")); err != nil {
		t.Fatal("configuration selected an unprepared executable", err)
	}
	return legacyUIRolloutFixture{provider: p, rc: rc, backend: backend, agents: agents, root: root, name: name, binary: binary, before: before}
}
