// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	supabaseagent "github.com/KarakoramDev/Karakoram/internal/agent/supabase"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLegacyComponentVersionsWaitForRollout(t *testing.T) {
	for _, failure := range []string{"none", "rejected", "lost-receipt", "failed-receipt"} {
		t.Run(failure, func(t *testing.T) { testLegacyComponentVersionsWaitForRollout(t, failure) })
	}
}

func testLegacyComponentVersionsWaitForRollout(t *testing.T, failure string) {
	// These are the released v0.4.5 versions, not current members downgraded
	// after convergence. No optional member ever receives current preparation.
	legacy := map[string]string{
		"gateway": "1.39.1", "auth": "2.197.0", "rest": "14.18",
		"realtime": "2.143.3", "storage": "1.81.0", "imgproxy": "3.31.4",
		"meta": "0.100.0", "studio": "2026.10.12-bc27d2661edd",
		"functions": "1.77.4", "pooler": "2.9.13", "analytics": "1.50.15", "vector": "0.53.0",
	}
	p, rc, b := setupProvider(t)
	for _, def := range stackComponents {
		c := rc.Cluster.Spec.Components.Extra[def.Name]
		c.Replicas = 0
		rc.Cluster.Spec.Components.Extra[def.Name] = c
	}
	convergeProvider(t, p, rc) // Only the PostgreSQL core is provisioned.
	for _, def := range stackComponents {
		c := rc.Cluster.Spec.Components.Extra[def.Name]
		c.Replicas = 1
		rc.Cluster.Spec.Components.Extra[def.Name] = c
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, def := range stackComponents {
		name := rc.Cluster.Name + codes[def.Name]
		c := rc.Cluster.Spec.Components.Extra[def.Name]
		// Decode the historical persisted schema; there is deliberately no new
		// revision/preparation field or successful deployment receipt.
		raw, _ := json.Marshal(map[string]any{
			"name": name, "component": def.Name, "serviceId": name + "-id",
			"stage": engine.ComponentJoined, "version": legacy[def.Name],
			"replicas": 1, "resources": c.Resources, "runtimePort": ports[def.Name],
		})
		var m componentMember
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		s.Members[def.Name] = &m
		token := &domain.AgentToken{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Member: name, Token: name}
		if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
			t.Fatal(err)
		}
		b.services[name] = &infra.Service{ID: m.ServiceID, Hostname: name, Kind: infra.KindRuntime, Status: infra.StatusActive, Ownership: map[string]string{}, MinContainers: 1, MaxContainers: 1, Resources: c.Resources}
		b.deployed[name] = true
		if err := saveState(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		sp, err := p.desiredSpec(t.Context(), rc, &m)
		if err != nil {
			t.Fatal(def.Name, err)
		}
		if sp.Version != legacy[def.Name] {
			t.Fatalf("%s legacy rendering selected %s", def.Name, sp.Version)
		}
		// The member filesystem has only the old executable location.
		process, _, err := supabaseagent.Render(sp, root)
		if err != nil {
			t.Fatal(err)
		}
		old := filepath.Join(root, process.Binary)
		if err := os.MkdirAll(filepath.Dir(old), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(old, nil, 0755); err != nil {
			t.Fatal(err)
		}
		if err := p.publishConfig(t.Context(), rc, &m, sp); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// Omit the persisted version field itself, while retaining the old sealed
	// configuration. A missing member pin must not select the current lock.
	var persisted map[string]any
	if err := json.Unmarshal(rc.Cluster.Internal, &persisted); err != nil {
		t.Fatal(err)
	}
	for _, raw := range persisted["supabase"].(map[string]any)["members"].(map[string]any) {
		delete(raw.(map[string]any), "version")
	}
	rc.Cluster.Internal, err = json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	b.componentReady = func(_ string, config agentapi.MemberConfig) bool {
		var spec sbspec.Spec
		if json.Unmarshal(config.EngineSpec, &spec) != nil {
			return false
		}
		process, _, err := supabaseagent.Render(spec, root)
		if err != nil {
			return false
		}
		_, err = os.Stat(filepath.Join(root, process.Binary))
		return err == nil
	}
	// Repeated CP-only reconciliation, including a provider reconstruction,
	// must neither deploy artifacts nor publish a different executable path.
	for range 3 {
		p = New(p.options)
		if _, err := p.syncComponents(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range stackComponents {
		m := s.Members[def.Name]
		if m.Version != legacy[def.Name] || m.Stage != engine.ComponentJoined || len(b.prepares[m.Name]) != 0 {
			t.Fatalf("CP-only reconcile changed %s: %+v", def.Name, m)
		}
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp sbspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &sp); err != nil {
			t.Fatal(err)
		}
		process, _, err := supabaseagent.Render(sp, root)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(root, process.Binary)); err != nil {
			t.Fatalf("%s selected an unprepared executable: %v", def.Name, err)
		}
	}
	// The documented, maintenance-gated rollout is allowed to prepare and
	// activate the current lock, including same-version rebuilt artifacts.
	b.agentBuilds = map[string]string{}
	wrapped := &componentSubmissionBackend{agentRolloutBackend: &agentRolloutBackend{testBackend: b}, provider: p, rc: rc, root: root, failure: failure}
	rc.Backend = wrapped
	order := make([]string, 0, len(stackComponents))
	for _, def := range stackComponents {
		order = append(order, def.Name)
	}
	cp := agentUpgradeCheckpoint{Target: engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true}, CoreDone: true, Order: order}
	raw, _ := json.Marshal(cp)
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	beforeFailure, err := p.MemberConfig(t.Context(), rc, s.Members["gateway"].Name)
	if err != nil {
		t.Fatal(err)
	}
	failures := 0
	done := false
	for range 100 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if r.Checkpoint != nil {
			op.Checkpoint = r.Checkpoint
		}
		if err != nil {
			expected := errors.Is(err, errComponentSubmission)
			if failure == "failed-receipt" {
				expected = strings.Contains(err.Error(), "PREPARING_RUNTIME_FAILED")
			}
			if !expected || failures != 0 {
				t.Fatal(err)
			}
			failures++
			after, e := p.MemberConfig(t.Context(), rc, s.Members["gateway"].Name)
			if e != nil {
				t.Fatal(e)
			}
			if failure == "failed-receipt" {
				if b.componentReady(after.Member, after) {
					t.Fatal("failed preparation fixture unexpectedly has its target executable")
				}
			} else if !reflect.DeepEqual(beforeFailure, after) {
				t.Fatal("failed submission published an unprepared target")
			}
			state, e := loadState(rc.Cluster)
			if e != nil || failure != "failed-receipt" && state.Members["gateway"].Version != legacy["gateway"] {
				t.Fatal("failed submission replaced the installed version", e)
			}
			continue
		}
		if r.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("component rollout did not finish")
	}
	if (failure == "none" && failures != 0) || (failure != "none" && failures != 1) {
		t.Fatalf("submission failures = %d for %s", failures, failure)
	}
	s, _ = loadState(rc.Cluster)
	for _, def := range stackComponents {
		a, err := nativeartifacts.Get(componentArtifact(def.Name))
		if err != nil {
			t.Fatal(err)
		}
		m := s.Members[def.Name]
		if m.Version != a.Version || m.Stage != engine.ComponentJoined {
			t.Fatalf("rollout did not activate %s: %+v", def.Name, m)
		}
		path := "/opt/karakoram/" + a.Name + "-" + a.Version
		if !strings.Contains(strings.Join(b.prepares[m.Name], "\n"), path) {
			t.Fatalf("%s activated without matching preparation", def.Name)
		}
	}
}

var errComponentSubmission = errors.New("component submission failed")

type componentSubmissionBackend struct {
	*agentRolloutBackend
	provider *Provider
	rc       *engine.Context
	root     string
	failure  string
	attempts int
}

func (b *componentSubmissionBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	if deployment.Runtime.Env["KARAKORAM_ENGINE"] != "supabase" {
		return b.agentRolloutBackend.Deploy(ctx, name, deployment)
	}
	b.attempts++
	if b.attempts == 1 && b.failure == "rejected" {
		return infra.Op{}, errComponentSubmission
	}
	config, err := b.provider.MemberConfig(ctx, b.rc, name)
	if err != nil {
		return infra.Op{}, err
	}
	var installed sbspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &installed); err != nil {
		return infra.Op{}, err
	}
	oldProcess, _, err := supabaseagent.Render(installed, b.root)
	if err != nil {
		return infra.Op{}, err
	}
	a, err := nativeartifacts.Get(componentArtifact(installed.Component))
	if err != nil {
		return infra.Op{}, err
	}
	prepared := installed
	prepared.Version, prepared.InstallDir = a.Version, "/opt/karakoram/"+a.Name+"-"+a.Version
	process, _, err := supabaseagent.Render(prepared, b.root)
	if err != nil {
		return infra.Op{}, err
	}
	op, err := b.agentRolloutBackend.Deploy(ctx, name, deployment)
	if err != nil {
		return op, err
	}
	if b.attempts == 1 && b.failure == "failed-receipt" {
		return infra.Op{ID: "gateway-prepare-failed", ServiceID: name + "-id", Kind: "deploy"}, nil
	}
	path := filepath.Join(b.root, process.Binary)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return op, err
	}
	if err := os.WriteFile(path, nil, 0755); err != nil {
		return op, err
	}
	if oldProcess.Binary != process.Binary {
		if err := os.Remove(filepath.Join(b.root, oldProcess.Binary)); err != nil && !os.IsNotExist(err) {
			return op, err
		}
	}
	if b.attempts == 1 && b.failure == "lost-receipt" {
		return infra.Op{}, errComponentSubmission
	}
	return op, nil
}

func (b *componentSubmissionBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "gateway-prepare-failed" {
		return infra.OpState{Done: true, Failed: true, Status: "PREPARING_RUNTIME_FAILED", Message: "target executable was not installed"}, nil
	}
	return b.agentRolloutBackend.OpStatus(ctx, op)
}

func TestUnknownIncumbentComponentVersionFailsClosed(t *testing.T) {
	p, rc, _ := setupProvider(t)
	for _, def := range stackComponents {
		m := &componentMember{Name: "unknown" + codes[def.Name], Component: def.Name, Stage: engine.ComponentJoined}
		if err := p.recoverComponentVersion(t.Context(), rc, m); err == nil || m.Version != "" {
			t.Fatalf("%s inferred an installation from the current lock", def.Name)
		}
	}
}
