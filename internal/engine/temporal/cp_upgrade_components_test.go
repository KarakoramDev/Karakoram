// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// The complete combined-cluster state/config/runtime was emitted by the real
// fixture/converge path in 441b0639, before the Temporal/UI artifact bumps.
func TestCPUpgradeRetainsPreparedTemporalAndUI(t *testing.T) {
	for _, version := range []string{"1.31.4", "1.32.1"} {
		t.Run(version, func(t *testing.T) { testCPUpgradeRetainsPreparedTemporalAndUI(t, version) })
	}
}

func testCPUpgradeRetainsPreparedTemporalAndUI(t *testing.T, version string) {
	p, rc, _, agents := fixture(t, "combined")
	backend := rc.Backend.(*testBackend)
	f := enginetest.RestoreCPUpgrade(t, rc, "testdata/cp-upgrade-components-"+version+"-441b0639.json")
	if f.Revision != "441b0639" || rc.Cluster.Spec.Version != version || len(f.Configs) != 2 || len(f.Runtimes) != 2 {
		t.Fatal("fixture does not contain genuine pre-upgrade server and UI runtimes")
	}
	clock := &testClock{now: f.At}
	rc.Clock = clock
	serverVersion := rc.Cluster.Spec.Version
	// Restore the old infrastructure observations with the captured old runtime,
	// not with today's runtime hash or preparation. The original service IDs
	// follow import/deploy order and remain unchanged across the CP upgrade.
	for _, service := range f.Services {
		runtime := f.Runtimes[service.Hostname]
		spec := infra.ServiceSpec{Hostname: service.Hostname, Kind: service.Kind, Base: runtime.Base, Resources: service.Resources, MinContainers: service.MinContainers, MaxContainers: service.MaxContainers, Env: f.Ownership[service.Hostname]}
		op, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{spec})
		if err != nil {
			t.Fatal(err)
		}
		if status, err := backend.OpStatus(t.Context(), op); err != nil || !status.Done {
			t.Fatal(status, err)
		}
		observed, err := backend.GetService(t.Context(), service.Hostname)
		if err != nil || observed.ID != service.ID {
			t.Fatal("legacy service identity changed during restoration", observed, service.ID, err)
		}
		op, err = engine.Deploy(t.Context(), rc, service.Hostname, runtime)
		if err != nil {
			t.Fatal(err)
		}
		if status, err := backend.OpStatus(t.Context(), op); err != nil || !status.Done {
			t.Fatal(status, err)
		}
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	ui := s.Members[tspec.UI]
	var legacy tspec.Spec
	if err := json.Unmarshal(f.Configs[ui.Name].EngineSpec, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.Version != "2.56.0" || !strings.Contains(strings.Join(f.Runtimes[ui.Name].Prepare, "\n"), "ui-server_2.56.0") {
		t.Fatal("UI fixture did not preserve the old locked preparation")
	}
	root := t.TempDir()
	binary := filepath.Join(root, legacy.InstallDir, "ui-server")
	if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, nil, 0755); err != nil {
		t.Fatal(err)
	}
	agents.componentReady = func(_ string, spec tspec.Spec) bool {
		if spec.Component != tspec.UI {
			return true
		}
		_, err := os.Stat(filepath.Join(root, spec.InstallDir, "ui-server"))
		return err == nil
	}
	agents.builds = map[string]string{}
	prepared := &uiSubmissionBackend{testBackend: backend, root: root, oldBinary: binary, uiName: ui.Name, prepared: func(name string) { agents.builds[name] = engine.CurrentAgentBuild().Build }}
	rc.Backend = prepared
	for range 3 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(prepared.names) != 0 {
		t.Fatal("CP-only lock update redeployed Temporal/UI", prepared.names)
	}
	for name, before := range f.Configs {
		config, err := p.memberConfig(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		var oldSpec, newSpec tspec.Spec
		if err := json.Unmarshal(before.EngineSpec, &oldSpec); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(config.EngineSpec, &newSpec); err != nil {
			t.Fatal(err)
		}
		if oldSpec.Version != newSpec.Version || oldSpec.InstallDir != newSpec.InstallDir {
			t.Fatalf("CP-only upgrade changed %s's installed version/path", name)
		}
	}

	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	done := false
	for range 40 {
		p = New(p.options)
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Checkpoint != nil {
			op.Checkpoint = result.Checkpoint
		}
		if result.Done {
			done = true
			break
		}
		clock.now = clock.now.Add(time.Minute)
	}
	if !done {
		t.Fatal("explicit prepared Temporal/UI rollout did not finish")
	}
	a, err := nativeartifacts.Get("temporal-ui-server")
	if err != nil {
		t.Fatal(err)
	}
	config, err := p.memberConfig(t.Context(), rc, ui.Name)
	if err != nil {
		t.Fatal(err)
	}
	var active tspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &active); err != nil {
		t.Fatal(err)
	}
	runtime, ok := backend.Deployed(ui.Name)
	if !ok || active.Version != a.Version || !strings.Contains(strings.Join(runtime.Prepare, "\n"), a.SHA256) || rc.Cluster.Spec.Version != serverVersion {
		t.Fatal("rollout did not prepare current UI while retaining the server version")
	}
}
