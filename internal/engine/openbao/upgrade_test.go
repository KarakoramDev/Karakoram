// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func TestVersionUpgradeRollsStandbysFirst(t *testing.T) {
	p, rc, f, a := setup(t)
	rc.Cluster.Spec.Version = "2.7.1"
	ready(t, p, rc)
	const newer = "2.7.2"
	old := *rc.Cluster
	upgraded := *rc.Cluster
	upgraded.Spec.Version = newer
	if err := p.Validate(t.Context(), &old, &upgraded); err != nil {
		t.Fatalf("forward catalog upgrade rejected: %v", err)
	}
	if err := p.Validate(t.Context(), &upgraded, &old); err == nil {
		t.Fatal("downgrade accepted")
	}
	previousGeneration := rc.Cluster.Status.ObservedGeneration
	rc.Cluster.Spec.Version = newer
	rc.Cluster.Generation++
	f.log.clear()
	initialDeployments := len(f.deployments)
	complete := false
	for range 250 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s := state(t, rc)
		sealed := 0
		allReady := true
		for _, m := range s.Members {
			if a.member(m.Name).sealed {
				sealed++
			}
			allReady = allReady && m.Version == newer && m.DesiredVersion == newer && m.Healthy && !m.Sealed && m.Change == nil
		}
		if sealed > 1 {
			t.Fatalf("upgrade disrupted %d members concurrently", sealed)
		}
		if !allReady && rc.Cluster.Status.ObservedGeneration != previousGeneration {
			t.Fatal("observed generation advanced before every upgraded voter recovered")
		}
		if s.Phase == "ready" && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatalf("upgrade did not converge: %s", rc.Cluster.Internal)
	}
	calls := f.log.snapshot()
	deploys := []string{}
	for _, call := range calls {
		if strings.HasSuffix(call, ":deploy") {
			deploys = append(deploys, call)
		}
	}
	if !slices.Equal(deploys, []string{"infra:demom1:deploy", "infra:demom2:deploy", "infra:demom0:deploy"}) {
		t.Fatalf("binary deployment order: %v", deploys)
	}
	stepDown := slices.Index(calls, "step-down:demom0")
	activeDeploy := slices.Index(calls, "infra:demom0:deploy")
	if stepDown < 0 || stepDown >= activeDeploy {
		t.Fatalf("active redeployed without prior step-down: %v", calls)
	}
	for _, deployment := range f.deployments[initialDeployments:] {
		preparation := strings.Join(deployment.Runtime.Prepare, "\n")
		if !strings.Contains(preparation, "/opt/karakoram/openbao-"+newer) || !strings.Contains(preparation, "openbao_"+newer+"_linux_amd64.tar.gz") {
			t.Fatalf("upgrade did not prepare the target binary: %s", preparation)
		}
		if !strings.Contains(preparation, "225c89a188052067d896395a344fc40ba951808cb145ddfa6b07b0be296e632d") || !strings.Contains(preparation, "2b33fa43f0e89dab29f1c4646b49039672285f3d3585f83d74785c24cddd44ca") {
			t.Fatal("member upgrade did not install both locked MongoDB binaries")
		}
	}
	if len(f.deployments)-initialDeployments != 3 {
		t.Fatal("upgrade did not redeploy exactly once per member")
	}
	for _, m := range state(t, rc).Members {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec baospec.MemberSpec
		if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.BinaryPath != "/opt/karakoram/openbao-"+newer+"/bao" || spec.Version != newer {
			t.Fatalf("incorrect upgraded member binary: %+v", spec)
		}
		if len(spec.Plugins) != 2 || spec.Plugins[0].Version != "v0.0.1" || spec.Plugins[1].Version != mongodbPluginVersion || m.PluginRevision != pluginRevision {
			t.Fatalf("member upgrade lost plugin registrations or deployment receipt: %+v", spec.Plugins)
		}
		if a.member(m.Name).unseals != 6 || m.Sealed || !m.Healthy {
			t.Fatalf("upgraded member not re-unsealed and healthy: %+v", m)
		}
	}
}

func TestExplicitUpgradeActivatesPluginsAfterControllerRestart(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Version = "2.7.1"
	ready(t, p, rc)
	s := state(t, rc)
	for _, m := range s.Members {
		m.PluginRevision = ""
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Version = "2.7.2"
	rc.Cluster.Generation++
	initialDeployments := len(f.deployments)
	// Persist the member change before recreating the controller.
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	p = New(p.options)
	for range 250 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		allInstalled := true
		for _, m := range state(t, rc).Members {
			allInstalled = allInstalled && m.PluginRevision == pluginRevision && m.Change == nil && m.Healthy
		}
		if allInstalled {
			if len(f.deployments)-initialDeployments != 3 {
				t.Fatal("explicit upgrade did not redeploy exactly once per member")
			}
			return
		}
	}
	t.Fatal("explicit upgrade failed to recover after controller restart")
}
