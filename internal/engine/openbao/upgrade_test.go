// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func TestVersionUpgradeRollsStandbysFirst(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	const newer = "2.7.2"
	originalCatalog, originalLookup := catalog, lookupRuntimeArtifact
	t.Cleanup(func() { catalog = originalCatalog; lookupRuntimeArtifact = originalLookup })
	catalog = append(slices.Clone(catalog), domain.VersionInfo{Version: newer, Major: "2.7", Status: "supported"})
	lookupRuntimeArtifact = func(name string) (nativeartifacts.Artifact, error) {
		artifact, err := originalLookup(name)
		if name == "openbao" {
			artifact.Version = newer
			artifact.File = "openbao_" + newer + "_linux_amd64.tar.gz"
			artifact.UpstreamURL = "https://artifacts.example.test/" + artifact.File
		}
		return artifact, err
	}
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
		if a.member(m.Name).unseals != 6 || m.Sealed || !m.Healthy {
			t.Fatalf("upgraded member not re-unsealed and healthy: %+v", m)
		}
	}
}
