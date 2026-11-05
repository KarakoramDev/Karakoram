// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	supabaseagent "github.com/KarakoramDev/Karakoram/internal/agent/supabase"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// This snapshot was emitted by setupProvider/convergeProvider in 441b0639,
// including all twelve enabled applications and their genuinely old lock/runtime.
// It is not a current cluster with a version or runtime fingerprint overwritten.
func TestCPUpgradeRetainsPreparedSupabaseComponents(t *testing.T) {
	p, rc, backend := setupProvider(t)
	f := enginetest.RestoreCPUpgrade(t, rc, "testdata/cp-upgrade-components-441b0639.json")
	if f.Revision != "441b0639" || len(f.Executables) != len(stackComponents) {
		t.Fatal("fixture does not contain the complete pre-upgrade component set")
	}
	backend.services = map[string]*infra.Service{}
	for i := range f.Services {
		backend.services[f.Services[i].Hostname] = &f.Services[i]
	}
	backend.deployed = map[string]bool{}
	for name := range f.Configs {
		backend.deployed[name] = true
	}
	backend.prepares = map[string][]string{}
	var storageSpec sbspec.Spec
	if err := json.Unmarshal(f.Configs[rc.Cluster.Name+codes["storage"]].EngineSpec, &storageSpec); err != nil {
		t.Fatal(err)
	}
	objects := &domain.BackupStorage{
		Name: "objects", Namespace: rc.Cluster.Namespace, Type: "s3",
		EndpointURL: storageSpec.Storage.Endpoint, Bucket: storageSpec.Storage.Bucket,
		Region: storageSpec.Storage.Region, ForcePathStyle: storageSpec.Storage.PathStyle, VerifyTLS: true,
		Credentials: domain.StorageCredentials{AccessKeyID: storageSpec.Storage.AccessKey, SecretAccessKey: storageSpec.Storage.SecretKey},
	}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), objects); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, executable := range f.Executables {
		path := filepath.Join(root, executable)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0755); err != nil {
			t.Fatal(err)
		}
	}
	backend.componentReady = func(_ string, config agentapi.MemberConfig) bool {
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
	for range 3 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.prepares) != 0 {
		t.Fatal("CP-only lock update redeployed prepared members", backend.prepares)
	}
	for name, executable := range f.Executables {
		config, err := p.MemberConfig(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		var before, after sbspec.Spec
		if err := json.Unmarshal(f.Configs[name].EngineSpec, &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(config.EngineSpec, &after); err != nil {
			t.Fatal(err)
		}
		process, _, err := supabaseagent.Render(after, root)
		if err != nil || after.Version != before.Version || after.InstallDir != before.InstallDir || process.Binary != executable {
			t.Fatalf("CP-only upgrade activated an unprepared %s artifact: %v", name, err)
		}
	}

	// The normal explicit rollout prepares every current locked payload, even
	// when only its checksum/dependency bytes changed at the same version/path.
	backend.agentBuilds = map[string]string{}
	rc.Backend = &componentSubmissionBackend{agentRolloutBackend: &agentRolloutBackend{testBackend: backend}, provider: p, rc: rc, root: root, failure: "none"}
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: true})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	done := false
	for range 150 {
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
	}
	if !done {
		t.Fatal("prepared component rollout did not finish")
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range stackComponents {
		a, err := nativeartifacts.Get(componentArtifact(def.Name))
		if err != nil {
			t.Fatal(err)
		}
		m := s.Members[def.Name]
		if m.Version != a.Version || !strings.Contains(strings.Join(backend.prepares[m.Name], "\n"), a.SHA256) {
			t.Fatalf("rollout did not prepare/activate current locked bytes for %s", def.Name)
		}
	}
}
