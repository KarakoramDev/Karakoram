// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// These fixtures were emitted by setup/ready at a50af33, before both the
// server/plugin lock updates and 4c218d29. No current provider provisioned them;
// their credentials, certificates and unseal shares are isolated test data.
func TestCPUpgradePreservesExistingOpenBaoRuntimeAndUnseal(t *testing.T) {
	for _, seal := range []string{"shamir", "awskms"} {
		t.Run(seal, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/cp-upgrade-a50af33-" + seal + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var fixture struct {
				Cluster       domain.Cluster
				Secrets       map[string][]byte
				Tokens        map[string]*domain.AgentToken
				At            time.Time
				Internal      json.RawMessage
				EngineOptions string
			}
			if err = json.Unmarshal(enginetest.RestoreFixtureKeys(t, raw), &fixture); err != nil {
				t.Fatal(err)
			}
			p, rc, f, a := setup(t)
			*rc.Cluster = fixture.Cluster
			rc.Cluster.Internal = fixture.Internal
			// MarshalIndent expands RawMessage; preserve the original option bytes
			// because the legacy restart fingerprint includes their exact text.
			rc.Cluster.Spec.EngineOptions = json.RawMessage(fixture.EngineOptions)
			rc.Clock = &testClock{at: fixture.At}
			for key, value := range fixture.Secrets {
				if err = putSecret(t.Context(), rc, key, value); err != nil {
					t.Fatal(err)
				}
			}
			for _, token := range fixture.Tokens {
				if err = rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
					t.Fatal(err)
				}
			}
			s := state(t, rc)
			material, err := readUnseal(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			a.shares = material.Shares
			a.management = string(fixture.Secrets[SecretManagementToken])
			a.tokenValid = true
			directory := t.TempDir()
			legacy := []baospec.Plugin{{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_linux_amd64_v1", Version: "v0.0.1"}}
			if seal == "awskms" {
				legacy = append(legacy, baospec.Plugin{Type: "kms", Name: "awskms", Command: "openbao-plugin-kms-aws_linux_amd64_v1", Version: "v0.1.0"})
			}
			for _, plugin := range legacy {
				if err = os.WriteFile(filepath.Join(directory, plugin.Command), []byte("legacy test executable"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			generations := map[string]int64{}
			for _, m := range s.Members {
				if m.PluginRevision != "" || m.Version != "2.7.1" || m.Stage != "ready" {
					t.Fatalf("not legacy installed state: %+v", m)
				}
				token := fixture.Tokens[m.Name]
				_, err = f.ImportServices(t.Context(), []infra.ServiceSpec{
					engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, m.Disk),
					engine.RuntimeServiceSpec(f.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1}),
				})
				if err != nil {
					t.Fatal(err)
				}
				a.members[m.Name] = &fakeBao{initialized: true, sealed: false, active: m.Active, running: true, voter: true, healthy: true, version: m.Version, generation: m.ProcessGeneration, config: m.ConfigVersion}
				generations[m.Name] = m.ProcessGeneration
			}
			f.deployments = nil
			f.imports = nil
			f.log.clear()
			for name, want := range map[string]string{"openbao": "2.7.2", "openbao-plugin-database-mongodb": "0.0.2", "openbao-plugin-kms-aws": "0.1.1"} {
				artifact, err := nativeartifacts.Get(name)
				if err != nil {
					t.Fatal(err)
				}
				if artifact.Version != want {
					t.Fatalf("unexpected upgraded lock %s=%s", name, artifact.Version)
				}
			}
			assertInstalled := func() {
				t.Helper()
				for _, m := range state(t, rc).Members {
					cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
					if err != nil {
						t.Fatal(err)
					}
					var spec baospec.MemberSpec
					if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
						t.Fatal(err)
					}
					if spec.BinaryPath != "/opt/karakoram/openbao-2.7.1/bao" || spec.ProcessGeneration != generations[m.Name] || !reflect.DeepEqual(spec.Plugins, legacy) {
						t.Fatalf("CP activated unprepared runtime: binary=%s generation=%d plugins=%+v change=%+v", spec.BinaryPath, spec.ProcessGeneration, spec.Plugins, m.Change)
					}
					for _, plugin := range spec.Plugins {
						info, err := os.Stat(filepath.Join(directory, plugin.Command))
						if err != nil || info.Mode()&0111 == 0 {
							t.Fatalf("restart/unseal cannot resolve installed plugin %s: %v", plugin.Command, err)
						}
					}
				}
			}
			reconcile := func() {
				t.Helper()
				p = New(p.options)
				a.p = p
				result, err := p.Sync(t.Context(), rc)
				if err != nil || result.ConvergenceError != nil {
					t.Fatalf("CP reconcile: %v %v", err, result.ConvergenceError)
				}
				assertInstalled()
			}
			assertInstalled()
			for range 12 {
				reconcile()
			}
			// A native restart is independent of CP replacement. The persisted legacy
			// declarations must remain usable while CP recovers the sealed process.
			target := s.Members[len(s.Members)-1].Name
			restarted := a.member(target)
			restarted.sealed = true
			restarted.active = false
			restarted.healthy = false
			if seal == "awskms" {
				// KMS performs auto-unseal in the server, never with Shamir recovery shares.
				reconcile()
				if restarted.unseals != 0 {
					t.Fatal("CP sent recovery shares to auto-unseal member")
				}
				restarted.sealed = false
				restarted.active = true
				restarted.healthy = true
			}
			for range 12 {
				reconcile()
			}
			if restarted.sealed || !restarted.healthy {
				t.Fatal("existing member did not recover after native restart")
			}
			if seal == "shamir" && restarted.unseals != material.Threshold {
				t.Fatalf("Shamir recovery did not use persisted shares: %d", restarted.unseals)
			}
			if len(f.deployments) != 0 || len(f.imports) != 0 {
				t.Fatalf("CP-only upgrade redeployed existing members: deploy=%d import=%d", len(f.deployments), len(f.imports))
			}
			for _, call := range f.log.snapshot() {
				if strings.HasPrefix(call, "restart:") || strings.HasSuffix(call, ":deploy") {
					t.Fatalf("CP-only upgrade restarted runtime: %s", call)
				}
			}
			for _, m := range state(t, rc).Members {
				if m.PluginRevision != "" || m.ResumeDeploy != nil || m.Change != nil {
					t.Fatalf("CP-only upgrade staged plugin rollout: %+v", m)
				}
			}
			// Only an explicit desired-version change may prepare current plugins.
			rc.Cluster.Spec.Version = "2.7.2"
			rc.Cluster.Generation++
			f.done = false
			for range 80 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if len(f.deployments) > 0 {
					break
				}
			}
			if len(f.deployments) != 1 {
				t.Fatal("explicit upgrade did not submit preparation")
			}
			// Submission is not installation: the first member's unresolved
			// receipt must still publish only its legacy declarations.
			for _, m := range state(t, rc).Members {
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec baospec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(spec.Plugins, legacy) {
					t.Fatal("unresolved preparation activated plugins")
				}
			}
			f.done = true
			complete := false
			for range 250 {
				p = New(p.options)
				a.p = p
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if seal == "awskms" {
					// Model native KMS auto-unseal after the prepared replacement.
					b := a.member(target)
					b.sealed, b.active, b.healthy = false, true, true
				}
				complete = true
				for _, m := range state(t, rc).Members {
					complete = complete && m.PluginRevision == pluginRevision && m.Version == "2.7.2" && m.Change == nil && m.Healthy
				}
				if complete {
					break
				}
			}
			if !complete || len(f.deployments) != len(s.Members) {
				t.Fatalf("explicit rollout incomplete: complete=%v deploys=%d", complete, len(f.deployments))
			}
			options := Options{}
			options.Seal.Type = seal
			for _, m := range state(t, rc).Members {
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec baospec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(spec.Plugins, memberPlugins(options, pluginRevision)) {
					t.Fatal("prepared member did not activate plugins")
				}
			}
			for _, deployment := range f.deployments {
				preparation := strings.Join(deployment.Runtime.Prepare, "\n")
				if !strings.Contains(preparation, "openbao_2.7.2_linux_amd64.tar.gz") || !strings.Contains(preparation, mongodbPluginCommand) {
					t.Fatal("explicit rollout did not prepare current server and plugins")
				}
			}
		})
	}
}
