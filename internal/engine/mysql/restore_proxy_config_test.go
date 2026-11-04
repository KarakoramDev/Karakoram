// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCRestoreProxyCandidatePassesRealValidator(t *testing.T) {
	for _, scenario := range []string{"fenced", "releasing", "no-restore", "uninitialized", "unadmitted", "removing", "joined-preferred"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			seed := s.find(s.Formation.Seed)
			proxy := ""
			for _, m := range s.Members {
				if m.Role == "proxy" {
					proxy = m.Name
				}
			}
			if proxy == "" {
				t.Fatal("fixture lacks proxy")
			}
			for _, m := range s.data() {
				m.Stage, m.Initialized = "deployed", m.Name == seed.Name
			}
			if scenario != "no-restore" {
				op := &domain.Operation{ID: "restore-proxy-config", Kind: "initial-restore"}
				if err := persistRestore(rc, op, backupCheckpoint{Step: "reform", Request: &myspec.Restore{}, GTIDCertification: &pxcRestoreCertification{Negotiated: true, FencesOwned: true}}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "fenced" {
				s.RestoreGTIDFence = "restore-proxy-config"
			}
			switch scenario {
			case "uninitialized":
				seed.Initialized = false
			case "unadmitted":
				seed.ServiceID = ""
			case "removing":
				seed.Removing = true
			case "joined-preferred":
				s.data()[1].Stage = "joined"
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, proxy)
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			// This is the actual validator used by the proxy agent's decodeSpec/Apply,
			// not the old fixture's unconditional successful reload response.
			validation := spec.Validate()
			valid := scenario == "fenced" || scenario == "releasing" || scenario == "joined-preferred"
			if valid {
				if validation != nil {
					t.Fatalf("owned restore generated invalid proxy image: %v", validation)
				}
				if spec.Proxy.MaxConnections < 1 || len(spec.Proxy.Backends) != 1 {
					t.Fatalf("invalid backend/limit image: %+v", spec.Proxy)
				}
				expected := seed.Name
				if scenario == "joined-preferred" {
					expected = s.data()[1].Name
				}
				if spec.Proxy.Backends[0].Name != expected {
					t.Fatal("restore published an untrusted or unnecessary backend")
				}
				if scenario == "fenced" && len(config.DesiredProcesses) != 0 {
					t.Fatal("candidate address was mistaken for permission to serve clients")
				}
			} else if validation == nil || !strings.Contains(validation.Error(), "proxy backends and positive connection limit required") {
				t.Fatalf("unowned/unproven empty proxy image bypassed strict validator: %v", validation)
			}
		})
	}
}

func TestPXCRestorePersistedReformRecoversWithRealProxyValidation(t *testing.T) {
	for _, outcome := range []string{"resume", "terminal-cleanup"} {
		t.Run(outcome, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			rc.Cluster.Spec.Backup.Storage = ""
			s, _ := load(rc.Cluster)
			s.Formation.GTIDCertification = nil // Model native restore.install, not the old formation's proof.
			op := &domain.Operation{ID: "persisted-reform", Kind: "initial-restore"}
			dataHeld, proxyHeld := map[string]bool{}, map[string]bool{}
			s.Formation.FormationEpoch++
			s.Formation.State = "bootstrapping"
			s.Formation.Reformation = &reformation{State: "bootstrapping"}
			s.RestoreGTIDFence = op.ID
			for _, m := range s.Members {
				if m.Role == "proxy" {
					proxyHeld[m.Name] = true
					continue
				}
				dataHeld[m.Name] = true
				m.Stage, m.Initialized, m.Offline = "deployed", m.Name == s.Formation.Seed, m.Name != s.Formation.Seed
				native := b.native[m.Name]
				native.MemberState, native.DataIdentity.FormationEpoch = "OFFLINE", 0
				b.native[m.Name] = native
			}
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			backup, _ := loadBackupState(rc.Cluster)
			generation := backup.Generation + 1
			cp := backupCheckpoint{Step: "reform", Member: s.Formation.Seed, Generation: generation, Request: &myspec.Restore{NewGroupName: s.Formation.GroupName}, GTIDCertification: &pxcRestoreCertification{Negotiated: true, Phase: "negotiated", FencesOwned: true}}
			if err := persistRestore(rc, op, cp); err != nil {
				t.Fatal(err)
			}
			validations, releases := 0, 0
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/status" {
					pid, process := 1, "mysqld"
					if held, proxy := proxyHeld[name]; proxy {
						process = "proxysql"
						if held {
							pid = 0
						}
					} else if dataHeld[name] {
						pid = 0
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: b.agentBuild, MySQLRestoreCertification: true, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{process: {PID: pid}}})
					return true
				}
				if _, proxy := proxyHeld[name]; proxy && r.URL.Path == "/v1/reload" {
					config, err := p.MemberConfig(r.Context(), rc, name)
					if err != nil {
						t.Error(err)
						http.Error(w, "configuration unavailable", http.StatusServiceUnavailable)
						return true
					}
					var spec myspec.Spec
					if err = json.Unmarshal(config.EngineSpec, &spec); err == nil {
						err = spec.Validate()
					}
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return true
					}
					validations++
					b.applied[name] = config.ConfigVersion
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/proxysql/") {
					if strings.HasSuffix(r.URL.Path, "/stop") {
						proxyHeld[name] = true
					}
					if strings.HasSuffix(r.URL.Path, "/start") {
						proxyHeld[name] = false
						releases++
					}
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/mysqld/start") {
					dataHeld[name] = false
					_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				if r.URL.Path == "/v1/mysql/status" && dataHeld[name] {
					http.Error(w, "restore-owned process stop", http.StatusServiceUnavailable)
					return true
				}
				if r.URL.Path == "/v1/mysql/galera-prepare-join" {
					_ = json.NewEncoder(w).Encode(struct{}{})
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			// The fixed CP starts from the old persisted reform checkpoint, not a new
			// restore request. The real validator is exercised on every proxy reload.
			p = New(Options{Provider: "pxc"})
			if outcome == "terminal-cleanup" {
				if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
			} else {
				done := false
				for range 40 {
					b.apply(t, p, rc)
					result, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{})
					if err != nil {
						t.Fatalf("persisted reform did not recover: step=%s err=%v", result.Step, err)
					}
					if result.Done {
						done = true
						break
					}
					p = New(Options{Provider: "pxc"})
				}
				if !done {
					t.Fatal("restore did not finish from persisted reform")
				}
				backup, _ := loadBackupState(rc.Cluster)
				if backup.RestoreOperation != nil || backup.Generation != generation {
					t.Fatal("successful recovery did not activate exactly the restored generation")
				}
			}
			current, _ := load(rc.Cluster)
			if current.RestoreGTIDFence != "" || validations == 0 || releases == 0 {
				t.Fatal("validating recovery failed to release client fences")
			}
			for name, held := range proxyHeld {
				if held {
					t.Fatalf("proxy remains held: %s", name)
				}
			}
		})
	}
}
