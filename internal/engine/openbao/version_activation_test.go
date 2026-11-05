// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func TestVersionPatchRetainsInstalledBinaryBeforePreparation(t *testing.T) {
	for _, fault := range []string{"seal-after-patch", "reload-before-prepare", "ordinary-pull"} {
		t.Run(fault, func(t *testing.T) {
			p, rc, f, a := setup(t)
			rc.Cluster.Spec.Version = "2.7.1"
			ready(t, p, rc)
			root := t.TempDir()
			install := func(name, version string) {
				t.Helper()
				path := filepath.Join(root, name, "opt/karakoram/openbao-"+version, "bao")
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("offline executable-location fixture"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			s := state(t, rc)
			for _, m := range s.Members {
				install(m.Name, "2.7.1")
				// Replay the released schema: no new installed-version pin.
				m.InstalledVersion = ""
				name := m.Name
				server := a.servers[name]
				next := server.Config.Handler
				applied := m.ConfigVersion
				var applyMu sync.Mutex
				server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					applyMu.Lock()
					defer applyMu.Unlock()
					if r.URL.Path == "/v1/status" || r.URL.Path == "/v1/reload" {
						cfg, err := a.p.MemberConfig(r.Context(), rc, name)
						var spec baospec.MemberSpec
						if err == nil {
							err = json.Unmarshal(cfg.EngineSpec, &spec)
						}
						if err == nil {
							_, err = os.Stat(filepath.Join(root, name, strings.TrimPrefix(spec.BinaryPath, "/")))
						}
						if err != nil {
							t.Errorf("%s consumed unprepared binary %s: %v", name, spec.BinaryPath, err)
							http.Error(w, "unprepared executable", http.StatusBadRequest)
							return
						}
						if r.URL.Path == "/v1/reload" {
							applied = cfg.ConfigVersion
						}
						if r.URL.Path == "/v1/status" {
							// Unlike the general fake, a status query cannot
							// acknowledge configuration it has not applied.
							recorder := httptest.NewRecorder()
							next.ServeHTTP(recorder, r)
							var status agentapi.Status
							if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
								t.Error(err)
								http.Error(w, "invalid status", http.StatusInternalServerError)
								return
							}
							status.ConfigVersion = applied
							_ = json.NewEncoder(w).Encode(status)
							return
						}
					}
					next.ServeHTTP(w, r)
				})
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Version = "2.7.2"
			rc.Cluster.Generation++
			initial := len(f.deployments)
			unseals := a.member("demom0").unseals
			if fault == "seal-after-patch" {
				b := a.member("demom0")
				b.sealed, b.active, b.healthy = true, false, false
			}
			f.done = false
			reloaded := false
			for range 100 {
				p = New(p.options)
				a.p = p
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s = state(t, rc)
				if len(f.deployments) > initial {
					break
				}
				for _, m := range s.Members {
					if fault == "reload-before-prepare" && m.Change != nil && m.Change.Step == "deploy" {
						agent, err := p.agent(t.Context(), rc, m.Name)
						if err != nil {
							t.Fatal(err)
						}
						err = agent.Reload(t.Context())
						agent.Close()
						if err != nil {
							t.Fatal(err)
						}
						reloaded = true
					}
				}
			}
			if len(f.deployments) != initial+1 || fault == "reload-before-prepare" && !reloaded {
				t.Fatal("did not reach first preparation after the requested recovery/reload")
			}
			if fault == "seal-after-patch" && a.member("demom0").unseals != unseals+3 {
				t.Fatal("patch did not recover the sealed incumbent before preparation")
			}
			var target string
			for _, m := range s.Members {
				if m.ResumeDeploy != nil {
					target = m.Name
				}
			}
			if target == "" {
				t.Fatal("missing pending preparation receipt")
			}
			// Model target preparation while its platform receipt is unresolved.
			// Installed files alone do not authorize configuration activation.
			install(target, "2.7.2")
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				for _, m := range state(t, rc).Members {
					cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
					if err != nil || cfg.EngineVersion != "2.7.1" {
						t.Fatalf("unresolved receipt activated %s: %s %v", m.Name, cfg.EngineVersion, err)
					}
				}
			}
			before := state(t, rc).find(target).ConfigVersion
			f.done = true
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s = state(t, rc)
			if s.find(target).InstalledVersion != "2.7.2" || s.find(target).ConfigVersion <= before {
				t.Fatal("completed preparation did not activate a fresh configuration")
			}
			complete := false
			for range 250 {
				p = New(p.options)
				a.p = p
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				for _, m := range state(t, rc).Members {
					if m.ResumeDeploy != nil {
						install(m.Name, "2.7.2")
					}
				}
				if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					complete = true
					break
				}
			}
			if !complete || len(f.deployments) != initial+3 {
				t.Fatalf("rollout failed after recovery: %s", rc.Cluster.Internal)
			}
			for _, m := range state(t, rc).Members {
				if m.InstalledVersion != "2.7.2" || m.Version != "2.7.2" || !m.Healthy || m.Change != nil {
					t.Fatalf("member did not converge: %+v", m)
				}
			}
		})
	}
}

func TestVersionPreparationFailureRetainsInstalledBinary(t *testing.T) {
	for _, failure := range []string{"submission", "receipt"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			rc.Cluster.Spec.Version = "2.7.1"
			ready(t, p, rc)
			rc.Cluster.Spec.Version = "2.7.2"
			s := state(t, rc)
			m := s.find("demom1")
			activateConfig(rc.Cluster, s, m)
			if failure == "submission" {
				f.failures["deploy:"+m.Name] = errors.New("preparation rejected")
			} else {
				f.failed = true
			}
			err := p.deploy(t.Context(), rc, s, m, "2.7.2")
			if failure == "receipt" {
				if err != nil {
					t.Fatal(err)
				}
				_, err = p.observeReceipt(t.Context(), rc, s, m.ResumeDeploy)
			}
			if err == nil {
				t.Fatal("expected preparation failure")
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			p = New(p.options)
			cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil || cfg.EngineVersion != "2.7.1" {
				t.Fatalf("failed preparation activated target: %s %v", cfg.EngineVersion, err)
			}
		})
	}
}

func TestUnknownInstalledVersionDoesNotSelectTarget(t *testing.T) {
	p, rc, s := edgeFixture(t)
	m := s.Members[0]
	m.Version, m.InstalledVersion = "", ""
	m.DesiredVersion = "2.7.2"
	rc.Cluster.Spec.Version = "2.7.2"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := p.MemberConfig(t.Context(), rc, m.Name); err == nil {
		t.Fatal("unknown incumbent installation selected desired target")
	}
}
