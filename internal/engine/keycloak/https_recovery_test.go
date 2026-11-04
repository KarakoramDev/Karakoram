// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

func TestHTTPSAdmissionSurvivesRecovery(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime"} {
		for _, public := range []bool{false, true} {
			for _, change := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/public=%t/change=%t", recovery, public, change), func(t *testing.T) {
					p, rc, bindings, _ := setup(t)
					rc.Backend = idleBackend{bindings.backend}
					rc.Cluster.Generation = 1
					rc.Clock = fixedClock{at: time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)}
					options := EngineOptions{Database: DatabaseOptions{Cluster: "pg"}, Hostname: "https://identity.example.test", ProxyHeaders: "xforwarded", PublicHTTPS: public}
					setOptions := func() {
						t.Helper()
						var err error
						rc.Cluster.Spec.EngineOptions, err = json.Marshal(options)
						if err != nil {
							t.Fatal(err)
						}
					}
					setOptions()
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						s, _ := load(rc.Cluster)
						if r.URL.Path == "/v1/status" {
							json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
							return
						}
						json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					}))
					t.Cleanup(server.Close)
					rc.Agents = idleAgents{server}
					step := func() {
						t.Helper()
						// Recreate the provider and deserialize its durable state on every pass.
						p = New(Options{Bindings: bindings})
						rc.Cluster.Internal = append(json.RawMessage(nil), rc.Cluster.Internal...)
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
					for range 20 {
						step()
					}
					if rc.Cluster.Status.ObservedGeneration != 1 || bindings.backend.public != public {
						t.Fatal("initial exposure did not converge")
					}
					// Exercise migration from the old runtime-only receipt as well.
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					s.ExposureAdmission.Admitted = nil
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					rc.Cluster.Generation = 2
					rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 2 *", Duration: "1h"}}}
					if change {
						options.PublicHTTPS = !public
						setOptions()
					}
					// Persist a deferred admission before losing the predecessor.
					if change {
						step()
					}
					if recovery == "resume" {
						done := false
						for range 20 {
							p = New(Options{Bindings: bindings})
							done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
							if err != nil {
								t.Fatal(err)
							}
							if done {
								break
							}
						}
						if !done {
							t.Fatal("suspension cleanup did not finish")
						}
					} else {
						bindings.backend.service = nil
						bindings.backend.public = false
					}
					for range 30 {
						step()
					}
					s, err = load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if !s.Deployed || s.Pending != nil || s.Public != public || bindings.backend.public != public || s.ExposureAdmission.Admitted == nil || s.ExposureAdmission.Admitted.Public != public {
						t.Fatal("recovery failed to restore only admitted HTTPS policy")
					}
					if change {
						if rc.Cluster.Status.ObservedGeneration != 1 {
							t.Fatal("deferred exposure generation acknowledged")
						}
						if public {
							rc.Cluster.MaintenanceOverrideGeneration = 2
						} else {
							rc.Clock = fixedClock{at: time.Date(2026, time.February, 1, 0, 1, 0, 0, time.UTC)}
						}
						for range 10 {
							step()
						}
						if bindings.backend.public != options.PublicHTTPS {
							t.Fatal("admission did not apply desired exposure")
						}
					}
					if rc.Cluster.Status.ObservedGeneration != 2 {
						t.Fatal("recovered admitted generation not acknowledged")
					}
				})
			}
		}
	}
}

func TestApplicationAdmissionSurvivesSuspension(t *testing.T) {
	for _, setting := range []string{"hostname", "proxy-headers"} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/legacy=%t", setting, legacy), func(t *testing.T) {
				p, rc, bindings, _ := setup(t)
				rc.Backend = idleBackend{bindings.backend}
				rc.Cluster.Generation = 1
				rc.Clock = fixedClock{at: time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)}
				options := EngineOptions{Database: DatabaseOptions{Cluster: "pg"}, Hostname: "https://identity.example.test", ProxyHeaders: "xforwarded"}
				setOptions := func() {
					t.Helper()
					var err error
					rc.Cluster.Spec.EngineOptions, err = json.Marshal(options)
					if err != nil {
						t.Fatal(err)
					}
				}
				setOptions()
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s, _ := load(rc.Cluster)
					if r.URL.Path == "/v1/status" {
						json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
						return
					}
					json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				}))
				t.Cleanup(server.Close)
				rc.Agents = idleAgents{server}
				step := func() {
					t.Helper()
					p = New(Options{Bindings: bindings})
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				for range 20 {
					step()
				}
				prior := applicationSettings{Hostname: options.Hostname, ProxyHeaders: options.ProxyHeaders}
				if rc.Cluster.Status.ObservedGeneration != 1 {
					t.Fatal("initial application settings did not converge")
				}
				if legacy {
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					s.Application = nil
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
				}
				rc.Cluster.Generation = 2
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 2 *", Duration: "1h"}}}
				if setting == "hostname" {
					options.Hostname = "https://new-identity.example.test"
				} else {
					options.ProxyHeaders = "forwarded"
				}
				setOptions()
				// Legacy cleanup must seed from the retained member config even
				// without an intervening Sync after the desired spec changed.
				if !legacy {
					step()
				}
				stopped := false
				for range 20 {
					done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
					if err != nil {
						t.Fatal(err)
					}
					if done {
						stopped = true
						break
					}
				}
				if !stopped {
					t.Fatal("suspension did not finish")
				}
				for range 30 {
					step()
				}
				assertSettings := func(want applicationSettings) {
					t.Helper()
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					config, err := p.MemberConfig(t.Context(), rc, s.Member)
					if err != nil {
						t.Fatal(err)
					}
					var spec kcspec.Spec
					if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
						t.Fatal(err)
					}
					if spec.Hostname != want.Hostname || spec.ProxyHeaders != want.ProxyHeaders || s.Application == nil || *s.Application != want {
						t.Fatal("runtime received unadmitted application settings", spec.Hostname, spec.ProxyHeaders)
					}
				}
				assertSettings(prior)
				if rc.Cluster.Status.ObservedGeneration != 1 {
					t.Fatal("suspension admitted the deferred application generation")
				}
				rc.Cluster.MaintenanceOverrideGeneration = 2
				for range 10 {
					step()
				}
				assertSettings(applicationSettings{Hostname: options.Hostname, ProxyHeaders: options.ProxyHeaders})
				if rc.Cluster.Status.ObservedGeneration != 2 {
					t.Fatal("override did not apply and acknowledge application settings")
				}
			})
		}
	}
}

func TestApplicationAdmissionMissingLegacySettingsRequiresAdmission(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Generation = 2
	rc.Cluster.Status.ObservedGeneration = 1
	rc.Clock = fixedClock{at: time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 2 *", Duration: "1h"}}}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	desired := EngineOptions{Hostname: "https://identity.example.test", ProxyHeaders: "xforwarded"}
	if err := p.seedApplication(t.Context(), rc, s, desired); err == nil || s.Application != nil {
		t.Fatal("unknown legacy settings were admitted outside a window")
	}
	rc.Cluster.MaintenanceOverrideGeneration = 2
	if err := p.seedApplication(t.Context(), rc, s, desired); err != nil || s.Application == nil || s.Application.Hostname != desired.Hostname || s.Application.ProxyHeaders != desired.ProxyHeaders {
		t.Fatal("explicit override did not admit unavailable legacy settings", err)
	}
}

func TestLegacyMissingConfigRetainsAdmissionPath(t *testing.T) {
	for _, release := range []string{"window", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, bindings, secrets := setup(t)
			rc.Backend = idleBackend{bindings.backend}
			rc.Cluster.Generation = 1
			rc.Clock = fixedClock{at: time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)}
			options := EngineOptions{Database: DatabaseOptions{Cluster: "pg"}, Hostname: "https://identity.example.test", ProxyHeaders: "xforwarded"}
			setOptions := func() {
				t.Helper()
				var err error
				rc.Cluster.Spec.EngineOptions, err = json.Marshal(options)
				if err != nil {
					t.Fatal(err)
				}
			}
			setOptions()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s, _ := load(rc.Cluster)
				if r.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
					return
				}
				json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			}))
			t.Cleanup(server.Close)
			rc.Agents = idleAgents{server}
			step := func() error {
				p = New(Options{Bindings: bindings})
				_, err := p.Sync(t.Context(), rc)
				return err
			}
			for range 20 {
				if err := step(); err != nil {
					t.Fatal(err)
				}
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.ConfigHash == "" || rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("initial configuration did not converge")
			}
			legacyHash := s.ConfigHash
			rc.Cluster.Generation = 2
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 2 *", Duration: "1h"}}}
			options.Hostname = "https://pending.example.test"
			setOptions()
			// Interrupted legacy cleanup deleted the config but retained hash.
			delete(secrets.values, "keycloak/config/"+s.Member)
			s.Application = nil
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			stopped := false
			for range 20 {
				done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
				if err != nil {
					t.Fatal("already-deleted config blocked suspension", err)
				}
				if done {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatal("interrupted legacy cleanup did not finish")
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			// Completed older cleanup also left exactly this stale hash.
			s.ConfigHash, s.Application = legacyHash, nil
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := step(); err == nil || !strings.Contains(err.Error(), "admitted hostname/proxy settings are unavailable") {
				t.Fatal("missing legacy config did not retain a gated admission path", err)
			}
			if rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("unknown legacy settings were acknowledged")
			}
			if release == "window" {
				rc.Clock = fixedClock{at: time.Date(2026, time.February, 1, 0, 1, 0, 0, time.UTC)}
			} else {
				rc.Cluster.MaintenanceOverrideGeneration = 2
			}
			for range 30 {
				if err := step(); err != nil {
					t.Fatal("admission failed to release legacy resume", err)
				}
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.Application == nil || s.Application.Hostname != options.Hostname || rc.Cluster.Status.ObservedGeneration != 2 {
				t.Fatal("legacy resume did not apply and acknowledge admitted settings")
			}
		})
	}
}

type failedApplicationSecrets struct {
	*memorySecrets
	err error
}

func (s failedApplicationSecrets) GetClusterSecret(context.Context, domain.ClusterRef, string) ([]byte, error) {
	return nil, s.err
}

func TestApplicationMigrationPreservesStoreAndDecodeErrors(t *testing.T) {
	for _, failure := range []string{"store", "member-json", "engine-json"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, _, secrets := setup(t)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.ConfigHash = "legacy-applied"
			s.Application = nil
			storeError := errors.New("secret store unavailable")
			switch failure {
			case "store":
				rc.Secrets = failedApplicationSecrets{secrets, storeError}
			case "member-json":
				secrets.values["keycloak/config/"+s.Member] = []byte("{")
			case "engine-json":
				secrets.values["keycloak/config/"+s.Member] = []byte(`{"engineSpec":{"hostname":123}}`)
			}
			err = p.seedApplication(t.Context(), rc, s, EngineOptions{})
			if err == nil || s.Application != nil || failure == "store" && !errors.Is(err, storeError) {
				t.Fatal("real store/decode failure was treated as unknown admission", err)
			}
		})
	}
}
