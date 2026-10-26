// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
)

func TestClusterMonitoringExplicitBoolean(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	for _, provider := range []string{"postgres", "supabase", "elasticsearch"} {
		for _, method := range []string{"PATCH", "PUT"} {
			t.Run(provider+"/"+method, func(t *testing.T) {
				s, _, token := testServer(t)
				var p engine.Provider
				switch provider {
				case "postgres":
					p = postgres.New(postgres.Options{Store: s.Store})
				case "supabase":
					p = supabase.New(postgres.Options{Store: s.Store})
				default:
					p = elastic.New(elastic.ProviderOptions{Store: s.Store})
				}
				if err := s.Registry.Register(p); err != nil {
					t.Fatal(err)
				}
				c := &domain.Cluster{Name: "scalar"}
				p.ApplyDefaults(&c.Spec)
				if err := s.Store.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				for _, enabled := range []bool{false, true, false} {
					before, err := s.Store.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					body := fmt.Sprintf(`{"spec":{"monitoring":{"enabled":%t}}}`, enabled)
					want := before.Spec
					want.Monitoring.Enabled = enabled
					if method == "PUT" {
						input := specAPI(want)
						if want.Backup.Schedules == nil {
							input.Backup.Schedules = nil
						}
						if want.Exposure.Allowlist == nil {
							input.Exposure.Allowlist = nil
						}
						data, err := json.Marshal(map[string]any{"spec": input})
						if err != nil {
							t.Fatal(err)
						}
						body = string(data)
					}
					w := call(s, method, "/api/v1/clusters/scalar", body, token)
					if w.Code != 200 {
						t.Fatal(w.Code, w.Body.String())
					}
					after, err := s.Store.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(after.Spec, want) || after.Generation != before.Generation+1 {
						t.Fatalf("enabled=%t: monitoring=%t generation=%d, want monitoring=%t generation=%d; spec equal=%t", enabled, after.Spec.Monitoring.Enabled, after.Generation, enabled, before.Generation+1, reflect.DeepEqual(after.Spec, want))
					}
					events := auditEvents(t, s, "ClusterSpecChangeRequested")
					event := events[len(events)-1]
					var details specDetails
					if err = json.Unmarshal(event.Details, &details); err != nil {
						t.Fatal(err)
					}
					if len(details.Changes) != 1 || details.Changes[0].Path != "monitoring.enabled" || details.Changes[0].Before != !enabled || details.Changes[0].After != enabled || !strings.Contains(event.Message, fmt.Sprintf("monitoring.enabled: %t → %t", !enabled, enabled)) {
						t.Fatal(event.Message, string(event.Details))
					}
					w = call(s, "GET", "/api/v1/clusters/scalar", "", token)
					if w.Code != 200 || !strings.Contains(w.Body.String(), fmt.Sprintf(`"monitoring":{"enabled":%t}`, enabled)) {
						t.Fatal(w.Code, w.Body.String())
					}
					w = call(s, "PATCH", "/api/v1/clusters/scalar", `{"spec":{}}`, token)
					preserved, err := s.Store.GetCluster(t.Context(), c.Ref())
					if err != nil || w.Code != 200 || !reflect.DeepEqual(preserved.Spec, after.Spec) || preserved.Generation != after.Generation {
						t.Fatal("omitted monitoring changed", err, w.Code, w.Body.String())
					}
				}
			})
		}
	}
}

func TestClusterCreateMonitoringFalse(t *testing.T) {
	s, _, token := testServer(t)
	if err := s.Registry.Register(postgres.New(postgres.Options{Store: s.Store})); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, monitoring string
		enabled          bool
	}{
		{"disabled", `,"monitoring":{"enabled":false}`, false},
		{"defaulted", "", true},
	} {
		w := call(s, "POST", "/api/v1/clusters", fmt.Sprintf(`{"name":%q,"spec":{"provider":"postgres"%s}}`, tc.name, tc.monitoring), token)
		if w.Code != 201 {
			t.Fatal(w.Code, w.Body.String())
		}
		c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: tc.name})
		if err != nil || c.Spec.Monitoring.Enabled != tc.enabled {
			t.Fatal("monitoring default overrode input", err, w.Body.String())
		}
	}
}

func TestClusterPatchExplicitZeroExtra(t *testing.T) {
	for _, field := range []string{"replicas", "minDisk", "maxDisk"} {
		t.Run(field, func(t *testing.T) {
			s, _, token := testServer(t)
			p := elastic.New(elastic.ProviderOptions{Store: s.Store})
			if err := s.Registry.Register(p); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "scalar", Spec: domain.ClusterSpec{Topology: domain.Topology{Type: "dedicatedMasters"}}}
			p.ApplyDefaults(&c.Spec)
			if err := s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			body := `{"spec":{"components":{"extra":{"master":{"replicas":0}}}}}`
			if field != "replicas" {
				body = fmt.Sprintf(`{"spec":{"components":{"extra":{"master":{"resources":{%q:0}}}}}}`, field)
			}
			w := call(s, "PATCH", "/api/v1/clusters/scalar", body, token)
			if w.Code != 400 {
				t.Fatalf("invalid explicit %s=0 silently defaulted: %d %s", field, w.Code, w.Body.String())
			}
		})
	}
}

func TestClusterPatchOtherFalseAndZeroValues(t *testing.T) {
	for _, tc := range []struct {
		name, patch string
		update      func(*domain.ClusterSpec)
	}{
		{"public", `{"spec":{"exposure":{"public":false}}}`, func(s *domain.ClusterSpec) { s.Exposure.Public = false }},
		{"pitr", `{"spec":{"backup":{"pitr":{"enabled":false}}}}`, func(s *domain.ClusterSpec) { s.Backup.PITR.Enabled = false }},
		{"schedule", `{"spec":{"backup":{"schedules":[{"name":"daily","cron":"0 0 * * *","type":"logical","storage":"archive","retainCount":2,"enabled":false}]}}}`, func(s *domain.ClusterSpec) { s.Backup.Schedules[0].Enabled = false }},
		{"optional replicas", `{"spec":{"components":{"extra":{"optional":{"replicas":0}}}}}`, func(s *domain.ClusterSpec) {
			c := s.Components.Extra["optional"]
			c.Replicas = 0
			s.Components.Extra["optional"] = c
		}},
		{"optional disk", `{"spec":{"components":{"extra":{"optional":{"resources":{"minDisk":0,"maxDisk":0}}}}}}`, func(s *domain.ClusterSpec) {
			c := s.Components.Extra["optional"]
			c.Resources.MinDisk, c.Resources.MaxDisk = 0, 0
			s.Components.Extra["optional"] = c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			storage := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Region: "region", Bucket: "objects"}
			if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
				t.Fatal(err)
			}
			before, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			before.Spec.Exposure.Public = true
			before.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
			before.Spec.Backup.PITR.Enabled = true
			before.Spec.Backup.Storage = "archive"
			before.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Cron: "0 0 * * *", Type: "logical", Storage: "archive", RetainCount: 2, Enabled: true}}
			before.Spec.Components.Extra = map[string]domain.ComponentSpec{"optional": before.Spec.Components.Engine}
			if err = s.Store.UpdateClusterSpec(t.Context(), before.Ref(), before.Spec); err != nil {
				t.Fatal(err)
			}
			before, err = s.Store.GetCluster(t.Context(), before.Ref())
			if err != nil {
				t.Fatal(err)
			}
			// Deep-copy collections before updating the expected state.
			data, err := json.Marshal(before.Spec)
			if err != nil {
				t.Fatal(err)
			}
			var want domain.ClusterSpec
			if err = json.Unmarshal(data, &want); err != nil {
				t.Fatal(err)
			}
			tc.update(&want)
			w := call(s, "PATCH", "/api/v1/clusters/prod", tc.patch, token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			after, err := s.Store.GetCluster(t.Context(), before.Ref())
			if err != nil || !reflect.DeepEqual(after.Spec, want) || after.Generation != before.Generation+1 {
				t.Fatal("explicit scalar lost or absent field changed", err, w.Body.String())
			}
		})
	}
}
