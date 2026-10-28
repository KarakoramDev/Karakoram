// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
)

type maintenanceClock struct{ at time.Time }

func (c maintenanceClock) Now() time.Time { return c.at }
func TestMaintenanceValidation(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			path := "/api/v1/clusters/prod"
			body := `{"spec":{"maintenance":{"windows":[{"schedule":"not cron","duration":"1h"}]}}}`
			if method == "POST" {
				path = "/api/v1/clusters"
				body = `{"name":"newcluster","spec":{"provider":"fake","maintenance":{"windows":[{"schedule":"not cron","duration":"1h"}]}}}`
			}
			if method == "PUT" {
				body = `{"spec":{"provider":"fake","maintenance":{"windows":[{"schedule":"not cron","duration":"1h"}]}}}`
			}
			w := call(s, method, path, body, token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "spec.maintenance.windows[0].schedule") {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}

func TestMaintenanceValidationWindowLimit(t *testing.T) {
	s, _, token := testServer(t)
	windows := make([]map[string]string, 17)
	for i := range windows {
		windows[i] = map[string]string{"schedule": "0 2 * * *", "duration": "1h"}
	}
	body, err := json.Marshal(map[string]any{"spec": map[string]any{"maintenance": map[string]any{"windows": windows}}})
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "PATCH", "/api/v1/clusters/prod", string(body), token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "spec.maintenance.windows") {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestMaintenanceAPIStatusAndGenerationOverride(t *testing.T) {
	s, _, token := testServer(t)
	s.Context.Clock = maintenanceClock{at: time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC)}
	w := call(s, "PATCH", "/api/v1/clusters/prod?ignoreMaintenanceWindow=true", `{"spec":{"maintenance":{"timeZone":"UTC","windows":[{"schedule":"0 2 * * *","duration":"1h"}]}}}`, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || c.MaintenanceOverrideGeneration != c.Generation {
		t.Fatal(c, err)
	}
	for _, path := range []string{"/api/v1/clusters/prod", "/api/v1/clusters/prod/status"} {
		w = call(s, "GET", path, "", token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var got map[string]any
		if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if status, ok := got["status"].(map[string]any); ok {
			got = status
		}
		policy, _ := got["maintenance"].(map[string]any)
		if policy == nil || policy["open"] != false || policy["nextWindowStart"] != "2026-01-01T02:00:00Z" {
			t.Fatal(got)
		}
	}
	s.Context.Clock = maintenanceClock{at: time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)}
	w = call(s, "GET", "/api/v1/clusters/prod", "", token)
	if !strings.Contains(w.Body.String(), `"open":true`) || !strings.Contains(w.Body.String(), `"currentWindowEnd":"2026-01-01T03:00:00Z"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	override := c.MaintenanceOverrideGeneration
	w = call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"engineConfig":"changed"}}`, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	c, err = s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil || c.MaintenanceOverrideGeneration != override || c.Generation <= override {
		t.Fatal(c, err)
	}
}
func TestMaintenanceImperativeOverrides(t *testing.T) {
	for _, endpoint := range []struct{ path, body string }{{"restart", ""}, {"rotate-tls", ""}, {"members/prodm0/replace", ""}, {"raise-fcv", `{"version":"8.0"}`}} {
		t.Run(endpoint.path, func(t *testing.T) {
			s, _, token := testServer(t)
			w := call(s, "POST", "/api/v1/clusters/prod/"+endpoint.path+"?ignoreMaintenanceWindow=true", endpoint.body, token)
			if w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			var response struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			op, err := s.Store.GetOperation(t.Context(), response.ID)
			if err != nil || !op.IgnoreMaintenanceWindow {
				t.Fatal(op, err)
			}
		})
	}
}

func TestMaintenanceConditionAfterPolicyClose(t *testing.T) {
	for _, provider := range []engine.Provider{&fakeProvider{}, openbao.New(openbao.ProviderOptions{})} {
		t.Run(provider.Name(), func(t *testing.T) {
			s, _, token := testServer(t)
			var err error
			s.Registry, err = engine.NewRegistry(&fakeProvider{}, openbao.New(openbao.ProviderOptions{}))
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 10, 28, 13, 35, 0, 0, time.UTC)
			s.Context.Clock = maintenanceClock{at: at}
			c := &domain.Cluster{Name: "window"}
			provider.ApplyDefaults(&c.Spec)
			c.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "* * * * *", Duration: "15m"}}}
			c.Status.Conditions = []domain.Condition{{Type: "MaintenanceWindow", Status: "True", Reason: "Open", LastTransitionTime: at.Add(-time.Hour)}}
			if err := s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			assertStatus := func(status api.ClusterStatus, open bool) {
				t.Helper()
				if status.Maintenance == nil || status.Maintenance.Open != open {
					t.Fatalf("maintenance = %+v, want open %v", status.Maintenance, open)
				}
				wantStatus, wantReason := api.ConditionStatus("False"), "Closed"
				if open {
					wantStatus, wantReason = api.ConditionStatus("True"), "Open"
				}
				count := 0
				for _, condition := range status.Conditions {
					if condition.Type == "MaintenanceWindow" {
						count++
						if condition.Status != wantStatus || condition.Reason != wantReason {
							t.Fatalf("condition = %+v, want %s/%s", condition, wantStatus, wantReason)
						}
					}
				}
				if count != 1 {
					t.Fatalf("maintenance condition count = %d", count)
				}
			}
			readCluster := func(open bool) {
				t.Helper()
				w := call(s, "GET", "/api/v1/clusters/window", "", token)
				var got api.Cluster
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil {
					t.Fatal(w.Code, w.Body.String())
				}
				assertStatus(got.Status, open)
			}
			readCluster(true)
			// A policy-only edit needs no disruption and therefore need not call
			// the provider gate again. Its previous Open receipt remains stored.
			w := call(s, "PATCH", "/api/v1/clusters/window", `{"spec":{"maintenance":{"windows":[{"schedule":"0 0 1 1 *","duration":"15m"}]}}}`, token)
			var patched api.Cluster
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &patched) != nil {
				t.Fatal(w.Code, w.Body.String())
			}
			assertStatus(patched.Status, false)
			readCluster(false)
			w = call(s, "GET", "/api/v1/clusters/window/status", "", token)
			var status api.ClusterStatus
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil {
				t.Fatal(w.Code, w.Body.String())
			}
			assertStatus(status, false)
			// Responses must not rewrite the gate receipt used by reconciliation.
			stored, err := s.Store.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status.Conditions[0].Status != "True" {
				t.Fatal("response mutated persisted gate receipt")
			}
			s.Context.Clock = maintenanceClock{at: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)}
			readCluster(true)
		})
	}
}

func TestMaintenanceConditionProjection(t *testing.T) {
	at := time.Date(2026, 10, 28, 13, 35, 0, 0, time.UTC)
	for _, scenario := range []string{"new", "waiting", "removed", "empty"} {
		t.Run(scenario, func(t *testing.T) {
			s := &Server{Options: Options{Context: engine.Context{Clock: maintenanceClock{at: at}}}}
			c := domain.Cluster{Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "15m"}}}}}
			if scenario != "new" {
				c.Status.Conditions = []domain.Condition{
					{Type: "Ready", Status: "True"},
					{Type: "MaintenanceWindow", Status: "False", Reason: "WaitingForWindow", Message: "old schedule", LastTransitionTime: at.Add(-time.Hour)},
				}
			}
			if scenario == "removed" {
				c.Spec.Maintenance = nil
			} else if scenario == "empty" {
				c.Spec.Maintenance.Windows = nil
			}
			original := c.Status.Conditions
			s.maintenanceStatus(&c)
			if scenario == "removed" || scenario == "empty" {
				if c.Status.Maintenance != nil || len(c.Status.Conditions) != 1 || c.Status.Conditions[0].Type != "Ready" {
					t.Fatal(c.Status)
				}
			} else {
				condition := c.Status.Conditions[len(c.Status.Conditions)-1]
				if c.Status.Maintenance.Open || condition.Status != "False" {
					t.Fatal(c.Status)
				}
				if scenario == "waiting" && (condition.Reason != "WaitingForWindow" || !condition.LastTransitionTime.Equal(at.Add(-time.Hour)) || !strings.Contains(condition.Message, "2027-01-01T00:00:00Z")) {
					t.Fatal(condition)
				}
			}
			if len(original) > 0 && original[1].Message != "old schedule" {
				t.Fatal("response mutated source conditions")
			}
		})
	}
}
