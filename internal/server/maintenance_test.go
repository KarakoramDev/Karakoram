// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
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
