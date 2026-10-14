// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
)

type preflightProvider struct {
	fakeProvider
	windows      []domain.TimeWindow
	initializing bool
	rolling      bool
	rotated      time.Time
}

func (p *preflightProvider) ValidatePITRRestore(_ context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, error) {
	if p.initializing || len(p.windows) == 0 {
		return nil, domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict, "source.pitrTime cannot be restored until initialization and PITR coverage are ready")
	}
	return p.windows, pbm.ValidatePITRTarget(p.windows, target, time.Now())
}
func (p *preflightProvider) PITRWindow(_ context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	if p.initializing {
		return nil, domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict, "PITR coverage is not ready while cluster is initializing")
	}
	return p.windows, nil
}
func (p *preflightProvider) ValidateFCV(_ context.Context, rc *engine.Context, version string) error {
	if version != "8.0" {
		return engine.InvalidField("version", domain.ReasonInvalid, "version must be deployed major form 8.0, not a package version")
	}
	if p.rolling {
		return domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict, "version promotion requires a completed binary rollout")
	}
	return nil
}
func (*preflightProvider) ValidateDatabasePassword(password string) error {
	return users.ValidatePassword(password)
}
func (p *preflightProvider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	return []domain.SystemUser{{Name: "clusterMonitor", LastRotatedAt: &p.rotated}}, nil
}

func TestRestorePreflightRejectsOutOfWindowBeforeEnqueue(t *testing.T) {
	s, _, token := testServer(t)
	start, end := time.Now().UTC().Add(-time.Hour).Truncate(time.Second), time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	p := &preflightProvider{windows: []domain.TimeWindow{{Start: start, End: end}}}
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []time.Time{time.Now().Add(time.Hour), start.Add(-time.Second), end.Add(time.Second)} {
		body, _ := json.Marshal(map[string]any{"name": "recovery", "source": map[string]any{"sourceCluster": "prod", "pitrTime": target}})
		w := call(s, "POST", "/api/v1/clusters/prod/restores", string(body), token)
		var got struct {
			Code    string
			Details map[string]any
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if w.Code != 400 || got.Code != "invalid_request" || got.Details["field"] != "source.pitrTime" || got.Details["reason"] != "out_of_range" || got.Details["earliest"] != start.Format(time.RFC3339) || got.Details["latest"] != end.Format(time.RFC3339) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	operations, err := s.Store.ListOperations(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(operations) != 0 {
		t.Fatal("rejected restore enqueued", operations, err)
	}
	restores, err := s.Store.ListRestores(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(restores) != 0 {
		t.Fatal("rejected restore persisted", restores, err)
	}
	p.initializing = true
	for _, path := range []string{"/api/v1/clusters/prod/pitr", "/api/v1/clusters/prod/restores"} {
		method, body := "GET", ""
		if strings.HasSuffix(path, "restores") {
			method, body = "POST", `{"name":"recovery","source":{"sourceCluster":"prod","pitrTime":"2025-01-01T00:00:00Z"}}`
		}
		w := call(s, method, path, body, token)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "initializ") || !strings.Contains(w.Body.String(), "invalid_state") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	p.initializing = false
	target := start.Add(time.Minute)
	body, _ := json.Marshal(map[string]any{"name": "recovery", "source": map[string]any{"sourceCluster": "prod", "pitrTime": target}})
	if w := call(s, "POST", "/api/v1/clusters/prod/restores", string(body), token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestFCVPreflightRejectsPackageAndRollingVersions(t *testing.T) {
	s, _, token := testServer(t)
	p := &preflightProvider{}
	s.Registry, _ = engine.NewRegistry(p)
	for _, version := range []string{"8.0.32-14", "7.0"} {
		w := call(s, "POST", "/api/v1/clusters/prod/raise-fcv", `{"version":"`+version+`"}`, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"version"`) || !strings.Contains(w.Body.String(), "8.0") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	p.rolling = true
	if w := call(s, "POST", "/api/v1/clusters/prod/raise-fcv", `{"version":"8.0"}`, token); w.Code != 409 || !strings.Contains(w.Body.String(), "invalid_state") {
		t.Fatal(w.Code, w.Body.String())
	}
	operations, err := s.Store.ListOperations(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(operations) != 0 {
		t.Fatal("rejected FCV enqueued", operations, err)
	}
}

func TestDatabasePasswordValidationAndRotationMetadata(t *testing.T) {
	s, _, token := testServer(t)
	p := &preflightProvider{rotated: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	s.Registry, _ = engine.NewRegistry(p)
	for _, password := range []string{"short", "long-enough-🔐", ""} {
		for _, method := range []string{"POST", "PUT"} {
			body, _ := json.Marshal(map[string]any{"username": "app", "database": "app", "password": password, "roles": []any{}})
			path := "/api/v1/clusters/prod/database-users"
			if method == "PUT" {
				path += "/app/app"
				body, _ = json.Marshal(map[string]any{"password": password, "roles": []any{}})
			}
			w := call(s, method, path, string(body), token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"password"`) {
				t.Fatal(w.Code, w.Body.String())
			}
		}
	}
	if w := call(s, "POST", "/api/v1/clusters/prod/database-users", `{"username":"app","database":"app","roles":[]}`, token); w.Code != 201 || !strings.Contains(w.Body.String(), "generatedPassword") {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call(s, "GET", "/api/v1/clusters/prod/system-users", "", token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"lastRotatedAt":"2026-01-02T03:04:05Z"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	var field *engine.ValidationError
	if !errors.As(p.ValidateDatabasePassword("short"), &field) {
		t.Fatal("validator did not preserve field error")
	}
}
