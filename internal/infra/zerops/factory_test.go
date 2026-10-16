// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestFactoryInfoAndEnvironment(t *testing.T) {
	f := NewFactory(Options{})
	info := f.Info()
	if info.Kind != "zerops" || info.DisplayName != "Zerops" || info.Experimental || !info.Relay || info.Capabilities != zeropsCapabilities || info.ConfigKeys != nil {
		t.Fatalf("factory info: %+v", info)
	}
	for _, tc := range []struct {
		name string
		env  map[string]string
		id   string
	}{
		{"defaults", map[string]string{"KARAKORAM_ZEROPS_TOKEN": "token", "projectId": "fallback"}, "fallback"},
		{"explicit", map[string]string{"KARAKORAM_ZEROPS_TOKEN": "token", "KARAKORAM_ZEROPS_PROJECT_ID": "explicit", "projectId": "fallback", "KARAKORAM_ZEROPS_API_URL": "https://api.example.com"}, "explicit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, err := f.TargetFromEnv(func(key string) string { return tc.env[key] })
			if err != nil || target.Kind != "zerops" || target.ID != tc.id || target.Token != "token" || target.Config != nil {
				t.Fatalf("target: %+v %v", target, err)
			}
		})
	}
}

func TestFactoryConstructionAndValidation(t *testing.T) {
	server := newFakeAPI(t)
	f := NewFactory(Options{BaseURL: server.URL, Token: "unused", ProjectID: "unused", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
	target := infra.Target{Kind: "zerops", ID: "project", Token: "token", Config: map[string]string{"apiUrl": "https://unused.example.com"}}
	b, err := f.New(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := b.(infra.CredentialRotator); !ok {
		t.Fatal("credential rotation missing")
	}
	p, err := b.ProjectInfo(t.Context())
	if err != nil || p.ID != target.ID {
		t.Fatalf("constructed backend: %+v %v", p, err)
	}
	valid, p, err := f.Validate(t.Context(), target)
	if err != nil || valid == nil || p.ID != target.ID {
		t.Fatalf("validated backend: %+v %v", p, err)
	}
	target.Token = "wrong"
	invalid, _, err := f.Validate(t.Context(), target)
	if invalid != nil || !errors.Is(err, infra.ErrUnauthorized) || strings.Contains(infra.SafeDetail(err), target.Token) {
		t.Fatalf("invalid credential: %v", err)
	}
}

func TestFactoryProjectMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Project{ID: "different-project", Name: "Other project"})
	}))
	defer server.Close()
	b, _, err := NewFactory(Options{BaseURL: server.URL, MaxRetries: -1}).Validate(t.Context(), infra.Target{Kind: "zerops", ID: "requested-project", Token: "secret-token"})
	if b != nil || !errors.Is(err, infra.ErrUnauthorized) {
		t.Fatalf("project mismatch: %v", err)
	}
	detail := infra.SafeDetail(err)
	if !strings.Contains(detail, "does not match") || strings.Contains(detail, "different-project") || strings.Contains(detail, "requested-project") || strings.Contains(detail, "secret-token") || len(detail) > 160 {
		t.Fatalf("unsafe mismatch detail: %s", detail)
	}
}
