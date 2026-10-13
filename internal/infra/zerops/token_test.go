// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestValidateProjectPermissionsAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, token, project string
		denyList             bool
		wantError            string
	}{{"valid", "valid-token", "proj-test", false, ""}, {"bad-token", "invalid-token", "proj-test", false, "cannot access"}, {"mismatch", "valid-token", "proj-other", false, "does not match"}, {"service-permission", "valid-token", "proj-test", true, "cannot list"}} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer valid-token" {
					http.Error(w, "denied", 401)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/service-stack") {
					if tc.denyList {
						http.Error(w, "denied", 403)
						return
					}
					fmt.Fprint(w, `{"list":[],"total":0}`)
					return
				}
				fmt.Fprintf(w, `{"id":%q,"name":"Test project"}`, tc.project)
			}))
			defer srv.Close()
			b, p, err := ValidateProject(context.Background(), Options{BaseURL: srv.URL, Token: tc.token, ProjectID: "proj-test", MaxRetries: -1})
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || strings.Contains(err.Error(), tc.token) {
					t.Fatalf("validation: %v", err)
				}
				return
			}
			if err != nil || b == nil || p.ID != "proj-test" {
				t.Fatalf("validation: %+v %v", p, err)
			}
		})
	}
}
func TestLiveTokenSwapConcurrentRequests(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("Authorization")]++
		mu.Unlock()
		fmt.Fprint(w, `{"id":"proj-test"}`)
	}))
	defer srv.Close()
	b, err := New(Options{BaseURL: srv.URL, Token: "old-token", ProjectID: "proj-test", MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.ProjectInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 20 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 10 {
				if _, err := b.ProjectInfo(context.Background()); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for range 10 {
		b.SetToken("new-token")
		b.SetToken("old-token")
	}
	b.SetToken("new-token")
	workers.Wait()
	if _, err = b.ProjectInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["Bearer old-token"] == 0 || seen["Bearer new-token"] == 0 {
		t.Fatal("token swap not reflected")
	}
	for value := range seen {
		if value != "Bearer old-token" && value != "Bearer new-token" {
			t.Fatal("partial credential observed")
		}
	}
}
