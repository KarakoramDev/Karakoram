// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestOrphanedVolumesFilteredByClusterRead(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	at := time.Date(2024, 1, 7, 9, 0, 0, 0, time.UTC)
	// Neither originating cluster exists: authorization must use the recorded
	// name without requiring a live cluster resource.
	volumes := []domain.OrphanedVolume{
		{Hostname: "am0v", ServiceID: "service-a", Cluster: "cluster-a", Member: "am0", FirstError: "first mounted error", LastError: "last mounted error", FirstFailedAt: at, OrphanedAt: at.Add(30 * time.Minute), LastAttemptAt: at.Add(32 * time.Minute), NextAttemptAt: at.Add(36 * time.Minute), Attempts: 6, LastProcessID: "process-a"},
		{Hostname: "bm0v", ServiceID: "service-b", Cluster: "cluster-b", Member: "bm0", FirstError: "mounted", LastError: "mounted", FirstFailedAt: at, OrphanedAt: at.Add(time.Hour), LastAttemptAt: at.Add(time.Hour), NextAttemptAt: at.Add(time.Hour + 2*time.Minute), Attempts: 5, LastProcessID: "process-b"},
	}
	for i := range volumes {
		if err := s.Store.PutOrphanedVolume(ctx, &volumes[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"orphan-reader", "orphan-no-access"} {
		roles := []string{}
		if name == "orphan-reader" {
			roles = []string{"role:orphan-reader"}
		}
		if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: name, Roles: roles}, "long-test-password"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, role:orphan-reader, clusters, read, cluster-a, allow\n"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := s.Auth.CreateToken(ctx, "orphan-reader", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	noAccess, _, err := s.Auth.CreateToken(ctx, "orphan-no-access", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		token string
		want  []domain.OrphanedVolume
	}{
		{name: "admin sees all", token: admin, want: volumes},
		{name: "cluster reader sees only A", token: reader, want: volumes[:1]},
		{name: "no cluster grants sees empty list", token: noAccess, want: []domain.OrphanedVolume{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := call(s, "GET", "/api/v1/orphaned-volumes", "", tt.token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var got struct {
				Items []domain.OrphanedVolume `json:"items"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Items, tt.want) {
				t.Fatalf("items = %+v; want %+v", got.Items, tt.want)
			}
		})
	}
	w := call(s, "GET", "/api/v1/orphaned-volumes", "", "")
	if w.Code != 401 || strings.Contains(w.Body.String(), "service-a") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOrphanedVolumesEmptyList(t *testing.T) {
	s, _, admin := testServer(t)
	w := call(s, "GET", "/api/v1/orphaned-volumes", "", admin)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"items":[]}` {
		t.Fatal(w.Code, w.Body.String())
	}
	var list api.OrphanedVolumeList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || list.Items == nil || len(list.Items) != 0 {
		t.Fatalf("list = %+v; err = %v", list, err)
	}
}
