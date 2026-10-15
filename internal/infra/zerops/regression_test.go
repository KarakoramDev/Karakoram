// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestPublicPortInventoryIncludesOtherServicesAndPendingRoutes(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/service-stack"):
			_ = json.NewEncoder(w).Encode(page[ServiceStack]{List: []ServiceStack{{ID: "ours", Name: "member"}, {ID: "other", Name: "foreign"}}, Total: 2})
		case r.URL.Path == "/service-stack/ours/public-port-routing":
			_ = json.NewEncoder(w).Encode(page[PublicPortRouting]{List: []PublicPortRouting{{PublicPort: 27017, InternalPort: 27017, InternalProtocol: "tcp", PublicIPType: "IPV6"}}, Total: 1})
		case r.URL.Path == "/service-stack/other/public-port-routing":
			_ = json.NewEncoder(w).Encode(page[PublicPortRouting]{List: []PublicPortRouting{{PublicPort: 27018, InternalPort: 80, InternalProtocol: "tcp", DeleteOnSync: true}}, Total: 1})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.Error(w, "unexpected", 404)
		}
	})
	ports, err := b.ListPublicPorts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ports) != 2 || ports[0].Hostname != "member" || ports[0].Port != 27017 || ports[1].Hostname != "foreign" || ports[1].Port != 27018 {
		t.Fatalf("project routes %+v", ports)
	}
}

func TestObservedOwnershipReturnsOnlyManagedIdentityKeys(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/env") {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{{Key: "KARAKORAM_OWNER", Content: "marker"}, {Key: "KARAKORAM_CLUSTER", Content: "demo"}, {Key: "KARAKORAM_MEMBER", Content: "member"}, {Key: "KARAKORAM_AGENT_TOKEN", Content: "owned-token", Sensitive: true}, {Key: "KARAKORAM_DATA_DIR", Content: "/mnt/membervolume"}, {Key: "UNRELATED_PASSWORD", Content: "must-not-return"}, {Key: "UNRELATED_MASKED", Content: "REDACTED", Sensitive: true}}})
			return
		}
		if serviceHandler(t, w, r) {
			return
		}
		t.Errorf("unexpected request %s", r.URL.Path)
		http.Error(w, "unexpected", 404)
	})
	service, err := b.GetService(context.Background(), "member")
	if err != nil {
		t.Fatal(err)
	}
	if service == nil || service.Masked || len(service.Ownership) != 5 || service.Ownership["KARAKORAM_OWNER"] != "marker" || service.Ownership["KARAKORAM_CLUSTER"] != "demo" || service.Ownership["KARAKORAM_AGENT_TOKEN"] != "owned-token" || service.Ownership["KARAKORAM_DATA_DIR"] != "/mnt/membervolume" {
		t.Fatal("ownership not observed")
	}
	raw, _ := json.Marshal(service)
	if strings.Contains(string(raw), "owned-token") || strings.Contains(string(raw), "must-not-return") {
		t.Fatal("ownership secret serialized")
	}
}

func TestObservedOwnershipOmitsMaskedSensitiveValues(t *testing.T) {
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/env") {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{
				{Key: "KARAKORAM_OWNER", Content: "readable-marker"},
				{Key: "KARAKORAM_CLUSTER", Content: "REDACTED", Sensitive: true},
				{Key: "KARAKORAM_MEMBER", Content: "REDACTED", Sensitive: true},
				{Key: "KARAKORAM_AGENT_TOKEN", Content: "REDACTED", Sensitive: true},
				{Key: "KARAKORAM_DATA_DIR", Content: "REDACTED", Sensitive: true},
				{Key: "UNRELATED_PASSWORD", Content: "must-not-return", Sensitive: true},
			}})
			return
		}
		if !serviceHandler(t, w, r) {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	})
	service, err := b.GetService(t.Context(), "member")
	if err != nil {
		t.Fatal(err)
	}
	if service == nil || !service.Masked || len(service.Ownership) != 1 || service.Ownership["KARAKORAM_OWNER"] != "readable-marker" {
		t.Fatalf("masked ownership: %+v", service)
	}
}
