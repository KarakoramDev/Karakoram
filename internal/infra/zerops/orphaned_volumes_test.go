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

func TestDeleteServiceByIDFencesReplacementAndProtectedServices(t *testing.T) {
	for _, variant := range []string{"matching", "replacement", "empty-id", "core", "system", "absent"} {
		t.Run(variant, func(t *testing.T) {
			mutations := 0
			service := ServiceStack{ID: "recorded-volume", Name: "demom0v"}
			if variant == "system" {
				service.IsSystem = true
			}
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/service-stack") {
					items := []ServiceStack{service}
					if variant == "absent" {
						items = nil
					}
					_ = json.NewEncoder(w).Encode(page[ServiceStack]{List: items})
					return
				}
				if r.Method == "GET" {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				mutations++
				if r.URL.Path != "/service-stack/recorded-volume" {
					t.Error("deleted wrong service", r.URL.Path)
				}
				reply(t, w, "deploy")
			})
			name, id := service.Name, service.ID
			if variant == "replacement" {
				id = "older-service"
			}
			if variant == "empty-id" {
				id = ""
			}
			if variant == "core" {
				name = "core"
			}
			_, err := b.DeleteServiceByID(context.Background(), name, id)
			allowed := variant == "matching" || variant == "absent"
			if allowed && err != nil || !allowed && err == nil {
				t.Fatal(variant, err)
			}
			want := 0
			if variant == "matching" {
				want = 1
			}
			if mutations != want {
				t.Fatal("unsafe deletion", mutations, want)
			}
		})
	}
}
