// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRoutingReplacesTargetOnSamePublicPort(t *testing.T) {
	for _, target := range []int{5671, 5672} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			oldTarget := 5672
			if target == 5672 {
				oldTarget = 5671
			}
			current := []PublicPortRouting{
				{ID: "obsolete", PublicIPType: "IP_V6", PublicPort: 15671, InternalPort: oldTarget, InternalProtocol: "tcp", FirewallPolicy: "WHITELIST", FirewallIPRanges: []string{"2001:db8::/64"}, IsSynced: true},
				{ID: "unrelated", PublicIPType: "IP_V6", PublicPort: 19443, InternalPort: 9443, InternalProtocol: "tcp", FirewallPolicy: "WHITELIST", FirewallIPRanges: []string{}, IsSynced: true},
			}
			var calls []string
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if serviceHandler(t, w, r) {
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/public-port-routing") && r.Method == "GET":
					_ = json.NewEncoder(w).Encode(page[PublicPortRouting]{Total: len(current), List: current})
				case strings.HasSuffix(r.URL.Path, "/public-port-routing") && r.Method == "POST":
					calls = append(calls, "create")
					var route PublicPortRouting
					if err := json.NewDecoder(r.Body).Decode(&route); err != nil {
						t.Error(err)
					}
					for _, existing := range current {
						if existing.PublicPort == route.PublicPort {
							t.Error("public port reused before obsolete route deletion was synchronized")
							w.WriteHeader(http.StatusConflict)
							return
						}
					}
					route.ID = "replacement"
					current = append(current, route)
					reply(t, w, "route")
				case strings.HasPrefix(r.URL.Path, "/public-port-routing/"):
					id := strings.TrimPrefix(r.URL.Path, "/public-port-routing/")
					calls = append(calls, "delete:"+id)
					for i := range current {
						if current[i].ID == id {
							current[i].DeleteOnSync = true
						}
					}
					reply(t, w, "deleteRoute")
				case strings.HasSuffix(r.URL.Path, "/sync-public-port-routing"):
					calls = append(calls, "sync")
					current = slices.DeleteFunc(current, func(route PublicPortRouting) bool { return route.DeleteOnSync })
					for i := range current {
						current[i].IsSynced = true
					}
					reply(t, w, "deploy")
				case strings.HasPrefix(r.URL.Path, "/process/"):
					reply(t, w, "finished")
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
			wanted := infra.PublicPort{Port: 15671, TargetPort: target, Protocol: "tcp", IPType: "IPV6", Allowlist: []string{"2001:db8::/64"}}
			for range 2 {
				if err := b.EnsurePublicPort(t.Context(), "member", wanted); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(calls, []string{"delete:obsolete", "sync", "create", "sync"}) {
				t.Fatalf("replacement order or idempotency: %v", calls)
			}
			if len(current) != 2 || current[0].ID != "unrelated" || current[1].InternalPort != target || current[1].PublicPort != wanted.Port || !slices.Equal(current[1].FirewallIPRanges, wanted.Allowlist) || !current[1].IsSynced {
				t.Fatalf("wrong route replacement: %+v", current)
			}
		})
	}
}
