// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestAccessRoutesToEligibleWriter(t *testing.T) {
	for _, mode := range []string{"pxc", "group", "single"} {
		t.Run(mode, func(t *testing.T) {
			topology := mode
			if mode == "pxc" {
				topology = "group"
			}
			p, rc, b := setup(t, topology)
			if mode == "pxc" {
				p, rc, b = setupPXC(t)
			}
			converge(t, p, rc, b)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			writer := s.data()[len(s.data())-1].Name
			calls := 0
			installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/mysql/status":
					st := myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "SECONDARY", ReadOnly: true}
					if mode == "pxc" {
						st.MemberState, st.MemberRole, st.ReadOnly = "Synced", "PRIMARY", false
					}
					if name == writer {
						st.MemberRole, st.ReadOnly = "PRIMARY", false
					}
					json.NewEncoder(w).Encode(st)
				case "/v1/mysql/galera-status":
					state := "Synced"
					if name == s.data()[0].Name {
						state = "Donor/Desynced"
					}
					if name == s.data()[1].Name {
						state = "Joining"
					}
					json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: state, Ready: true, Connected: true, ClusterSize: 3})
				case "/v1/mysql/proxy-status":
					json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: myspec.ProxyWriterGroup, Host: writer, Status: "ONLINE"}}})
				case "/v1/mysql/" + myspec.ActionAccessDatabases, "/v1/mysql/" + myspec.ActionAccessUsers, "/v1/mysql/" + myspec.ActionAccessGrants:
					if name != writer {
						t.Errorf("access routed to %s, want %s", name, writer)
					}
					var req myspec.AccessRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.Operation != "create" {
						t.Error("lost create operation")
					}
					calls++
					json.NewEncoder(w).Encode(struct{}{})
				default:
					t.Error("unexpected route", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			for _, action := range []string{myspec.ActionAccessDatabases, myspec.ActionAccessUsers, myspec.ActionAccessGrants} {
				if err := p.accessAction(t.Context(), rc, action, myspec.AccessRequest{Operation: "create"}, &struct{}{}); err != nil {
					t.Fatal(action, err)
				}
			}
			if calls != 3 {
				t.Fatal("missing access calls", calls)
			}
		})
	}
}
