// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package gateway

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestParseHostCounts(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       map[string]int
		invalid    bool
	}{
		{"resolved-and-unresolved", `{"cluster_statuses":[{"name":"auth","host_statuses":[{"address":{"socket_address":{"address":"10.0.0.1","port_value":9999}}}]},{"name":"studio"},{"name":"rest","host_statuses":[]},{"name":"storage","host_statuses":[{},{}]}]}`, map[string]int{"auth": 1, "studio": 0, "rest": 0, "storage": 2}, false},
		{"empty", `{}`, map[string]int{}, false},
		{"empty-array", `{"cluster_statuses":[]}`, map[string]int{}, false},
		{"malformed", `{"cluster_statuses":[`, nil, true},
		{"wrong-shape", `{"cluster_statuses":{}}`, nil, true},
		{"upstream-body-never-leaks", `private response containing credentials`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseHostCounts(strings.NewReader(tc.body))
			if tc.invalid {
				if err == nil || strings.Contains(err.Error(), tc.body) {
					t.Fatal("invalid status must fail without exposing its body", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("hosts = %v, err = %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestReadHostCounts(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.URL.RequestURI() != "/clusters?format=json" {
					t.Errorf("unexpected admin request %s %s", req.Method, req.URL.RequestURI())
				}
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"cluster_statuses":[{"name":"studio","host_statuses":[{}]}]}`))
			}))
			defer server.Close()
			hosts, err := hostCounts(t.Context(), server.URL+"/clusters?format=json")
			if code == http.StatusOK {
				if err != nil || hosts["studio"] != 1 {
					t.Fatal("resolved hosts unavailable", hosts, err)
				}
			} else if err == nil || hosts != nil {
				t.Fatal("non-200 admin response accepted", hosts, err)
			}
		})
	}
}
