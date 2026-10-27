// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate"
)

func TestProviderDiskMinimumAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider engine.Provider
		options  string
	}{
		{"keycloak", keycloak.New(keycloak.Options{}), `,"engineOptions":{"database":{"cluster":"appdb"}}`},
		{"neo4j", neo4j.New(neo4j.Options{}), ""},
		{"weaviate", weaviate.New(weaviate.Options{}), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			var err error
			s.Registry, err = engine.NewRegistry(tc.provider)
			if err != nil {
				t.Fatal(err)
			}
			spec := `{"provider":"` + tc.name + `","components":{"engine":{"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":2,"maxRam":2,"minDisk":1,"maxDisk":1}}}` + tc.options + `}`
			create := `{"name":"small","spec":` + spec + `}`
			for _, prefix := range []string{"/api/v1/dry-run/clusters", "/api/v1/clusters"} {
				want := 200
				if prefix == "/api/v1/clusters" {
					want = 201
				}
				w := call(s, "POST", prefix, create, token)
				if w.Code != want {
					t.Fatalf("POST %s: %d %s", prefix, w.Code, w.Body.String())
				}
			}
			// A full replacement copied from returned defaults must not encounter
			// a stricter disk floor than the provider's accepted explicit input.
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "small"})
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(map[string]any{"spec": specAPI(c.Spec)})
			if err != nil {
				t.Fatal(err)
			}
			for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
				for _, method := range []string{"PUT", "PATCH"} {
					request := string(body)
					if method == "PATCH" {
						request = `{"spec":{"components":{"engine":{"resources":{"minDisk":1,"maxDisk":1}}}}}`
					}
					w := call(s, method, prefix+"small", request, token)
					if w.Code != 200 {
						t.Fatalf("%s %s: %d %s", method, prefix, w.Code, w.Body.String())
					}
				}
			}
		})
	}
}

func TestPostgresDataDiskMinimumPreserved(t *testing.T) {
	s, _, token := testServer(t)
	p := postgres.New(postgres.Options{Store: s.Store})
	if err := s.Registry.Register(p); err != nil {
		t.Fatal(err)
	}
	spec := `{"provider":"postgres","components":{"engine":{"resources":{"minDisk":1,"maxDisk":1}}}}`
	for _, prefix := range []string{"/api/v1/dry-run/clusters", "/api/v1/clusters"} {
		w := call(s, "POST", prefix, `{"name":"smallpg","spec":`+spec+`}`, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "data members require at least 1 GiB RAM and 5 GB disk") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := call(s, "POST", "/api/v1/clusters", `{"name":"smallpg","spec":{"provider":"postgres"}}`, token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
		for _, method := range []string{"PUT", "PATCH"} {
			w := call(s, method, prefix+"smallpg", `{"spec":`+spec+`}`, token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "data members require at least 1 GiB RAM and 5 GB disk") {
				t.Fatal(w.Code, w.Body.String())
			}
		}
	}
}
