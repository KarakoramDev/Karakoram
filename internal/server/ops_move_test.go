// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type moveTestProvider struct {
	fakeProvider
	name string
}

func (p *moveTestProvider) Name() string { return p.name }

func configureMoveTestProvider(t *testing.T, s *Server, name string) *moveTestProvider {
	t.Helper()
	p := &moveTestProvider{name: name}
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = name
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	c, err = s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	c.Status.ObservedGeneration = c.Generation
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMoveMemberAPI(t *testing.T) {
	for _, authoritative := range []bool{false, true} {
		t.Run(map[bool]string{false: "best-effort", true: "backend"}[authoritative], func(t *testing.T) {
			s, _, token := testServer(t)
			configureMoveTestProvider(t, s, "psmdb")
			capabilities := infra.Capabilities{Placement: authoritative}
			s.namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{Capabilities: capabilities})}
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			c.Status.Members[0].PlacementGroup = "node-original"
			if err = s.Store.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			path := "/api/v1/clusters/prod/members/prodm0/move"
			w := call(s, "POST", path+"?ignoreMaintenanceWindow=true", "", token)
			if w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			op, err := s.Store.GetOperation(t.Context(), w.Header().Get("Karakoram-Operation-Id"))
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Member     string
				Relocate   bool
				AvoidGroup string
			}
			if err = json.Unmarshal(op.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if op.Kind != domain.OperationReplaceMember || payload.Member != "prodm0" || !payload.Relocate || payload.AvoidGroup != "node-original" || !op.IgnoreMaintenanceWindow || op.Target.Kind != "member" {
				t.Fatal(op, payload)
			}
			if !authoritative && (!strings.Contains(op.Message, "no host placement guarantee") || !strings.Contains(w.Body.String(), "no host placement guarantee")) {
				t.Fatal(op.Message, w.Body.String())
			}
		})
	}
}

func TestMoveMemberAdmission(t *testing.T) {
	s, _, token := testServer(t)
	provider := configureMoveTestProvider(t, s, "psmdb")
	capabilities := infra.Capabilities{Placement: true}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{Capabilities: capabilities})}
	path := "/api/v1/clusters/prod/members/prodm0/move"
	for _, group := range []string{"", "unknown", "best-effort:unknown"} {
		c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
		if err != nil {
			t.Fatal(err)
		}
		c.Status.Members[0].PlacementGroup = group
		if err = s.Store.UpdateClusterState(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		w := call(s, "POST", path, "", token)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "placement group is unknown") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/clusters/missing/members/prodm0/move", "/api/v1/clusters/prod/members/missing/move"} {
		if w := call(s, "POST", path, "", token); w.Code != 404 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	info := provider.Describe()
	info.Operations = nil
	provider.info = &info
	if w := call(s, "POST", path, "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(ops) != 0 {
		t.Fatal(ops, err)
	}
}

type restrictedReplacementProvider struct{ fakeProvider }

func (*restrictedReplacementProvider) ValidateMemberReplacement(*domain.Cluster, string) error {
	return engine.Invalid("replacement target must be a data member")
}

func TestMoveAndReplaceShareMemberRefusal(t *testing.T) {
	s, _, token := testServer(t)
	registry, err := engine.NewRegistry(&restrictedReplacementProvider{})
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	for _, action := range []string{"replace", "move"} {
		w := call(s, "POST", "/api/v1/clusters/prod/members/prodm0/"+action, "", token)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "replacement target must be a data member") {
			t.Fatal(action, w.Code, w.Body.String())
		}
	}
}

func TestMoveBestEffortUnknownPlacement(t *testing.T) {
	s, _, token := testServer(t)
	configureMoveTestProvider(t, s, "psmdb")
	s.namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{})}
	w := call(s, "POST", "/api/v1/clusters/prod/members/prodm0/move", "", token)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	op, err := s.Store.GetOperation(t.Context(), w.Header().Get("Karakoram-Operation-Id"))
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Relocate   bool
		AvoidGroup string
	}
	if err = json.Unmarshal(op.Payload, &payload); err != nil || !payload.Relocate || payload.AvoidGroup != "" {
		t.Fatal(payload, err)
	}
}

func TestMoveMemberRejectsIntegratedEnginesWithoutRelocation(t *testing.T) {
	for _, name := range []string{"mysql", "pxc", "neo4j", "weaviate", "keycloak", "openbao", "temporal"} {
		t.Run(name, func(t *testing.T) {
			s, _, token := testServer(t)
			// Advertise replacement to prove it does not imply relocation support.
			configureMoveTestProvider(t, s, name)
			w := call(s, "POST", "/api/v1/clusters/prod/members/prodm0/move", "", token)
			if w.Code != 409 || !strings.Contains(w.Body.String(), "Member moves are not supported for engine "+name) {
				t.Fatal(w.Code, w.Body.String())
			}
			ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil || len(ops) != 0 {
				t.Fatal(ops, err)
			}
			// Placement is advisory and remains readable even without move support.
			w = call(s, "GET", "/api/v1/clusters/prod/placement", "", token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}
