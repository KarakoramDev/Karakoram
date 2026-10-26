// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

func TestElasticClonePatchAfterSourceDeletion(t *testing.T) {
	s, _, token := testServer(t)
	p := elastic.New(elastic.ProviderOptions{Store: s.Store})
	if err := s.Registry.Register(p); err != nil {
		t.Fatal(err)
	}
	source := &domain.Cluster{Name: "source"}
	p.ApplyDefaults(&source.Spec)
	if err := s.Store.CreateCluster(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Cluster: source.Name, Name: "snapshot", Type: "snapshot", Provider: "elasticsearch", SourceVersion: source.Spec.Version, Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	body := `{"name":"copied","spec":{"provider":"elasticsearch","dataSource":{"cluster":"source","backup":"snapshot"}}}`
	if w := call(s, "POST", "/api/v1/clusters", body, token); w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	completed, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "copied"})
	if err != nil {
		t.Fatal(err)
	}
	completed.Internal = []byte(`{"elasticsearch":{"initialRestore":"done"}}`)
	if err := s.Store.UpdateClusterState(t.Context(), completed); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteBackup(t.Context(), source.Ref(), backup.Name); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "PATCH", "/api/v1/clusters/copied", `{"spec":{"monitoring":{"enabled":false}}}`, token); w.Code != 200 {
		t.Fatalf("patch after snapshot deletion: %d %s", w.Code, w.Body.String())
	}
	if err := s.Store.DeleteCluster(t.Context(), source.Ref()); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "PATCH", "/api/v1/clusters/copied", `{"spec":{"components":{"engine":{"replicas":5}}}}`, token); w.Code != 200 {
		t.Fatalf("patch after source cluster deletion: %d %s", w.Code, w.Body.String())
	}
	if w := call(s, "PATCH", "/api/v1/clusters/copied", `{"spec":{"dataSource":{"backup":"other"}}}`, token); w.Code != 400 || !strings.Contains(w.Body.String(), "immutable") {
		t.Fatalf("changed source accepted: %d %s", w.Code, w.Body.String())
	}
	// Creation still resolves the source and identifies its namespace, cluster and snapshot.
	if w := call(s, "POST", "/api/v1/clusters", strings.Replace(body, "copied", "another", 1), token); w.Code != 404 || !strings.Contains(w.Body.String(), `"namespace":"default"`) || !strings.Contains(w.Body.String(), `"cluster":"source"`) || !strings.Contains(w.Body.String(), `"name":"snapshot"`) {
		t.Fatalf("missing creation source diagnostic: %d %s", w.Code, w.Body.String())
	}
}
