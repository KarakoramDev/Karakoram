// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestLostVolumeAdmissionEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, evidence, guidance string
		noQuorum                 bool
	}{
		{"sole-replica", `{"collection":"Books","shards":[{"shard":"hot-tenant","replicas":["wvdemo0"]}]}`, "only replica", false},
		{"missing-collection", `{}`, "sharding evidence", false},
		{"wrong-collection", `{"collection":"Other","shards":[]}`, "sharding evidence", false},
		{"missing-shards", `{"collection":"Books"}`, "sharding evidence", false},
		{"missing-replicas", `{"collection":"Books","shards":[{"shard":"cold-tenant"}]}`, "replica evidence", false},
		{"no-quorum", `{"collection":"Books","shards":[]}`, "quorum", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f *coreFixture
			f = coreSetup(t, 3, func(w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/v1/schema":
					w.Write([]byte(`{"classes":[{"class":"Books"}]}`))
					return true
				case "/v1/replication/sharding-state":
					// Substitute fixture names rather than coupling evidence to hostname syntax.
					s, _ := load(f.rc.Cluster)
					raw := strings.ReplaceAll(tc.evidence, "wvdemo0", s.Members[0].Name)
					w.Write([]byte(`{"shardingState":` + raw + `}`))
					return true
				}
				return false
			})
			s := coreConverge(t, f)
			f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":2}`)
			m := s.Members[0]
			m.RuntimeRecovery.VolumeLost = true
			if tc.noQuorum {
				s.Voters = append(s.Voters, "missing-voter-a", "missing-voter-b")
			}
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			before := len(f.backend.Calls())
			_, err := f.p.RunOperation(context.Background(), f.rc, &domain.Operation{ID: "lost", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + m.Name + `"}`)})
			if err == nil || !strings.Contains(err.Error(), tc.guidance) {
				t.Fatalf("expected %s refusal, got %v", tc.guidance, err)
			}
			if !tc.noQuorum && !strings.Contains(err.Error(), "backup") {
				t.Fatal("missing operator recovery guidance", err)
			}
			for _, call := range f.backend.Calls()[before:] {
				if call == "DeleteServiceByID" || call == "ImportServices" || call == "Deploy" {
					t.Fatal("refused replacement mutated runtime", call)
				}
			}
		})
	}
}

func TestLostVolumeReplicatedReplacementRecovery(t *testing.T) {
	ctx := context.Background()
	var f *coreFixture
	replicasBack := false
	f = coreSetup(t, 3, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v1/schema":
			w.Write([]byte(`{"classes":[{"class":"Books"}]}`))
			return true
		case "/v1/replication/sharding-state":
			s, _ := load(f.rc.Cluster)
			json.NewEncoder(w).Encode(map[string]any{"shardingState": map[string]any{"collection": "Books", "shards": []any{map[string]any{"shard": "tenant-hot", "replicas": []string{s.Members[0].Name, s.Members[1].Name}}, map[string]any{"shard": "tenant-cold", "replicas": []string{s.Members[0].Name, s.Members[2].Name}}}}})
			return true
		case "/v1/nodes":
			s, _ := load(f.rc.Cluster)
			nodes := []any{}
			for _, m := range s.Members {
				shards := []any{}
				if replicasBack {
					shards = append(shards, map[string]any{"class": "Books", "name": "tenant-hot", "replicationFactor": 2})
				}
				nodes = append(nodes, map[string]any{"name": m.Name, "status": "HEALTHY", "version": m.DesiredVersion, "shards": shards})
			}
			json.NewEncoder(w).Encode(map[string]any{"nodes": nodes})
			return true
		}
		return false
	})
	coreConverge(t, f)
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":2}`)
	// Activate the RF policy first, so replacement is the only pending disruption.
	s := coreConverge(t, f)
	m := s.Members[0]
	name, oldVolume := m.Name, m.VolumeID
	if _, err := f.backend.DeleteServiceByID(ctx, m.Name, m.ServiceID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID); err != nil {
		t.Fatal(err)
	}
	m.RuntimeRecovery.VolumeLost = true
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "replicated-recovery", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + name + `"}`)}
	done := false
	for range 180 {
		f.p = New(Options{Store: f.rc.Store})
		r, err := f.p.RunOperation(ctx, f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
		s, _ = load(f.rc.Cluster)
		m = s.find(name)
		if m != nil && m.VolumeID != "" && m.VolumeID != oldVolume && m.Stage == "deployed" {
			replicasBack = true
		}
		if r.Done {
			done = true
			break
		}
	}
	if !done || !replicasBack || m.Name != name || m.VolumeID == oldVolume || m.Stage != "ready" {
		t.Fatalf("replicated replacement incomplete: done=%v replicas=%v member=%+v", done, replicasBack, m)
	}
	coreConverge(t, f)
	if f.rc.Cluster.Status.Phase != domain.PhaseReady {
		t.Fatal("cluster not Ready")
	}
}
