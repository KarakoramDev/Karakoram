// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

type retirementDialer struct {
	public   engine.ContextDialer
	internal string
}

func (d retirementDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if strings.HasSuffix(address, ":7947") {
		return (&net.Dialer{}).DialContext(ctx, network, d.internal)
	}
	return d.public.DialContext(ctx, network, address)
}

func TestScaleDownDrainResume(t *testing.T) {
	ctx := context.Background()
	replicas := []string{NodeHostname("demo", 0), NodeHostname("demo", 3), NodeHostname("demo", 4)}
	moves := []wvapi.ReplicateRequest{}
	pending := map[string]wvapi.ReplicateRequest{}
	f := coreSetup(t, 5, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v1/schema":
			json.NewEncoder(w).Encode(map[string]any{"classes": []any{map[string]any{"class": "C", "replicationConfig": map[string]int{"factor": 3}}}})
		case "/v1/replication/sharding-state":
			json.NewEncoder(w).Encode(wvapi.ShardingStateResponse{ShardingState: wvapi.ShardingState{Collection: "C", Shards: []wvapi.ShardReplicas{{Shard: "S", Replicas: replicas}}}})
		case "/v1/replication/replicate/list":
			json.NewEncoder(w).Encode([]wvapi.Replication{})
		case "/v1/replication/replicate":
			var req wvapi.ReplicateRequest
			json.NewDecoder(r.Body).Decode(&req)
			moves = append(moves, req)
			id := fmt.Sprint(len(moves))
			pending[id] = req
			json.NewEncoder(w).Encode(wvapi.ReplicateResponse{ID: id})
		default:
			if strings.HasPrefix(r.URL.Path, "/v1/replication/replicate/") {
				id := strings.TrimPrefix(r.URL.Path, "/v1/replication/replicate/")
				req := pending[id]
				for i, n := range replicas {
					if n == req.SourceNode {
						replicas[i] = req.TargetNode
					}
				}
				json.NewEncoder(w).Encode(wvapi.Replication{ID: id, Status: wvapi.ReplicationStatus{State: wvapi.ReplicaReady}})
			} else {
				return false
			}
		}
		return true
	})
	coreConverge(t, f)
	removed := []string{}
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/remove" {
			t.Error(r.URL.Path)
		}
		user, pw, ok := r.BasicAuth()
		want, _ := secret(ctx, f.rc, secretClusterPassword)
		if !ok || user != ClusterUser || pw != string(want) {
			t.Error("missing cluster basic auth")
		}
		s, _ := load(f.rc.Cluster)
		m := s.find(s.ScaleDown.Member)
		if m.CleanupStep != "stopped" {
			t.Error("Raft removal before stop")
		}
		removed = append(removed, m.Name)
	}))
	defer internal.Close()
	f.rc.Dialer = retirementDialer{f.rc.Dialer, strings.TrimPrefix(internal.URL, "http://")}
	before := len(f.backend.Calls())
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	for range 300 {
		if _, err := f.p.Sync(ctx, f.rc); err != nil {
			t.Fatal(err)
		}
		// Every pass reloads a JSON checkpoint, including outstanding MOVE IDs.
		s, err := load(f.rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.ScaleDown != nil && s.ScaleDown.Stage == "volume" && s.Pending == nil {
			m := s.find(s.ScaleDown.Member)
			runtime, e := f.backend.GetService(ctx, m.Name)
			if e != nil || runtime != nil {
				t.Fatal("runtime must be absent before volume deletion", e)
			}
			volume, e := f.backend.GetService(ctx, m.Volume)
			if e != nil || volume == nil {
				t.Fatal("volume removed before runtime retirement", e)
			}
		}
		if err = s.save(f.rc.Cluster); err != nil {
			t.Fatal(err)
		}
		if len(s.Members) == 3 && s.ScaleDown == nil {
			break
		}
	}
	s := coreConverge(t, f)
	if len(s.Members) != 3 {
		t.Fatal("did not retire", len(s.Members))
	}
	if len(moves) != 2 || moves[0].SourceNode != NodeHostname("demo", 4) || moves[1].SourceNode != NodeHostname("demo", 3) {
		t.Fatalf("moves: %+v", moves)
	}
	if moves[0].TargetNode != NodeHostname("demo", 1) || moves[1].TargetNode != NodeHostname("demo", 2) {
		t.Fatalf("targets: %+v", moves)
	}
	if len(removed) != 2 {
		t.Fatal(removed)
	}
	for _, call := range f.backend.Calls()[before:] {
		if call == "Deploy" {
			t.Fatal("remaining members needlessly restarted")
		}
	}
}

func TestScaleDownFeasibilityParks(t *testing.T) {
	f := coreSetup(t, 5, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/schema" {
			return false
		}
		json.NewEncoder(w).Encode(map[string]any{"classes": []any{map[string]any{"class": "TooMany", "replicationConfig": map[string]int{"factor": 4}}}})
		return true
	})
	coreConverge(t, f)
	f.rc.Cluster.Spec.Components.Engine.Replicas = 3
	for range 5 {
		if _, e := f.p.Sync(context.Background(), f.rc); e != nil {
			t.Fatal(e)
		}
	}
	s, _ := load(f.rc.Cluster)
	if s.ScaleDown == nil || !strings.Contains(s.ScaleDown.Failure, "TooMany") {
		t.Fatalf("missing feasibility block: %+v", s.ScaleDown)
	}
	if s.find(s.ScaleDown.Member).Removing {
		t.Fatal("removed before feasibility")
	}
	for range 8 {
		s.ScaleDown.RetryAt = time.Time{}
		if err := s.save(f.rc.Cluster); err != nil {
			t.Fatal(err)
		}
		if _, err := f.p.Sync(context.Background(), f.rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(f.rc.Cluster)
	}
	if s.ScaleDown.Attempts != 3 {
		t.Fatalf("retry bound not honored: %d", s.ScaleDown.Attempts)
	}
	status, e := f.p.Status(context.Background(), f.rc)
	if e != nil {
		t.Fatal(e)
	}
	if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool { return c.Type == "ScaleDown" && c.Reason == "DrainBlocked" }) {
		t.Fatal(status.Conditions)
	}
}

func TestScaleBackUpCancelsDrain(t *testing.T) {
	cancelled := false
	f := coreSetup(t, 5, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancelled = true
			return true
		}
		return false
	})
	s := coreConverge(t, f)
	m := s.Members[4]
	m.Removing = true
	s.ScaleDown = &scaleDown{Member: m.Name, Stage: "drain", MoveID: "move", Target: 3}
	s.save(f.rc.Cluster)
	if _, e := f.p.Sync(context.Background(), f.rc); e != nil {
		t.Fatal(e)
	}
	s, _ = load(f.rc.Cluster)
	if !cancelled || s.ScaleDown != nil || s.find(m.Name).Removing {
		t.Fatal("drain not cancelled")
	}
}

func TestScaleDownAdmissionFixedVoters(t *testing.T) {
	p := New(Options{})
	old := surfaceCluster("cluster")
	old.Spec.Components.Engine.Replicas = 5
	next := *old
	next.Spec.Components.Engine.Replicas = 3
	if err := p.Validate(context.Background(), old, &next); err != nil {
		t.Fatal(err)
	}
	s := &state{Voters: []string{"v0", "v1", "v2", "v3"}}
	if err := s.save(old); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(context.Background(), old, &next); err == nil || !strings.Contains(err.Error(), "voter") {
		t.Fatalf("fixed voter removal accepted: %v", err)
	}
	next.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":4}`)
	if err := p.Validate(context.Background(), old, &next); err == nil {
		t.Fatal("replication minimum above target accepted")
	}
	next = *old
	next.Spec.Components.Engine.Replicas = 2
	if err := p.Validate(context.Background(), old, &next); err == nil {
		t.Fatal("scale-down below three accepted")
	}
}
