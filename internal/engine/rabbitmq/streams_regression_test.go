// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type streamManagementDialer struct{ address string }

func (d streamManagementDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

func TestStreamReplicasMoveBeforeQueueShrink(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.TLS.Mode = "disabled"
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"plugins":["rabbitmq_stream"]}`)
	ready(t, p, rc)
	a.log.clear()
	var mu sync.Mutex
	queues := []mgmt.Queue{
		{Name: "events", Vhost: "/", Type: "stream", Members: []string{"rabbit@demom4", "rabbit@demom3"}},
		{Name: "jobs", Vhost: "/", Type: "quorum", Members: []string{"rabbit@demom4"}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "karakoram" || password == "" {
			t.Error("management request lacks system authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/api/overview":
			_ = json.NewEncoder(w).Encode(map[string]string{"rabbitmq_version": "4.3.6"})
		case "/api/queues":
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(queues)
		default:
			t.Errorf("unexpected management request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Dialer = streamManagementDialer{address: server.Listener.Addr().String()}
	a.streamHandler = func(req rmqspec.StreamReplicasRequest) {
		mu.Lock()
		defer mu.Unlock()
		if req.VHost != "/" || req.Queue != "events" {
			t.Errorf("stream command targeted wrong queue: %+v", req)
		}
		switch req.Action {
		case "add":
			if slices.Contains(queues[0].Members, req.Node) {
				t.Errorf("stream replica add replayed: %+v", req)
			}
			queues[0].Members = append(queues[0].Members, req.Node)
		case "delete":
			for _, survivor := range []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"} {
				if !slices.Contains(queues[0].Members, survivor) {
					t.Errorf("deleted leaving stream replica before %s was added", survivor)
				}
			}
			queues[0].Members = slices.DeleteFunc(queues[0].Members, func(node string) bool { return node == req.Node })
		default:
			t.Errorf("unexpected stream action %+v", req)
		}
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	ready(t, p, rc)
	calls := a.log.snapshot()
	for _, survivor := range []string{"demom0", "demom1", "demom2"} {
		add := "agent:demom0:stream-replicas:add:rabbit@" + survivor
		if countCall(calls, add) != 1 {
			t.Fatalf("stream replica was not added exactly once: %v", calls)
		}
		assertBefore(t, calls, "agent:demom0:grow:rabbit@"+survivor, add)
		assertBefore(t, calls, add, "agent:demom0:stream-replicas:delete:rabbit@demom4")
	}
	for _, leaving := range []string{"demom4", "demom3"} {
		remove := "agent:demom0:stream-replicas:delete:rabbit@" + leaving
		if countCall(calls, remove) != 1 {
			t.Fatalf("leaving stream replica was not deleted exactly once: %v", calls)
		}
		assertBefore(t, calls, remove, "agent:demom0:shrink:rabbit@"+leaving)
	}
	mu.Lock()
	defer mu.Unlock()
	members := slices.Clone(queues[0].Members)
	slices.Sort(members)
	if !slices.Equal(members, []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}) || !slices.Equal(queues[1].Members, []string{"rabbit@demom4"}) {
		t.Fatalf("stream migration misplaced replicas or touched non-stream queue: %+v", queues)
	}
}

func TestScaleDownOfUnstartedMembersAvoidsBrokerActions(t *testing.T) {
	for _, stage := range []string{"planned", "imported"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			for range 2 {
				m := s.add(rc.Cluster.Name)
				m.Stage = stage
				if stage == "imported" {
					m.ServiceID, m.VolumeID = m.Name, m.Volume
					f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name, Type: "ubuntu@24.04"}
					f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Type: "local-storage:single@1"}
				}
			}
			if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			a.log.clear()
			ready(t, p, rc)
			for _, call := range a.log.snapshot() {
				if strings.HasPrefix(call, "agent:demom3:") || strings.HasPrefix(call, "agent:demom4:") {
					t.Fatalf("called an undeployed broker: %s", call)
				}
				for _, action := range []string{":grow", ":shrink", ":drain", ":stop", ":forget"} {
					if strings.Contains(call, "agent:") && strings.Contains(call, action) {
						t.Fatalf("unstarted member removal should not manipulate cluster queues: %s", call)
					}
				}
			}
			for _, name := range []string{"demom3", "demom4"} {
				if f.services[name] != nil || f.services[name+"v"] != nil {
					t.Fatalf("unstarted member resources survived: %s", name)
				}
			}
			if state(t, rc).NextIndex != 5 {
				t.Fatal("unstarted member removal reused allocation indices")
			}
		})
	}
}
