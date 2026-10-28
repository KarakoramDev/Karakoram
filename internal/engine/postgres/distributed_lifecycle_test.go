// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type distributedAgents struct {
	mu                    sync.Mutex
	leaders               map[int]string
	nodes                 map[int]pgspec.CitusNode
	blocked               map[int]bool
	calls                 []string
	loseDrain, loseRemove bool
	connectivityFailed    bool
}

func setupDistributed(t *testing.T, replicas int) (*Core, *engine.Context, *fakeBackend, *distributedAgents) {
	t.Helper()
	p, rc, b := setup(t, "distributed")
	rc.Cluster.Spec.Components.Engine.Replicas = replicas
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1}}`)
	// Runtime installation is represented by the fake backend; use a synthetic
	// pinned artifact without changing the production lock or admission behavior.
	p.citusArtifact = func(product string, major int) (nativeartifacts.Artifact, error) {
		return nativeartifacts.Artifact{Name: fmt.Sprintf("%s-pg%d", product, major), Kind: "pg-extension", PGMajor: major, File: "citus-test.tar.gz", SHA256: strings.Repeat("a", 64), UpstreamURL: "https://artifacts.example.invalid/citus-test.tar.gz"}, nil
	}
	original := rc.Agents.(fakeAgents)
	upstream, err := url.Parse(original.url)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(original.ca)
	proxy.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	t.Cleanup(func() { proxy.Transport.(*http.Transport).CloseIdleConnections() })
	fake := &distributedAgents{leaders: map[int]string{}, nodes: map[int]pgspec.CitusNode{}, blocked: map[int]bool{}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		s, err := load(rc.Cluster)
		if err != nil {
			t.Error(err)
			http.Error(w, "state", 500)
			return
		}
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		m := s.find(name)
		leader := func(group int) string {
			if fake.leaders[group] != "" {
				return fake.leaders[group]
			}
			for _, peer := range s.group(group).data() {
				if !peer.Removing {
					return peer.Name
				}
			}
			return ""
		}
		switch r.URL.Path {
		case "/v1/postgres/status":
			if m == nil {
				http.NotFound(w, r)
				return
			}
			if m.Role == "proxy" {
				proxy.ServeHTTP(w, r)
				return
			}
			role := "replica"
			if name == leader(m.Group) {
				role = "primary"
			}
			options, _ := ParseOptions(m.Options)
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: role, Leader: leader(m.Group), Running: !m.Offline, Streaming: role == "replica", Version: m.Version, SystemID: fmt.Sprintf("group-%d", m.Group), Timeline: 1, WALPosition: "1/100", EtcdHealthy: true, EtcdMemberID: name + "-etcd", CredentialReceipts: true, SlotStatusKnown: true, WALStatusKnown: true, ArchiveStatusKnown: true, SharedMemoryParameters: fakeSharedMemory(options.Parameters), HealthCapabilities: &pgspec.HealthCapabilities{LocalEtcd: true, ReplicationSlots: true, WALArchive: true, ReplicationDistance: true, PatroniPause: true}})
		case "/v1/postgres/citus-status", "/v1/postgres/citus-connectivity":
			if r.URL.Path == "/v1/postgres/citus-connectivity" && fake.connectivityFailed {
				http.Error(w, "worker authentication unavailable", http.StatusServiceUnavailable)
				return
			}
			for _, g := range s.groups() {
				l := s.find(leader(g.id))
				if !fake.blocked[g.id] && l != nil && !l.Offline && (l.Stage == "joined" || l.Stage == "deployed") {
					fake.nodes[g.id] = pgspec.CitusNode{NodeID: g.id + 1, GroupID: g.id, Host: l.Name, Port: 5432, Role: "primary", Active: true}
				}
			}
			catalog := pgspec.CitusStatusResponse{IsCoordinator: true}
			for _, node := range fake.nodes {
				catalog.Nodes = append(catalog.Nodes, node)
			}
			json.NewEncoder(w).Encode(catalog)
		case "/v1/postgres/switchover":
			var req pgspec.SwitchoverRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			candidate := s.find(req.Candidate)
			if m == nil || candidate == nil || candidate.Group != m.Group {
				t.Error("cross-group switchover")
			} else {
				fake.leaders[m.Group] = candidate.Name
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/citus-drain-node", "/v1/postgres/citus-remove-node":
			var req pgspec.CitusNodeRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			action := strings.TrimPrefix(r.URL.Path, "/v1/postgres/")
			fake.calls = append(fake.calls, action+":"+req.Host)
			if action == "citus-drain-node" {
				if fake.loseDrain {
					fake.loseDrain = false
					http.Error(w, "lost drain response", http.StatusServiceUnavailable)
					return
				}
			} else {
				for group, node := range fake.nodes {
					if node.Host == req.Host && node.Port == req.Port {
						delete(fake.nodes, group)
					}
				}
				if fake.loseRemove {
					fake.loseRemove = false
					http.Error(w, "lost remove response", http.StatusServiceUnavailable)
					return
				}
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/upgrade-stop":
			if m == nil || !m.Offline {
				t.Error("stop before durable offline fence")
			}
			fake.calls = append(fake.calls, "stop:"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			proxy.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, b, fake
}

func TestDistributedFormationLifecycle(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			p, rc, _, fake := setupDistributed(t, replicas)
			if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			s := converge(t, p, rc)
			if len(s.data()) != 2*replicas || !s.GroupFormed[1] {
				t.Fatal("formation not registered", s)
			}
			if replicas == 3 {
				fake.leaders[1] = "demow1p1"
				if err := switchAway(t.Context(), rc, s, "demow1p1"); err != nil {
					t.Fatal(err)
				}
				if fake.leaders[1] == "demow1p1" || fake.leaders[0] != "" {
					t.Fatal("group handoff did not stay local", fake.leaders)
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "CitusTopology" && condition.Status != "True" {
					t.Fatal(condition)
				}
			}
			connection, err := p.Connection(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(connection.Connection.URI, "demopx:5432/app") || connection.Credentials[0].Database != "app" {
				t.Fatal("connection did not target coordinator database", connection)
			}
			// Retain stale metadata while a worker promotes: report rather than repair.
			fake.blocked[1] = true
			fake.nodes[1] = pgspec.CitusNode{GroupID: 1, Host: "stale", Port: 5432, Role: "primary", Active: true}
			status, err = p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			drift := false
			for _, condition := range status.Conditions {
				drift = drift || condition.Type == "CitusTopology" && condition.Reason == "MetadataDrift"
			}
			if !drift {
				t.Fatal("metadata drift not reported")
			}
		})
	}
}

func TestDistributedScaleRegistrationAndDrainReplay(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			testDistributedScaleRegistrationAndDrainReplay(t, replicas)
		})
	}
}

func testDistributedScaleRegistrationAndDrainReplay(t *testing.T, replicas int) {
	p, rc, b, fake := setupDistributed(t, replicas)
	converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	rc.Cluster.Generation++
	fake.blocked[2] = true
	for range 80 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.find("demow2p0") == nil || s.find("demow2p0").Stage != "joined" || s.GroupFormed[2] {
		t.Fatal("scale-up registration barrier failed")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.ObservedGeneration == rc.Cluster.Generation {
		t.Fatal("unregistered worker reported ready")
	}
	fake.blocked[2] = false
	s = converge(t, p, rc)
	if !s.GroupFormed[2] {
		t.Fatal("registration did not commit")
	}
	storage := &domain.BackupStorage{Prefix: "archive"}
	oldRepo := groupRepoPrefix(storage, rc.Cluster, s, 2)
	coordinatorRepo := groupRepoPrefix(storage, rc.Cluster, s, 0)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1}}`)
	rc.Cluster.Generation++
	fake.loseDrain, fake.loseRemove = true, true
	lost := 0
	for range 100 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			if !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "citus-drain-node") && !strings.Contains(err.Error(), "citus-remove-node") {
				t.Fatal(err)
			}
			lost++
			// Simulate process restart from the last durable cluster checkpoint.
			p = New(Options{})
			p.citusArtifact = func(product string, major int) (nativeartifacts.Artifact, error) {
				return nativeartifacts.Artifact{Name: fmt.Sprintf("%s-pg%d", product, major), Kind: "pg-extension", PGMajor: major, File: "citus-test.tar.gz", SHA256: strings.Repeat("a", 64), UpstreamURL: "https://artifacts.example.invalid/citus-test.tar.gz"}, nil
			}
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.CitusScale == nil && s.find("demow2p0") == nil {
			break
		}
	}
	if lost != 2 || s.CitusScale != nil || len(s.group(2).data()) != 0 || b.services["demow2p0"] != nil || b.services["demow2p0v"] != nil {
		t.Fatalf("retirement incomplete: lost=%d state=%+v", lost, s.CitusScale)
	}
	drain := slices.Index(fake.calls, "citus-drain-node:demow2p0")
	stop := slices.Index(fake.calls, "stop:demow2p0")
	remove := slices.Index(fake.calls, "citus-remove-node:demow2p0")
	if drain < 0 || stop <= drain || remove <= stop {
		t.Fatal("unsafe drain/remove ordering", fake.calls)
	}
	removals := 0
	for _, call := range fake.calls {
		if call == "citus-remove-node:demow2p0" {
			removals++
		}
	}
	if removals != 1 {
		t.Fatal("ambiguous removal replayed without observing catalog", fake.calls)
	}
	converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	rc.Cluster.Generation++
	s = converge(t, p, rc)
	if len(s.group(2).data()) != replicas || s.find(fmt.Sprintf("demow2p%d", replicas)) == nil {
		t.Fatal("recreated worker group reused an identity")
	}
	if s.GroupRepositoryIncarnations[2] != 1 || groupRepoPrefix(storage, rc.Cluster, s, 2) == oldRepo {
		t.Fatal("recreated worker reused its retired backup repository")
	}
	if groupRepoPrefix(storage, rc.Cluster, s, 0) != coordinatorRepo {
		t.Fatal("worker retirement changed coordinator repository")
	}
}
