// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type admissionBackend struct {
	infra.Backend
	proxy string
	count int
	stops map[string]int
}

func (b *admissionBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name != b.proxy {
		return b.Backend.Containers(ctx, name)
	}
	out := []infra.Container{{ID: "first", Hostname: "first", Status: infra.ContainerRunning}, {ID: "second", Hostname: "second", Status: infra.ContainerRunning}}
	return out[:b.count], nil
}

func (b *admissionBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	b.stops[name]++
	return b.Backend.Stop(ctx, name)
}

type admissionAgents struct {
	fakeAgents
	proxy string
	peers map[string]string
}

func (a admissionAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name != a.proxy {
		return a.fakeAgents.Dial(ctx, ref, name)
	}
	endpoint, err := url.Parse(a.url)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: name,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
		},
		DialInstanceContext: func(ctx context.Context, network, _, instance string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, a.peers[instance])
		},
	})
}

// Each response is independently mutable between reconciliations. The wire
// image deliberately contains only the backends fields shipped in v0.4.0.
func admissionFixture(t *testing.T, rc *engine.Context, s *state) (*admissionBackend, *[2]myspec.ProxyStatusResponse, *[2]bool) {
	t.Helper()
	var proxy *member
	for _, m := range s.Members {
		if m.Role == "proxy" {
			proxy = m
		}
	}
	if proxy == nil {
		t.Fatal("missing fixture proxy")
	}
	proxy.Replicas = 2
	responses := new([2]myspec.ProxyStatusResponse)
	failed := new([2]bool)
	agents := admissionAgents{fakeAgents: rc.Agents.(fakeAgents), proxy: proxy.Name, peers: map[string]string{}}
	for i, instance := range []string{"first", "second"} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if failed[i] {
				http.Error(w, "observation unavailable", http.StatusServiceUnavailable)
				return
			}
			if r.URL.Path == "/v1/status" {
				current, err := load(rc.Cluster)
				if err != nil {
					http.Error(w, "checkpoint unavailable", http.StatusServiceUnavailable)
					return
				}
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: current.find(proxy.Name).ConfigVersion})
				return
			}
			json.NewEncoder(w).Encode(responses[i])
		}))
		t.Cleanup(server.Close)
		agents.ca = append(agents.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})...)
		agents.peers[instance] = server.Listener.Addr().String()
	}
	backend := &admissionBackend{Backend: rc.Backend, proxy: proxy.Name, count: 2, stops: map[string]int{}}
	rc.Backend, rc.Agents = backend, agents
	return backend, responses, failed
}

func admitted(host string, group int, status string) myspec.ProxyBackend {
	return myspec.ProxyBackend{Host: host, Hostgroup: group, Port: 3306, Status: status}
}

func replayAdmissionState(t *testing.T, rc *engine.Context, s *state) *state {
	t.Helper()
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func TestRollingRejoinWaitsForEveryProxyInstance(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			if provider == "pxc" {
				p, rc, b = setupPXC(t)
			}
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			var next *member
			for _, m := range s.data() {
				if m.Name != b.leader {
					next = m
					break
				}
			}
			backend, responses, failed := admissionFixture(t, rc, s)
			s.Rolling = &rollingState{To: rc.Cluster.Spec.Version, Order: []string{next.Name}, Stage: "online"}
			for _, blocked := range []myspec.ProxyBackend{admitted(next.Name, 30, "ONLINE"), admitted(next.Name, 20, "SHUNNED"), admitted("unrelated", 10, "ONLINE")} {
				for i := range responses {
					responses[i].Backends = []myspec.ProxyBackend{admitted(b.leader, 10, "ONLINE"), blocked}
				}
				if _, err := p.reconcileRolling(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) || s.Rolling.Index != 0 {
					t.Fatalf("advanced past non-admitted member: %v", err)
				}
			}
			responses[0].Backends = []myspec.ProxyBackend{admitted(b.leader, 10, "ONLINE"), admitted(next.Name, 20, "ONLINE")}
			s = replayAdmissionState(t, rc, s)
			if _, err := p.reconcileRolling(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) || s.Rolling.Index != 0 {
				t.Fatal("checkpoint replay accepted partial admission", err)
			}
			responses[1] = responses[0]
			failed[1] = true
			if _, err := p.reconcileRolling(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("accepted unavailable proxy", err)
			}
			failed[1] = false
			backend.count = 1
			if _, err := p.reconcileRolling(t.Context(), rc, s); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("accepted missing serving replica", err)
			}
			backend.count = 2
			if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil || s.Rolling.Index != 1 {
				t.Fatal("both proxies admitted member but rolling did not advance", err)
			}
		})
	}
}

func TestPrimaryRetirementProxyBarriersReplay(t *testing.T) {
	for _, operation := range []string{"rolling", "replacement", "scale-down"} {
		t.Run(operation, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			old := b.leader
			var target string
			for _, m := range s.data() {
				if m.Name != old {
					target = m.Name
				}
			}
			// Make the candidate deterministic without weakening GTID containment.
			for name, native := range b.native {
				if name != old && name != target {
					native.ServerUUID = ""
					b.native[name] = native
				}
			}
			backend, responses, _ := admissionFixture(t, rc, s)
			m := s.find(old)
			if operation == "rolling" {
				s.Rolling = &rollingState{To: rc.Cluster.Spec.Version, Order: []string{old}, Stage: "handoff", Targets: map[string]domain.ComponentSpec{old: desiredComponent(rc, m)}}
			} else {
				m.Retirement = "handoff"
				if operation == "replacement" {
					s.Replacement = &replacementState{Old: old, New: target, Stage: "retire"}
				}
			}
			switches, stops := 0, 0
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/switchover" {
					switches++
				}
				if r.URL.Path == "/v1/mysql/stop-group" && name == old {
					stops++
				}
				return false
			}
			tick := func() error {
				if operation == "rolling" {
					_, err := p.reconcileRolling(t.Context(), rc, s)
					return err
				}
				_, err := p.retireMember(t.Context(), rc, s, s.find(old))
				return err
			}
			for _, group := range []int{30, 20} {
				status := "ONLINE"
				if group == 20 {
					status = "SHUNNED"
				}
				for i := range responses {
					responses[i].Backends = []myspec.ProxyBackend{admitted(old, 10, "ONLINE"), admitted(target, group, status)}
				}
				if err := tick(); !errors.Is(err, engine.ErrUnavailable) || switches != 0 || s.find(old).Offline {
					t.Fatal("switched before target reader admission", err)
				}
			}
			responses[0].Backends = []myspec.ProxyBackend{admitted(old, 10, "ONLINE"), admitted(target, 20, "ONLINE")}
			if err := tick(); !errors.Is(err, engine.ErrUnavailable) || switches != 0 {
				t.Fatal("partial admission allowed switch", err)
			}
			responses[1] = responses[0]
			if err := tick(); err != nil || switches != 0 {
				t.Fatal("selection was not checkpointed before action", err)
			}
			s = replayAdmissionState(t, rc, s)
			p = New(Options{})
			if err := tick(); err != nil || switches != 1 || b.leader != target {
				t.Fatal("admitted native-safe target was not elected", err)
			}
			s = replayAdmissionState(t, rc, s)
			if err := tick(); !errors.Is(err, engine.ErrUnavailable) || s.find(old).Offline || s.find(old).Removing || stops != 0 {
				t.Fatal("old writer retired before selected writer HG10", err)
			}
			responses[0].Backends = []myspec.ProxyBackend{admitted(target, 10, "ONLINE")}
			if err := tick(); !errors.Is(err, engine.ErrUnavailable) || stops != 0 {
				t.Fatal("partial writer admission allowed stop", err)
			}
			responses[1] = responses[0]
			if err := tick(); err != nil {
				t.Fatal("writer admission did not release handoff", err)
			}
			s = replayAdmissionState(t, rc, s)
			b.apply(t, p, rc)
			for range 3 {
				if err := tick(); err != nil {
					t.Fatal(err)
				}
				s = replayAdmissionState(t, rc, s)
				b.apply(t, p, rc)
			}
			if operation == "rolling" {
				if backend.stops[old] != 1 {
					t.Fatalf("backend stop count=%d", backend.stops[old])
				}
			} else if stops != 1 || backend.stops[old] != 1 {
				t.Fatalf("native/backend stops=%d/%d", stops, backend.stops[old])
			}
		})
	}
}

func TestPXCPreOfflineRequiresAdmittedSurvivor(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	_, responses, _ := admissionFixture(t, rc, s)
	old := b.leader
	for i := range responses {
		responses[i].Backends = []myspec.ProxyBackend{admitted(old, 10, "ONLINE")}
	}
	target := ""
	if err := proxyHandoffReady(t.Context(), rc, s, old, &target); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("old writer alone satisfied survivor barrier", err)
	}
	for _, peer := range s.data() {
		if peer.Name != old {
			target = peer.Name
			break
		}
	}
	responses[0].Backends = append(responses[0].Backends, admitted(target, 11, "ONLINE"))
	if err := proxyHandoffReady(t.Context(), rc, s, old, &target); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("partial Galera admission passed", err)
	}
	responses[1] = responses[0]
	if err := proxyHandoffReady(t.Context(), rc, s, old, &target); err != nil {
		t.Fatal("admitted backup writer did not release Galera barrier", err)
	}
}

func TestProxyHandoffReselectsElectedWriterAcrossReplay(t *testing.T) {
	for _, path := range []string{"rolling-handoff", "rolling-offline", "retirement-handoff", "retirement-drain"} {
		t.Run(path, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			old := b.leader
			var peers []string
			for _, peer := range s.data() {
				if peer.Name != old {
					peers = append(peers, peer.Name)
				}
			}
			pinned, elected := peers[0], peers[1]
			backend, responses, _ := admissionFixture(t, rc, s)
			m := s.find(old)
			switch path {
			case "rolling-handoff", "rolling-offline":
				stage := "handoff"
				if path == "rolling-offline" {
					stage, m.Offline = "offline", true
				}
				s.Rolling = &rollingState{To: rc.Cluster.Spec.Version, Order: []string{old}, Stage: stage, HandoffTarget: pinned, Targets: map[string]domain.ComponentSpec{old: desiredComponent(rc, m)}}
			case "retirement-handoff":
				m.Retirement, m.HandoffTarget = "handoff", pinned
			case "retirement-drain":
				m.Retirement, m.HandoffTarget, m.Removing = "drain", pinned, true
			}
			// The selected primary has restarted and rejoined as a secondary;
			// another healthy member won a legitimate native election.
			for name, native := range b.native {
				native.MemberRole = "SECONDARY"
				native.ReadOnly, native.SuperReadOnly = true, true
				if name == elected {
					native.MemberRole = "PRIMARY"
					native.ReadOnly, native.SuperReadOnly = false, false
				}
				b.native[name] = native
			}
			b.leader = elected
			for i := range responses {
				responses[i].Backends = []myspec.ProxyBackend{admitted(pinned, 10, "ONLINE"), admitted(elected, 20, "ONLINE")}
			}
			nativeStops := 0
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if name == old && r.URL.Path == "/v1/mysql/stop-group" {
					nativeStops++
				}
				return false
			}
			tick := func() error {
				if s.Rolling != nil {
					_, err := p.reconcileRolling(t.Context(), rc, s)
					return err
				}
				_, err := p.retireMember(t.Context(), rc, s, s.find(old))
				return err
			}
			s = replayAdmissionState(t, rc, s)
			b.apply(t, p, rc)
			if err := tick(); !errors.Is(err, errProxyWriterChanged) {
				t.Fatal("native election did not checkpoint a new admission target", err)
			}
			s = replayAdmissionState(t, rc, s)
			target := s.find(old).HandoffTarget
			if s.Rolling != nil {
				target = s.Rolling.HandoffTarget
			}
			if target != elected || backend.stops[old] != 0 || nativeStops != 0 {
				t.Fatal("reselection was not durable or stopped old writer", target)
			}
			p = New(Options{})
			if err := tick(); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("obsolete writer HG10 admitted the newly elected target", err)
			}
			responses[0].Backends = []myspec.ProxyBackend{admitted(elected, 10, "ONLINE")}
			if err := tick(); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("partial admission of newly elected writer passed", err)
			}
			responses[1] = responses[0]
			if err := tick(); err != nil {
				t.Fatal("new native writer admitted everywhere did not release maintenance", err)
			}
		})
	}
}

func TestSwitchAwayRanksOnlyProxyAdmittedCandidates(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	old := b.leader
	var peers []string
	for _, peer := range s.data() {
		if peer.Name != old {
			peers = append(peers, peer.Name)
		}
	}
	eligible, blocked := peers[0], peers[1]
	for name, native := range b.native {
		native.GTIDExecuted = "11111111-2222-3333-4444-555555555555:1-9"
		b.native[name] = native
	}
	_, responses, _ := admissionFixture(t, rc, s)
	for i := range responses {
		responses[i].Backends = []myspec.ProxyBackend{admitted(old, 10, "ONLINE"), admitted(eligible, 20, "ONLINE"), admitted(blocked, 30, "ONLINE")}
	}
	target := &s.find(old).HandoffTarget
	if err := switchAway(t.Context(), rc, s, old, target); err != nil || *target != eligible || b.leader != old {
		t.Fatal("equal-GTID but proxy-offlined tie winner prevented eligible selection", *target, err)
	}
	s = replayAdmissionState(t, rc, s)
	target = &s.find(old).HandoffTarget
	// Even if the former tie winner recovers, replay retains the admitted
	// candidate whose identity was checkpointed before native switchover.
	for i := range responses {
		responses[i].Backends = append(responses[i].Backends, admitted(blocked, 20, "ONLINE"))
	}
	if err := switchAway(t.Context(), rc, s, old, target); err != nil || b.leader != eligible {
		t.Fatal("checkpointed eligible candidate was not promoted", err)
	}
}
