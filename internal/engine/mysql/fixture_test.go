// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeBackend struct {
	infra.Backend
	services          map[string]*infra.Service
	deployed          map[string]bool
	mu                sync.Mutex
	applied           map[string]int64
	native            map[string]myspec.StatusResponse
	offline           map[string]myspec.OfflineGTIDResponse
	bootstrap         map[uint64]int
	bootstrapReceipts map[uint64]bool
	clones            map[string]string
	leader            string
	action            func(http.ResponseWriter, *http.Request, string) bool
	agentBuild        agentapi.AgentBuild
}

func (*fakeBackend) Name() string { return "fake" }
func (*fakeBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{HorizontalScaling: true, Placement: true, PublicTCP: true}
}
func (*fakeBackend) Layout() infra.Layout {
	return infra.Layout{AppDir: "/srv/application", VolumeRoot: "/srv/volumes", User: "app", PrivateAddressFamily: infra.AddressFamilyIPv4}
}
func (b *fakeBackend) GetService(_ context.Context, name string) (*infra.Service, error) {
	return b.services[name], nil
}
func (b *fakeBackend) SetEnvVars(_ context.Context, name string, vars map[string]string) error {
	for k, v := range vars {
		b.services[name].Ownership[k] = v
	}
	return nil
}
func (b *fakeBackend) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	ids := map[string]string{}
	for _, spec := range specs {
		ownership := map[string]string{}
		for k, v := range spec.Env {
			ownership[k] = v
		}
		for k, v := range spec.Vars {
			ownership[k] = v
		}
		b.services[spec.Hostname] = &infra.Service{ID: spec.Hostname + "-id", Hostname: spec.Hostname, Kind: spec.Kind, Ownership: ownership, Resources: spec.Resources}
		if spec.Kind == infra.KindVolume {
			b.services[spec.Hostname].Status = infra.StatusActive
		}
		ids[spec.Hostname] = spec.Hostname + "-id"
	}
	return infra.Op{Services: ids}, nil
}
func (b *fakeBackend) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if _, err := io.Copy(io.Discard, d.Artifact.Reader); err != nil {
		return infra.Op{}, err
	}
	b.deployed[name] = true
	b.services[name].Status = infra.StatusRunning
	return infra.Op{}, nil
}
func (*fakeBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: true}, nil
}
func (b *fakeBackend) Containers(_ context.Context, name string) ([]infra.Container, error) {
	if b.services[name] == nil {
		return nil, infra.ErrNotFound
	}
	return []infra.Container{{ID: name + "-instance", Hostname: name + "-instance", Status: infra.ContainerRunning, PlacementGroup: name + "-host"}}, nil
}
func (*fakeBackend) DeletePublicPorts(context.Context, string) error { return nil }
func (b *fakeBackend) DeleteServiceByID(_ context.Context, name, id string) (infra.Op, error) {
	if service := b.services[name]; service != nil && service.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	delete(b.services, name)
	delete(b.deployed, name)
	return infra.Op{}, nil
}

func (b *fakeBackend) Stop(_ context.Context, name string) (infra.Op, error) {
	b.services[name].Status = infra.StatusStopped
	return infra.Op{}, nil
}
func (b *fakeBackend) SetResources(_ context.Context, name string, r infra.Resources) (infra.Op, error) {
	b.services[name].Resources = r
	return infra.Op{}, nil
}
func (*fakeBackend) SetContainerCount(context.Context, string, int, int) (infra.Op, error) {
	return infra.Op{}, nil
}

type fakeArtifact struct{}

func (fakeArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type fakeAgents struct {
	url string
	ca  []byte
}

func (a fakeAgents) Dial(_ context.Context, _ domain.ClusterRef, member string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(a.url)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: member, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}})
}
func setup(t *testing.T, topology string) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	p := New(Options{})
	c := &domain.Cluster{Name: "demo", Namespace: "default", Generation: 1, Spec: domain.ClusterSpec{Topology: domain.Topology{Type: topology}}}
	p.ApplyDefaults(&c.Spec)
	if topology == "single" {
		proxy := c.Spec.Components.Extra["proxy"]
		proxy.Replicas = 0
		c.Spec.Components.Extra["proxy"] = proxy
	}
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	b := &fakeBackend{services: map[string]*infra.Service{}, deployed: map[string]bool{}, applied: map[string]int64{}, native: map[string]myspec.StatusResponse{}, offline: map[string]myspec.OfflineGTIDResponse{}, bootstrap: map[uint64]int{}, bootstrapReceipts: map[uint64]bool{}, clones: map[string]string{}}
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: b, Artifact: fakeArtifact{}}
	rechecks := map[string]string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if r.URL.Path == "/v1/mysql/restore-status" && rechecks[name] != "" {
			json.NewEncoder(w).Encode(myspec.RestoreStatus{State: "prepared", Stage: "ready", PlanDigest: fixtureManifestDigest, RecheckID: rechecks[name], VerifiedRecheckID: rechecks[name]})
			delete(rechecks, name)
			return
		}
		if b.action != nil && b.action(w, r, name) {
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			return
		}
		switch r.URL.Path {
		case "/v1/mysql/restore-preflight":
			var request myspec.RestoreRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			rechecks[name] = request.RecheckID
			json.NewEncoder(w).Encode(myspec.RestoreStatus{Receipt: request.Receipt, State: "prepared", Stage: "ready", PlanDigest: fixtureManifestDigest, RecheckID: request.RecheckID, VerifiedRecheckID: request.RecheckID})
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{Agent: b.agentBuild, ConfigVersion: b.applied[name]})
		case "/v1/mysql/status":
			status := b.native[name]
			for _, member := range b.native {
				if member.MemberState == "ONLINE" {
					status.GroupMembers++
				}
			}
			json.NewEncoder(w).Encode(status)
		case "/v1/mysql/offline-gtid":
			json.NewEncoder(w).Encode(b.offline[name])
		case "/v1/mysql/proxy-status":
			json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: 10, Host: b.leader, Port: 3306, Status: "ONLINE"}}})
		case "/v1/reload", "/v1/process":
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/mysql/bootstrap":
			var req myspec.GroupRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if !b.bootstrapReceipts[req.FormationEpoch] {
				b.bootstrapReceipts[req.FormationEpoch] = true
				b.bootstrap[req.FormationEpoch]++
				b.leader = name
				status := b.native[name]
				status.MemberState = "ONLINE"
				status.MemberRole = "PRIMARY"
				status.ReadOnly, status.SuperReadOnly = false, false
				status.DataIdentity.FormationEpoch = req.FormationEpoch
				b.native[name] = status
			}
			json.NewEncoder(w).Encode(struct{}{})
		case "/v1/mysql/clone":
			var req myspec.CloneRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			b.clones[name] = req.Donor.Name
			json.NewEncoder(w).Encode(struct{}{})
		case "/v1/mysql/start-group":
			status := b.native[name]
			status.MemberState = "ONLINE"
			status.MemberRole = "SECONDARY"
			status.ReadOnly, status.SuperReadOnly = true, true
			b.native[name] = status
			json.NewEncoder(w).Encode(struct{}{})
		case "/v1/mysql/stop-group":
			status := b.native[name]
			status.MemberState = "OFFLINE"
			b.native[name] = status
			json.NewEncoder(w).Encode(struct{}{})
		case "/v1/mysql/switchover":
			var req myspec.SwitchoverRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			for member, status := range b.native {
				status.MemberRole = "SECONDARY"
				status.ReadOnly = true
				status.SuperReadOnly = true
				if status.ServerUUID == req.TargetUUID {
					status.MemberRole = "PRIMARY"
					status.ReadOnly = false
					status.SuperReadOnly = false
					b.leader = member
				}
				b.native[member] = status
			}
			json.NewEncoder(w).Encode(struct{}{})
		default:
			t.Errorf("unexpected agent action %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, b
}

// apply simulates durable agent config/formation receipts independently of Core instances.
func (b *fakeBackend) apply(t *testing.T, p *Core, rc *engine.Context) {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		if !b.deployed[m.Name] {
			continue
		}
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp myspec.Spec
		if err = json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
			t.Fatal(err)
		}
		b.applied[m.Name] = cfg.ConfigVersion
		if m.Role == "proxy" {
			continue
		}
		status := b.native[m.Name]
		status.Version = sp.Version
		if status.ServerUUID == "" {
			status.ServerUUID = fmt.Sprintf("11111111-2222-3333-4444-%012d", m.Index+1)
			status.DataIdentity.Initialized = true
		}
		if len(cfg.DesiredProcesses) == 0 {
			status.MemberState = "OFFLINE"
			if b.leader == m.Name {
				for _, peer := range s.data() {
					other := b.native[peer.Name]
					if peer.Name != m.Name && other.MemberState == "ONLINE" && !peer.Offline {
						b.leader = peer.Name
						other.MemberRole = "PRIMARY"
						other.ReadOnly, other.SuperReadOnly = false, false
						b.native[peer.Name] = other
						break
					}
				}
			}
			b.native[m.Name] = status
			continue
		}
		if sp.Topology == "single" {
			status.MemberState = "ONLINE"
			status.MemberRole = "PRIMARY"
		} else if status.MemberState == "" {
			status.MemberState = "OFFLINE"
		}
		if sp.Galera != nil && !sp.Galera.Bootstrap && b.leader != "" {
			status.MemberState = "ONLINE"
			status.MemberRole = "SECONDARY"
			status.ReadOnly, status.SuperReadOnly = false, false
		}
		b.native[m.Name] = status
	}
}
func converge(t *testing.T, p *Core, rc *engine.Context, b *fakeBackend) {
	t.Helper()
	for i := range 120 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
		b.apply(t, p, rc)
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			return
		}
		p = New(Options{Provider: p.Name()})
	}
	s, _ := load(rc.Cluster)
	t.Fatalf("did not converge: %+v", s)
}
