// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeBackend struct {
	infra.Backend
	services            map[string]*infra.Service
	deployed            map[string]bool
	imports             []infra.ServiceSpec
	leader              string
	reloads             []string
	etcd                map[string]pgspec.EtcdMember
	actions             []string
	statusOverrides     map[string]pgspec.StatusResponse
	statusWireOverrides map[string]json.RawMessage
	genericOverrides    map[string]agentapi.Status
	appliedVersions     map[string]int64
	reloadFailures      int
	statusFailures      map[string]int
	fenced              map[string]bool
	held                map[string]bool
	runtimeStopBusy     bool
	runtimeStopFail     bool
	upgradePaused       bool
	upgradePausePending map[string]bool
	sharedMemory        map[string]map[string]int64
	beforeSwitchover    func(string, string) string
	plaintextSwitchover bool
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
		b.imports = append(b.imports, spec)
	}
	return infra.Op{Services: ids}, nil
}
func (b *fakeBackend) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if _, err := io.Copy(io.Discard, d.Artifact.Reader); err != nil {
		return infra.Op{}, err
	}
	b.actions = append(b.actions, "deploy:"+name)
	b.deployed[name] = true
	b.services[name].Status = infra.StatusRunning
	return infra.Op{}, nil
}
func (b *fakeBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "retirement-stop" {
		return infra.OpState{Done: !b.runtimeStopBusy, Failed: b.runtimeStopFail}, nil
	}
	return infra.OpState{Done: true}, nil
}

func (b *fakeBackend) Stop(_ context.Context, name string) (infra.Op, error) {
	b.actions = append(b.actions, "runtime-stop:"+name)
	b.services[name].Status = infra.StatusStopped
	return infra.Op{Kind: "retirement-stop", Hostname: name}, nil
}
func (*fakeBackend) Containers(_ context.Context, name string) ([]infra.Container, error) {
	return []infra.Container{{ID: name + "-instance", Hostname: name + "-instance", Status: infra.ContainerRunning, PlacementGroup: "node-a"}}, nil
}
func (*fakeBackend) DeletePublicPorts(context.Context, string) error { return nil }
func (b *fakeBackend) DeleteServiceByID(_ context.Context, name, id string) (infra.Op, error) {
	if service := b.services[name]; service != nil && service.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	b.actions = append(b.actions, "delete:"+name)
	delete(b.services, name)
	delete(b.deployed, name)
	return infra.Op{}, nil
}
func (b *fakeBackend) SetContainerCount(_ context.Context, name string, min, max int) (infra.Op, error) {
	b.services[name].MinContainers = min
	b.services[name].MaxContainers = max
	return infra.Op{}, nil
}
func (b *fakeBackend) SetResources(_ context.Context, name string, r domain.Resources) (infra.Op, error) {
	b.services[name].Resources = r
	b.actions = append(b.actions, "resize:"+name)
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
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: member,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
		},
	})
}
func setup(t *testing.T, topology string) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	return setupWithStatePath(t, topology, filepath.Join(t.TempDir(), "state.db"))
}
func setupWithStatePath(t *testing.T, topology, statePath string) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(statePath, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	p := New(Options{})
	c := &domain.Cluster{Name: "demo", Namespace: "default", Generation: 1, Spec: domain.ClusterSpec{Topology: domain.Topology{Type: topology}}}
	p.ApplyDefaults(&c.Spec)
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	b := &fakeBackend{services: map[string]*infra.Service{}, deployed: map[string]bool{}}
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: b, Artifact: fakeArtifact{}}
	var agentMu sync.Mutex
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parallel observation requests share this fake process's maps.
		agentMu.Lock()
		defer agentMu.Unlock()
		s, _ := load(c)
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		config := int64(0)
		if m := s.find(name); m != nil {
			config = m.ConfigVersion
			if m.TLSReloadPending {
				config = m.TLSReloadVersion
			}
		}
		switch r.URL.Path {
		case "/v1/status":
			if applied, ok := b.appliedVersions[name]; ok {
				config = applied
			}
			status := agentapi.Status{ConfigVersion: config}
			if override, ok := b.genericOverrides[name]; ok {
				status = override
			}
			json.NewEncoder(w).Encode(status)
		case "/v1/postgres/status":
			if b.statusFailures[name] > 0 {
				b.statusFailures[name]--
				http.Error(w, "PostgreSQL observation temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			if wire, ok := b.statusWireOverrides[name]; ok {
				json.NewEncoder(w).Encode(wire)
				return
			}
			role := "standalone"
			if c.Spec.Topology.Type == "ha" {
				leader := b.leader
				if leader == "" {
					leader = "demop0"
				}
				role = "replica"
				if name == leader {
					role = "primary"
				}
			}
			if strings.HasSuffix(name, "px") {
				role = "proxy"
			}
			m := s.find(name)
			version := c.Spec.Version
			running := true
			if m != nil {
				version = m.Version
				running = !m.Offline
			}
			status := pgspec.StatusResponse{
				HealthCapabilities: &pgspec.HealthCapabilities{
					LocalEtcd: true, ReplicationSlots: true, WALArchive: true,
					ReplicationDistance: true, PatroniPause: true,
				},
				CredentialReceipts: true,
				Role:               role, Running: running, Version: version, Locale: "C", Timeline: 1, SystemID: "123456789",
				Streaming: role == "replica", EtcdHealthy: true,
				EtcdMemberID:   name + "-etcd",
				WALStatusKnown: true, SlotStatusKnown: true, ArchiveStatusKnown: true,
			}
			if role == "proxy" {
				primary := b.leader
				if primary == "" {
					primary = "demop0"
				}
				status.ProxyBackends = []pgspec.ProxyBackend{{Host: primary, Hostgroup: 10, Status: "ONLINE"}}
			}
			if override, ok := b.statusOverrides[name]; ok {
				status = override
			}
			if m != nil && m.Role == "data" {
				if b.sharedMemory == nil {
					b.sharedMemory = map[string]map[string]int64{}
				}
				if b.sharedMemory[name] == nil {
					options, _ := p.parseOptions(m.Options)
					b.sharedMemory[name] = fakeSharedMemory(options.Parameters)
				}
				if status.SharedMemoryParameters == nil {
					status.SharedMemoryParameters = b.sharedMemory[name]
				}
				if status.WALPosition == "" {
					status.WALPosition = "1/100"
				}
				if s.Rolling != nil && s.Rolling.Phase < len(s.Rolling.Phases) {
					options, _ := ParseOptions(s.Rolling.Phases[s.Rolling.Phase].Options)
					status.PendingRestart = !sharedMemoryMatches(status, options.Parameters)
				}
			}
			status.Paused = b.upgradePaused && !b.upgradePausePending[name]
			status.FailoverFenced = b.fenced[name]
			status.RetirementTagsCleared = !b.fenced[name]
			if status.Leader == "" {
				status.Leader = b.leader
				if status.Leader == "" {
					status.Leader = "demop0"
				}
			}
			if b.held[name] {
				status.Running, status.Streaming, status.Role = false, false, "stopped"
				status.RetirementStopped = true
			}
			json.NewEncoder(w).Encode(status)
		case "/v1/reload":
			b.reloads = append(b.reloads, name)
			if b.reloadFailures > 0 {
				b.reloadFailures--
				http.Error(w, "reload temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			if b.appliedVersions != nil {
				b.appliedVersions[name] = config
			}
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/postgres/retire-fence":
			if b.fenced == nil {
				b.fenced = map[string]bool{}
			}
			desired := retirementTagsDesired(s.find(name))
			leader := b.leader
			if leader == "" {
				leader = "demop0"
			}
			if desired && leader == name {
				t.Error("retirement tagged a primary and suppressed its member slots")
			}
			b.fenced[name] = desired
			action := "retire-unfence:"
			if desired {
				action = "retire-fence:"
			}
			b.actions = append(b.actions, action+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/retire-stop":
			leader := b.leader
			if leader == "" {
				leader = "demop0"
			}
			if leader == name || !b.fenced[name] {
				t.Error("retirement stopped an unfenced member or current primary")
			}
			if b.held == nil {
				b.held = map[string]bool{}
			}
			b.held[name] = true
			b.actions = append(b.actions, "retire-stop:"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/switchover":
			if m := s.find(name); m != nil && m.Retirement != "" {
				if b.fenced[name] {
					t.Error("retirement switched a primary before clearing nofailover")
				}
				b.actions = append(b.actions, "retire-switchover:"+name)
			}
			var request pgspec.SwitchoverRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if b.beforeSwitchover != nil {
				if diagnostic := b.beforeSwitchover(name, request.Candidate); diagnostic != "" {
					if b.plaintextSwitchover {
						http.Error(w, diagnostic, http.StatusConflict)
					} else {
						w.WriteHeader(http.StatusConflict)
						json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: diagnostic})
					}
					return
				}
			}
			b.leader = request.Candidate
			if b.leader == "" {
				for _, m := range s.data() {
					if m.Name != name {
						b.leader = m.Name
						break
					}
				}
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/etcd-member":
			var request pgspec.EtcdMemberRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if b.etcd == nil {
				b.etcd = map[string]pgspec.EtcdMember{}
				for _, m := range s.data() {
					if m.EtcdState != "pending" {
						b.etcd[m.Name+"-etcd"] = pgspec.EtcdMember{
							ID: m.Name + "-etcd", Name: m.Name,
							PeerURL: []string{"https://" + m.Name + ":2380"},
						}
					}
				}
			}
			b.actions = append(b.actions, request.Op+":"+request.Name+":"+request.MemberID)
			switch request.Op {
			case "add-learner":
				for _, peer := range b.etcd {
					if peer.Learner {
						t.Error("concurrent etcd learners")
					}
				}
				b.etcd[request.Name+"-etcd"] = pgspec.EtcdMember{
					ID: request.Name + "-etcd", Name: request.Name,
					PeerURL: []string{request.PeerURL}, Learner: true,
				}
			case "remove":
				leader := b.leader
				if leader == "" {
					leader = "demop0"
				}
				if request.MemberID == leader+"-etcd" {
					t.Error("removed current primary from etcd")
				}
				delete(b.etcd, request.MemberID)
			case "promote":
				peer := b.etcd[request.MemberID]
				peer.Learner = false
				b.etcd[request.MemberID] = peer
			}
			response := pgspec.EtcdMemberResponse{}
			var entries []string
			for _, peer := range b.etcd {
				response.Members = append(response.Members, peer)
				entries = append(entries, peer.Name+"="+peer.PeerURL[0])
			}
			response.InitialCluster = strings.Join(entries, ",")
			json.NewEncoder(w).Encode(response)
		case "/v1/postgres/upgrade-pause":
			b.upgradePaused = true
			b.actions = append(b.actions, "upgrade-pause:"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/upgrade-stop":
			if m := s.find(name); m == nil || !m.Offline {
				t.Error("upgrade stopped a member without durable offline intent")
			}
			if s.Topology == "ha" && !b.upgradePaused {
				t.Error("upgrade stopped a member before election pause")
			}
			b.actions = append(b.actions, "upgrade-stop:"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/upgrade-check", "/v1/postgres/upgrade-run":
			b.actions = append(b.actions, r.URL.Path+":"+name)
			json.NewEncoder(w).Encode(pgspec.UpgradeResponse{Checked: true, Done: true})
		case "/v1/processes/postgres/restart":
			b.actions = append(b.actions, r.URL.Path+":"+name)
			status := b.statusOverrides[name]
			status.PendingRestart = false
			b.statusOverrides[name] = status
			w.WriteHeader(http.StatusNoContent)
		case "/v1/postgres/restart":
			b.actions = append(b.actions, r.URL.Path+":"+name)
			var request pgspec.RestartRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.PrimaryFirst {
				b.actions = append(b.actions, "primary-first:"+name)
			}
			if s.Rolling != nil && s.Rolling.Phase < len(s.Rolling.Phases) {
				options, _ := ParseOptions(s.Rolling.Phases[s.Rolling.Phase].Options)
				b.sharedMemory[name] = fakeSharedMemory(options.Parameters)
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/dcs-reset":
			// The real reset atomically deletes the entire owned DCS scope,
			// including config/pause. Keep the fixture's cluster-wide pause in
			// sync with that native contract, not a later unowned resume action.
			b.upgradePaused = false
			b.actions = append(b.actions, r.URL.Path+":"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/reinit", "/v1/postgres/stanza-create", "/v1/postgres/stanza-upgrade":
			b.actions = append(b.actions, r.URL.Path+":"+name)
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, b
}
func converge(t *testing.T, p *Core, rc *engine.Context) *state {
	t.Helper()
	var lastStatus domain.ClusterStatus
	for range 100 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
		if rc.Cluster.Status.ObservedGeneration < rc.Cluster.Generation && rc.SyncWait == "" {
			t.Fatal("pending generation has no convergence barrier diagnostic")
		}
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		lastStatus = status
		if status.ObservedGeneration < rc.Cluster.Status.ObservedGeneration {
			status.ObservedGeneration = rc.Cluster.Status.ObservedGeneration
		}
		rc.Cluster.Status = status
		if status.Phase == domain.PhaseReady {
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
	}
	t.Fatalf("did not converge: %+v", lastStatus)
	return nil
}
func TestCreateSingleAndHA(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setup(t, topology)
			if err := p.Validate(context.Background(), nil, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			s := converge(t, p, rc)
			if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("Sync did not checkpoint the applied generation")
			}
			want := 1
			if topology == "ha" {
				want = 3
			}
			if len(s.data()) != want || len(b.services) != want*2+1 {
				t.Fatalf("unexpected services: %d members %d services", len(s.data()), len(b.services))
			}
			for _, spec := range b.imports {
				if spec.Kind == infra.KindVolume && spec.SpreadGroup != "demo" {
					t.Fatal("volume lacks cluster spread group")
				}
			}
		})
	}
}
func TestProxyEnableDisable(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 0
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	rc.Cluster.Generation++
	s := converge(t, p, rc)
	if b.services["demopx"] != nil || connectionMember(rc, s) != "demop0" {
		t.Fatal("proxy not disabled")
	}
	proxy.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	rc.Cluster.Generation++
	s = converge(t, p, rc)
	if s.find("demopx").Replicas != 2 || connectionMember(rc, s) != "demopx" {
		t.Fatal("proxy not enabled")
	}
}
func TestTopologyValidation(t *testing.T) {
	p, rc, _ := setup(t, "single")
	old := *rc.Cluster
	rc.Cluster.Spec.Topology.Type = "ha"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	if err := p.Validate(context.Background(), &old, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	old = *rc.Cluster
	rc.Cluster.Spec.Topology.Type = "single"
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	if err := p.Validate(context.Background(), &old, rc.Cluster); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("ha downgrade accepted")
	}
}
func TestOptions(t *testing.T) {
	for _, raw := range []string{`{"unknown":true}`, `{"synchronousMode":"maybe"}`, `{"parameters":{"archive_command":"anything"}}`, `{"parameters":{"ssl":"off"}}`, `{"proxy":{"unknown":true}}`, `null`, `{} {}`} {
		if _, err := ParseOptions(json.RawMessage(raw)); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted %s", raw)
		}
	}
	o, err := ParseOptions(json.RawMessage(`{"synchronousMode":"strict","parameters":{"shared_buffers":"256MB"},"proxy":{"readerPort":true}}`))
	if err != nil || o.SynchronousMode != "strict" || !o.Proxy.ReaderPort {
		t.Fatalf("valid options failed: %+v %v", o, err)
	}
}
func TestMemberConfigAndSecretIsolation(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"synchronousMode":"on","parameters":{"max_connections":"150"}}`)
	s := converge(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Etcd == nil || len(spec.Peers) != 3 || spec.Postgres.Synchronous != "on" || !reflect.DeepEqual(spec.Postgres.ClientCIDRs, []string{"0.0.0.0/0"}) || spec.Superuser.Password == "" || config.AgentTLS.KeyPEM == "" {
		t.Fatalf("bad member config: missing required fields")
	}
	for _, name := range systemNames {
		value, err := secret(context.Background(), rc, "users/"+name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(rc.Cluster.Internal), string(value)) {
			t.Fatal("secret in Internal")
		}
	}
	for _, m := range s.Members {
		token, err := rc.Secrets.GetAgentToken(context.Background(), rc.Cluster.Ref(), m.Name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(rc.Cluster.Internal), token.Token) {
			t.Fatal("agent token in Internal")
		}
	}
	if strings.Contains(string(rc.Cluster.Internal), "PRIVATE KEY") {
		t.Fatal("private key in Internal")
	}
	if _, err = p.MemberConfig(context.Background(), rc, "foreign"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal("foreign member configuration returned")
	}
}
func TestForeignOwnershipRejected(t *testing.T) {
	p, rc, b := setup(t, "single")
	b.services["demop0"] = &infra.Service{ID: "foreign", Hostname: "demop0", Ownership: map[string]string{}}
	if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("foreign ownership accepted: %v", err)
	}
	if b.services["demop0"].ID != "foreign" {
		t.Fatal("foreign service modified")
	}
}
