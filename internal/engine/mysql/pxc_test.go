// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net/http"
	"strings"
	"testing"
)

func setupPXC(t *testing.T) (*Core, *engine.Context, *fakeBackend) {
	t.Helper()
	_, rc, b := setup(t, "group")
	p := New(Options{Provider: "pxc"})
	rc.Cluster.Spec.Provider, rc.Cluster.Spec.Version, rc.Cluster.Spec.Topology.Type = "pxc", pxcCatalog[0].Version, "cluster"
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		switch r.URL.Path {
		case "/v1/status":
			pid := 1
			if b.native[name].MemberState != "ONLINE" {
				pid = 0
			}
			json.NewEncoder(w).Encode(agentapi.Status{Agent: b.agentBuild, ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}}})
			return true
		case "/v1/mysql/galera-status":
			st := b.native[name]
			size := 0
			for _, peer := range b.native {
				if peer.MemberState == "ONLINE" {
					size++
				}
			}
			out := myspec.GaleraStatusResponse{ClusterStatus: "non-Primary", StateUUID: "11111111-2222-3333-4444-555555555555", LastCommitted: 10, ClusterSize: size, GTIDExecuted: st.GTIDExecuted}
			if st.MemberState == "ONLINE" {
				out.ClusterStatus, out.LocalStateComment, out.Ready, out.Connected = "Primary", "Synced", true, true
			}
			json.NewEncoder(w).Encode(out)
			return true
		case "/v1/mysql/proxy-status":
			cfg, err := p.MemberConfig(r.Context(), rc, name)
			if err != nil {
				t.Error(err)
				http.Error(w, "config unavailable", http.StatusServiceUnavailable)
				return true
			}
			var spec myspec.Spec
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil || spec.Proxy == nil {
				t.Error("invalid proxy config", err)
				http.Error(w, "invalid proxy config", http.StatusServiceUnavailable)
				return true
			}
			var status myspec.ProxyStatusResponse
			for _, peer := range spec.Proxy.Backends {
				if peer.Host == b.leader && b.native[peer.Host].MemberState == "ONLINE" {
					status.Backends = append(status.Backends, myspec.ProxyBackend{Hostgroup: myspec.ProxyWriterGroup, Host: peer.Host, Port: peer.Port, Status: "ONLINE"})
				}
			}
			json.NewEncoder(w).Encode(status)
			return true
		case "/v1/mysql/galera-bootstrap":
			var req myspec.GaleraBootstrapRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			current, err := load(rc.Cluster)
			if err != nil || req.ClusterName != current.Formation.GroupName {
				t.Error("bootstrap identity differs from durable Galera history", req.ClusterName, err)
			}
			if !b.bootstrapReceipts[req.FormationEpoch] {
				b.bootstrapReceipts[req.FormationEpoch] = true
				b.bootstrap[req.FormationEpoch]++
				b.leader = name
				st := b.native[name]
				st.MemberState, st.MemberRole, st.ReadOnly, st.SuperReadOnly = "ONLINE", "PRIMARY", false, false
				st.DataIdentity.FormationEpoch = req.FormationEpoch
				b.native[name] = st
			}
			json.NewEncoder(w).Encode(struct{}{})
			return true
		case "/v1/mysql/wsrep-recover":
			json.NewEncoder(w).Encode(myspec.WSRepRecoverResponse{UUID: "11111111-2222-3333-4444-555555555555", Seqno: 10, Stopped: true})
			return true
		case "/v1/mysql/bootstrap", "/v1/mysql/clone", "/v1/mysql/start-group", "/v1/mysql/stop-group", "/v1/mysql/switchover", "/v1/mysql/offline-gtid":
			t.Errorf("PXC invoked a Group Replication route %s", r.URL.Path)
			http.Error(w, "forbidden", 500)
			return true
		}
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") && strings.HasSuffix(r.URL.Path, "/stop") {
			st := b.native[name]
			st.MemberState = "OFFLINE"
			b.native[name] = st
		}
		return false
	}
	return p, rc, b
}

func TestPXCProviderCatalogAndValidation(t *testing.T) {
	p := New(Options{Provider: "pxc"})
	c := &domain.Cluster{Name: "sample"}
	p.ApplyDefaults(&c.Spec)
	if c.Spec.Provider != "pxc" || c.Spec.Topology.Type != "cluster" || c.Spec.Components.Engine.Replicas != 3 {
		t.Fatal(c.Spec)
	}
	if err := p.Validate(t.Context(), nil, c); err != nil {
		t.Fatal(err)
	}
	info := p.Describe()
	if info.Name != "pxc" || len(info.Topologies) != 1 || info.Topologies[0].Type != "cluster" {
		t.Fatal(info)
	}
	for _, v := range info.Versions {
		server, _, err := p.artifactPins(v.Version)
		if err != nil || !strings.HasPrefix(server, "percona-xtradb-cluster-") {
			t.Fatal(server, err)
		}
	}
	for _, topology := range []string{"single", "group"} {
		copy := *c
		copy.Spec.Topology.Type = topology
		if p.Validate(t.Context(), nil, &copy) == nil {
			t.Fatal("accepted topology", topology)
		}
	}
	for _, n := range []int{1, 2, 4} {
		copy := *c
		copy.Spec.Components.Engine.Replicas = n
		if p.Validate(t.Context(), nil, &copy) == nil {
			t.Fatal("accepted replicas", n)
		}
	}
	c.Spec.EngineOptions = json.RawMessage(`{"groupReplication":{}}`)
	if p.Validate(t.Context(), nil, c) == nil {
		t.Fatal("accepted GR options")
	}
}

func TestPXCResourceMemoryFloor(t *testing.T) {
	for _, ram := range []float64{1, 1.5, 2} {
		p := New(Options{Provider: "pxc"})
		c := &domain.Cluster{Name: "sample"}
		p.ApplyDefaults(&c.Spec)
		c.Spec.Components.Engine.Resources.MinRAM = ram
		// An elastic maximum must not bypass the guaranteed-memory floor.
		c.Spec.Components.Engine.Resources.MaxRAM = 4
		for _, parameters := range []string{`{}`, `{"parameters":{"innodb_buffer_pool_size":"134217728","max_connections":"30"}}`} {
			c.Spec.EngineOptions = json.RawMessage(parameters)
			for _, old := range []*domain.Cluster{nil, c} {
				err := p.Validate(t.Context(), old, c)
				if ram < 2 {
					if err == nil || !strings.Contains(err.Error(), "PXC data members require at least 2 GiB RAM") {
						t.Fatalf("RAM %g: expected PXC memory floor, got %v", ram, err)
					}
				} else if err != nil {
					t.Fatalf("RAM %g: %v", ram, err)
				}
			}
		}
	}
}

func TestMySQLMinimumRAMAndRenderedHeadroom(t *testing.T) {
	for _, topology := range []string{"single", "group", "cluster"} {
		for _, ram := range []float64{1, 2} {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if topology == "cluster" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, topology)
			}
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM = ram
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
			err := p.Validate(t.Context(), nil, rc.Cluster)
			if topology == "cluster" && ram == 1 {
				if err == nil {
					t.Fatal("admitted unsafe 1 GiB PXC")
				}
				continue
			}
			if err != nil {
				t.Fatal(topology, ram, err)
			}
			converge(t, p, rc, b)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range s.data() {
				config, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec myspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				limit := int64(ram * (1 << 30))
				if spec.BufferPoolBytes != limit/2 || spec.MaxConnections != 200 {
					t.Fatalf("%s RAM %g: unexpected memory defaults: %+v", topology, ram, spec)
				}
				// This is explicit cache headroom, not a bound on native RSS:
				// connection buffers, PS and replication allocate dynamically.
				caches := spec.BufferPoolBytes
				if topology == "cluster" {
					if spec.Galera == nil || spec.Galera.GCacheSize != "128M" {
						t.Fatal("missing Galera cache budget")
					}
					caches += 128 << 20
				}
				if limit-caches < limit*3/8 {
					t.Fatalf("%s RAM %g: explicit caches leave less than 37.5%% headroom", topology, ram)
				}
			}
		}
	}
}
func TestPXCFormationReplayAndNativeConfig(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	if s.Formation.State != "formed" || b.bootstrap[1] != 1 || len(b.clones) != 0 {
		t.Fatal(s.Formation, b.bootstrap, b.clones)
	}
	for _, m := range s.Members {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var sp myspec.Spec
		json.Unmarshal(cfg.EngineSpec, &sp)
		if m.Role == "data" && (sp.Galera == nil || sp.Galera.ClusterName != s.Formation.GroupName || sp.GroupReplication != nil || !strings.Contains(sp.ServerDir, "percona-xtradb-cluster-")) {
			t.Fatal(sp)
		}
		if m.Role == "proxy" && sp.Proxy.Mode != "galera" {
			t.Fatal(sp.Proxy)
		}
	}
	b.leader = "demom2"
	for _, m := range s.data() {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil || onlinePrimary(st) != (m.Name == b.leader) {
			t.Fatal(m.Name, st, err)
		}
	}
}
func TestPXCRecoveredPositionSelectionFailsClosed(t *testing.T) {
	uuid := "11111111-2222-3333-4444-555555555555"
	observations := map[string]myspec.WSRepRecoverResponse{"m0": {UUID: uuid, Seqno: 4, Stopped: true}, "m1": {UUID: uuid, Seqno: 9, Stopped: true}, "m2": {UUID: uuid, Seqno: 7, Stopped: true}}
	seed, position, err := selectGaleraPosition(observations)
	if err != nil || seed != "m1" || position != uuid+":9" {
		t.Fatal(seed, position, err)
	}
	for _, invalid := range []myspec.WSRepRecoverResponse{{UUID: uuid, Seqno: -1, Stopped: true}, {UUID: "22222222-2222-3333-4444-555555555555", Seqno: 20, Stopped: true}, {UUID: uuid, Seqno: 20}} {
		observations["m2"] = invalid
		if _, _, err := selectGaleraPosition(observations); err == nil {
			t.Fatal("accepted invalid recovered position", invalid)
		}
	}
}
func TestPXCScaleOutUsesSSTAndTLSRotation(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	day2Ticks(t, rc, b, func(s *state) bool {
		if len(s.data()) != 5 {
			return false
		}
		for _, m := range s.Members {
			if m.Stage != "joined" {
				return false
			}
		}
		return true
	})
	if len(b.clones) != 0 {
		t.Fatal("PXC cloned through GR", b.clones)
	}
	runDay2Operation(t, rc, b, &domain.Operation{ID: "pxc-tls", Kind: domain.OperationRotateTLS})
}
func TestPXCRestoreRejectsMySQLProvider(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	backup := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "foreign", Provider: "mysql", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, MySQL: &domain.MySQLBackup{Label: "foreign"}}
	if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: backup.Namespace, Cluster: backup.Cluster, Backup: backup.Name}); err == nil {
		t.Fatal("accepted cross-provider physical restore")
	}
}
func TestPXCRecoveryRequiresExplicitFencingConfirmation(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	_, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "recover", Kind: "recover-quorum", Payload: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("quorum recovery accepted without explicit confirmation")
	}
}

func TestPXCAccessUsesRoutedWriter(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	b.leader = "demom2"
	previous := b.action
	called := ""
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/"+myspec.ActionAccessUsers {
			called = name
			json.NewEncoder(w).Encode([]User{})
			return true
		}
		return previous(w, r, name)
	}
	if _, err := p.ListUsers(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if called != b.leader {
		t.Fatal("access did not use routed writer", called, b.leader)
	}
}

func TestPXCRestorePreparesSSTJoinersBeforeBootstrap(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	s.Formation.FormationEpoch++
	s.Formation.GroupName = "aaaaaaaabbbb4ccc8dddeeeeeeeeeeee"
	s.Formation.State = "bootstrapping"
	s.Formation.Reformation = &reformation{State: "bootstrapping"}
	for _, m := range s.data() {
		m.Offline = m.Name != s.Formation.Seed
		m.Initialized = false
		m.Stage = "deployed"
		st := b.native[m.Name]
		st.MemberState = "OFFLINE"
		b.native[m.Name] = st
	}
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.apply(t, p, rc)
	previous := b.action
	prepared := map[string]bool{}
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraPrepareJoin {
			var req myspec.GaleraBootstrapRequest
			json.NewDecoder(r.Body).Decode(&req)
			if req.ClusterName != s.Formation.GroupName || req.FormationEpoch != s.Formation.FormationEpoch || req.StartPosition != "" || name == s.Formation.Seed {
				t.Error("invalid restore SST authorization", name, req)
			}
			prepared[name] = true
			json.NewEncoder(w).Encode(struct{}{})
			return true
		}
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraBootstrap && len(prepared) != 2 {
			t.Error("bootstrap preceded joiner reset")
		}
		return previous(w, r, name)
	}
	cp := backupCheckpoint{Step: "reform", Member: s.Formation.Seed, Request: &myspec.Restore{NewGroupName: s.Formation.GroupName}}
	op := &domain.Operation{ID: "pxc-restore", Kind: domain.OperationRestore}
	if _, err := p.runRestore(t.Context(), rc, op, cp); err != nil {
		t.Fatal(err)
	}
	if len(prepared) != 2 {
		t.Fatal("restore did not authorize SST for every nonseed", prepared)
	}
	if len(b.clones) != 0 {
		t.Fatal("restore invoked clone instead of SST")
	}
	if _, err := p.runRestore(t.Context(), rc, op, cp); err != nil {
		t.Fatal(err)
	}
	current, err := load(rc.Cluster)
	if err != nil || current.Formation.State != "formed" || current.find(current.Formation.Seed).Stage != "joined" {
		t.Fatal("native seed formation waited for an empty proxy", current, err)
	}
	for _, m := range current.data() {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		json.Unmarshal(cfg.EngineSpec, &spec)
		if spec.Galera.ClusterName != cp.Request.NewGroupName {
			t.Fatal("restored config lost new Galera history", spec.Galera)
		}
	}
}

func TestPXCResumeRecoversEveryStoppedMemberAndPinsSeedPosition(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	s.Formation.Reformation = &reformation{State: "observing"}
	for _, m := range s.data() {
		m.Offline = true
	}
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.apply(t, p, rc)
	if _, err := p.reconcileGaleraFormation(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Formation.Reformation.State != "bootstrapping" || len(s.Formation.Reformation.GaleraObservations) != 3 {
		t.Fatal(s.Formation)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.data() {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		json.Unmarshal(cfg.EngineSpec, &spec)
		if (spec.Galera.StartPosition != "") != (m.Name == s.Formation.Seed) {
			t.Fatal("highest recovered position leaked to joiner", m.Name, spec.Galera)
		}
	}
}

func TestPXCScaleInReplacementAndResourceRollAvoidGR(t *testing.T) {
	p, rc, b := setupPXC(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	day2Ticks(t, rc, b, func(s *state) bool { return len(s.data()) == 3 && s.Pending == nil })
	old := b.leader
	runDay2Operation(t, rc, b, &domain.Operation{ID: "pxc-replace", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + old + `"}`)})
	s, _ := load(rc.Cluster)
	if s.find(old) != nil || len(s.data()) != 3 || len(b.clones) != 0 {
		t.Fatal(s, b.clones)
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 2
	day2Ticks(t, rc, b, func(s *state) bool {
		if s.Rolling != nil {
			return false
		}
		for _, m := range s.data() {
			if m.Resources.MaxCPU != 2 {
				return false
			}
		}
		return true
	})
}

func TestPXCQuorumRecoveryFencesMissingRuntimesBeforeMutation(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	previous := b.action
	forced, forceCalls := false, 0
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus {
			if name != "demom0" {
				http.Error(w, "missing", http.StatusServiceUnavailable)
				return true
			}
			st := myspec.GaleraStatusResponse{ClusterStatus: "non-Primary", LocalStateComment: "Synced", Ready: false, Connected: true, StateUUID: "11111111-2222-3333-4444-555555555555", LastCommitted: 42}
			if forced {
				st.ClusterStatus, st.Ready = "Primary", true
			}
			json.NewEncoder(w).Encode(st)
			return true
		}
		if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraForcePrimary {
			for _, peer := range []string{"demom1", "demom2"} {
				if b.services[peer].Status != infra.StatusStopped {
					t.Error("force-primary before missing runtime fence", peer)
				}
			}
			if name != "demom0" {
				t.Error("wrong minority recovery seed", name)
			}
			forced = true
			forceCalls++
			json.NewEncoder(w).Encode(struct{}{})
			return true
		}
		return previous(w, r, name)
	}
	op := &domain.Operation{ID: "recover-fenced", Kind: "recover-quorum", Payload: json.RawMessage(`{"confirmation":"all-absent-members-fenced"}`)}
	done := false
	for range 20 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Step == "observe" && forceCalls == 1 && !result.Done {
			replay, err := p.RunOperation(t.Context(), rc, op)
			if err != nil || replay.Step != "observe" {
				t.Fatal(replay, err)
			}
		}
		op.Checkpoint = result.Checkpoint
		p = New(Options{Provider: "pxc"})
		if result.Done {
			done = true
			break
		}
	}
	if !done || forceCalls != 1 {
		t.Fatal("fenced recovery did not complete exactly once", done, forceCalls)
	}
}
