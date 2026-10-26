// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"strings"
	"testing"
)

func TestCreateSingleConnection(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	conn, err := p.Connection(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(conn.Connection.Hosts) != 1 || conn.Connection.Hosts[0] != "demom0:3306" || len(conn.Credentials) != 1 || conn.Credentials[0].Username != "admin" || conn.Credentials[0].Password == "" || conn.Credentials[0].Database != "" {
		t.Fatalf("unexpected connection %+v", conn)
	}
	if strings.Contains(conn.Connection.URI, conn.Credentials[0].Password) {
		t.Fatal("discovery leaked password")
	}
	s, _ := load(rc.Cluster)
	cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	var sp myspec.Spec
	json.Unmarshal(cfg.EngineSpec, &sp)
	if sp.GroupReplication != nil || sp.AllowInitialize {
		t.Fatal("formed singleton has bootstrap authorization")
	}
	if err = sp.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGroupPeerAddressesMatchIssuedSQLIdentities(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range s.data() {
		cfg, err := p.MemberConfig(t.Context(), rc, member.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(spec.CAPEM)) {
			t.Fatal("invalid peer CA")
		}
		leaf := trafficCertificate(t, []byte(spec.ServerCert.CertPEM))
		for _, peer := range spec.Peers {
			if peer.Name != member.Name {
				continue
			}
			if peer.Host != member.Name {
				t.Fatalf("group address %q differs from report_host %q", peer.Host, member.Name)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: peer.Host, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				t.Fatalf("member %s peer identity: %v", member.Name, err)
			}
		}
	}
}

func TestCreateGroupReplayEveryCheckpoint(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.Formation.State != "formed" || len(s.data()) != 3 || s.find("demomx") == nil {
		t.Fatalf("unexpected formation %+v", s)
	}
	if b.bootstrap[1] != 1 {
		t.Fatalf("bootstrap count %v", b.bootstrap)
	}
	for _, name := range []string{"demom1", "demom2"} {
		if b.clones[name] != "demom0" {
			t.Fatalf("clone %s donor %q", name, b.clones[name])
		}
	}
	for range 5 {
		p = New(Options{})
		if _, err = p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	if b.bootstrap[1] != 1 {
		t.Fatal("replayed formation bootstrapped again")
	}
	cfg, err := p.MemberConfig(t.Context(), rc, "demomx")
	if err != nil {
		t.Fatal(err)
	}
	var sp myspec.Spec
	json.Unmarshal(cfg.EngineSpec, &sp)
	if err = sp.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(sp.Proxy.Backends) != 3 || sp.Proxy.Users[0].Username != "admin" {
		t.Fatalf("proxy %+v", sp.Proxy)
	}
}
func TestDeleteCluster(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	done := false
	for range 40 {
		var err error
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done || len(b.services) != 0 {
		t.Fatalf("cleanup done=%v services=%v", done, b.services)
	}
	if _, err := secret(t.Context(), rc, "users/root"); !missing(err) {
		t.Fatalf("root secret retained: %v", err)
	}
}
func suspendGroup(t *testing.T, p *Core, rc *engine.Context, b *fakeBackend) {
	t.Helper()
	done := false
	for range 40 {
		var err error
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("suspend did not finish")
	}
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		if b.services[m.Volume] == nil {
			t.Fatal("lost retained volume")
		}
		st := b.native[m.Name]
		st.MemberState = "OFFLINE"
		b.native[m.Name] = st
		b.offline[m.Name] = myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: st.ServerUUID, GTIDExecuted: sid + ":1-3", DataIdentity: myspec.DataIdentity{Initialized: true}}
	}
	b.leader = ""
}
func TestResumeGroupSelectsSuperset(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	suspendGroup(t, p, rc, b)
	out := b.offline["demom2"]
	out.GTIDExecuted = sid + ":1-9"
	b.offline["demom2"] = out
	converge(t, New(Options{}), rc, b)
	s, _ := load(rc.Cluster)
	if s.Formation.FormationEpoch != 2 || s.Formation.Seed != "demom2" || s.Formation.State != "formed" || b.bootstrap[2] != 1 {
		t.Fatalf("formation=%+v bootstraps=%v", s.Formation, b.bootstrap)
	}
}
func TestResumeGroupIncomparableBlocked(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	suspendGroup(t, p, rc, b)
	out := b.offline["demom2"]
	out.GTIDExecuted = sid2 + ":1-3"
	b.offline["demom2"] = out
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, _ := load(rc.Cluster)
	if s.Formation.FormationEpoch != 1 || b.bootstrap[2] != 0 {
		t.Fatal("unsafe bootstrap")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	blocked := false
	for _, c := range status.Conditions {
		blocked = blocked || c.Type == "GroupReformationBlocked" && c.Status == "True"
	}
	if !blocked {
		t.Fatalf("missing blocking condition %+v", status.Conditions)
	}
}
func TestResumeUnobservableBlocked(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	suspendGroup(t, p, rc, b)
	out := b.offline["demom1"]
	out.Stopped = false
	b.offline["demom1"] = out
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, _ := load(rc.Cluster)
	if s.Formation.Reformation.Blocked == "" || s.Formation.FormationEpoch != 1 {
		t.Fatal("unobservable member did not fence bootstrap")
	}
}
func TestRequiredCapabilities(t *testing.T) {
	p := New(Options{})
	var spec domain.ClusterSpec
	p.ApplyDefaults(&spec)
	if len(p.RequiredCapabilities(spec)) != 0 {
		t.Fatal("singleton proxy should not require scaling")
	}
	proxy := spec.Components.Extra["proxy"]
	proxy.Replicas = 2
	spec.Components.Extra["proxy"] = proxy
	if got := p.RequiredCapabilities(spec); len(got) != 1 || got[0] != "horizontalScaling" {
		t.Fatal(got)
	}
}
func TestUnsupportedOperations(t *testing.T) {
	p, rc, _ := setup(t, "single")
	for _, kind := range []string{"scale", "upgrade", "unknown"} {
		if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: kind}); !errors.Is(err, engine.ErrNotSupported) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestRestartCheckpoints(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	op := &domain.Operation{ID: "restart-1", Kind: domain.OperationRestart}
	for range 30 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Done {
			return
		}
		op.Checkpoint = result.Checkpoint
		p = New(Options{})
	}
	t.Fatal("restart did not converge")
}

func TestFormationCrashAfterNativeMutationBeforeStateSave(t *testing.T) {
	p, rc, b := setup(t, "group")
	crashedBootstrap, crashedClone := false, false
	for range 120 {
		before := append(json.RawMessage(nil), rc.Cluster.Internal...)
		bootstraps, clones := b.bootstrap[1], len(b.clones)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if !crashedBootstrap && b.bootstrap[1] > bootstraps {
			rc.Cluster.Internal = before
			crashedBootstrap = true
		}
		if !crashedClone && len(b.clones) > clones {
			rc.Cluster.Internal = before
			crashedClone = true
		}
		b.apply(t, p, rc)
		p = New(Options{})
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			if !crashedBootstrap || !crashedClone || b.bootstrap[1] != 1 {
				t.Fatalf("crash coverage bootstrap=%v clone=%v count=%v", crashedBootstrap, crashedClone, b.bootstrap)
			}
			return
		}
	}
	t.Fatal("crash replay did not converge")
}

func TestBootstrapConsumedEpochFailsClosed(t *testing.T) {
	p, rc, b := setup(t, "group")
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		s, _ := load(rc.Cluster)
		if s.Formation.State != "bootstrapping" {
			continue
		}
		status := b.native[s.Formation.Seed]
		status.DataIdentity.FormationEpoch = s.Formation.FormationEpoch
		b.native[s.Formation.Seed] = status
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(rc.Cluster)
		if s.Formation.Blocked == "" || b.bootstrap[1] != 0 {
			t.Fatal("consumed bootstrap epoch was replayed")
		}
		return
	}
	t.Fatal("bootstrap publication checkpoint not reached")
}

func TestFullGroupOutageRequiresOfflineSuperset(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	for name, status := range b.native {
		status.MemberState = "OFFLINE"
		b.native[name] = status
		b.offline[name] = myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: status.ServerUUID, GTIDExecuted: sid + ":1-3", DataIdentity: myspec.DataIdentity{Initialized: true}}
	}
	latest := b.offline["demom1"]
	latest.GTIDExecuted = sid + ":1-9"
	b.offline["demom1"] = latest
	b.leader = ""
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	if s.Formation.Seed != "demom1" || s.Formation.FormationEpoch != 2 || b.bootstrap[2] != 1 {
		t.Fatalf("formation %+v", s.Formation)
	}
}
