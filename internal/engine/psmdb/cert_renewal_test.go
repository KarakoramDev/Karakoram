// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type renewalAgent struct {
	version atomic.Int64
	running atomic.Bool
	reloads atomic.Int32
}

func renewalCluster(t *testing.T, replicas int) (*Provider, *engine.Context, map[string]*renewalAgent, *pki.CA) {
	t.Helper()
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = replicas
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "renewal"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
		m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
		m.Disk = component(rc.Cluster.Spec, m).Resources
		m.Containers = 1
		m.PublicHash = hash(rc.Cluster.Spec.Exposure)
	}
	for _, set := range s.Sets {
		set.Stage = "ready"
		set.Added = true
	}
	if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	agents := map[string]*renewalAgent{}
	dialer := memberDialer{}
	for _, m := range s.Members {
		a := &renewalAgent{}
		a.running.Store(true)
		a.version.Store(m.ConfigVersion)
		agents[m.Name] = a
		dialer[m.Name] = responder(t, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/status":
				state := "stopped"
				if a.running.Load() {
					state = "running"
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: a.version.Load(), Processes: map[string]agentapi.ProcessStatus{"mongod": {State: state}}})
			case "/v1/reload":
				a.reloads.Add(1)
				_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			default:
				http.NotFound(w, r)
			}
		})
	}
	rc.Agents = dialer
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// Establish the normal desired-config hash without any rolling work.
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		activateConfig(rc.Cluster, s, m)
		agents[m.Name].version.Store(m.ConfigVersion)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	crt, err := secret(context.Background(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := secret(context.Background(), rc, "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		t.Fatal(err)
	}
	return p, rc, agents, ca
}

func TestCertificateRenewalRollsAfterDisruptionAndRecovery(t *testing.T) {
	p, rc, agents, ca := renewalCluster(t, 3)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	original := map[string][]byte{}
	for _, m := range s.Members {
		for _, kind := range []string{"members", "agents"} {
			crt, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Validity: 24 * time.Hour, Server: true, Client: kind == "members"})
			if err != nil {
				t.Fatal(err)
			}
			name := "tls/" + kind + "/" + m.Name + ".pem"
			original[name] = append(crt, key...)
			if err := putSecret(ctx, rc, name, original[name]); err != nil {
				t.Fatal(err)
			}
		}
	}
	blocked := errors.New("no healthy voting majority")
	gateError := blocked
	stepped := false
	gateCalls := 0
	renewed := map[string]bool{}
	assertPublished := func() {
		t.Helper()
		for _, m := range s.Members {
			for _, kind := range []string{"members", "agents"} {
				name := "tls/" + kind + "/" + m.Name + ".pem"
				got, err := secret(ctx, rc, name)
				if err != nil {
					t.Fatal(err)
				}
				if changed := !bytes.Equal(got, original[name]); changed != renewed[m.Name] {
					t.Fatalf("%s changed=%v, approved=%v", name, changed, renewed[m.Name])
				}
			}
			config, err := p.MemberConfig(ctx, rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			if changed := !bytes.Equal([]byte(config.TLS.CertPEM+config.TLS.KeyPEM), original["tls/members/"+m.Name+".pem"]); changed != renewed[m.Name] {
				t.Fatalf("MemberConfig exposed unapproved renewal for %s", m.Name)
			}
		}
	}
	prepare := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (bool, error) {
		gateCalls++
		assertPublished()
		if renewed[m.Name] {
			t.Fatalf("disrupting already renewed member %s", m.Name)
		}
		return stepped, gateError
	}
	recovered := false
	replication := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (mongoadmin.Status, error) {
		state := "RECOVERING"
		if recovered {
			state = "SECONDARY"
		}
		return mongoadmin.Status{Members: []mongoadmin.Member{{Name: m.Name + ":27017", Health: 1, State: state, OptimeDate: time.Now()}}}, nil
	}
	syncCluster := func() error {
		_, err := p.sync(ctx, rc, prepare, replication)
		return err
	}
	if err := syncCluster(); !errors.Is(err, blocked) {
		t.Fatalf("disruption refusal not propagated: %v", err)
	}
	assertPublished()
	gateError = nil
	stepped = true
	if err := syncCluster(); err != nil {
		t.Fatal(err)
	}
	assertPublished()
	stepped = false
	for _, m := range s.Members {
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		assertPublished() // recording intent alone must not expose the certificate
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		renewed[m.Name] = true
		assertPublished()
		if agents[m.Name].reloads.Load() != 1 {
			t.Fatalf("reload count for %s: %d", m.Name, agents[m.Name].reloads.Load())
		}
		calls := gateCalls
		// No acknowledgement yet: another voter must not be published.
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		if gateCalls != calls {
			t.Fatal("disruption attempted before config acknowledgement")
		}
		current, _ := load(rc.Cluster)
		agents[m.Name].version.Store(current.find(m.Name).ConfigVersion)
		agents[m.Name].running.Store(false)
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		agents[m.Name].running.Store(true)
		// Process running and config acknowledged still do not prove recovery.
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		if gateCalls != calls {
			t.Fatal("disruption attempted before replication recovered")
		}
		assertPublished()
		recovered = true
		if err := syncCluster(); err != nil {
			t.Fatal(err)
		}
		current, _ = load(rc.Cluster)
		if current.find(m.Name).Stage != "active" {
			t.Fatalf("member %s did not leave recovery: %s", m.Name, current.find(m.Name).Stage)
		}
		recovered = false
		assertPublished()
	}
	if err := syncCluster(); err != nil {
		t.Fatal(err)
	}
	current, _ := load(rc.Cluster)
	if current.Phase != "ready" {
		t.Fatalf("renewal did not converge: %s", current.Phase)
	}
}

func TestProvidedNearExpiryCertificateRemainsStableAndReportsExpiry(t *testing.T) {
	p, rc, agents, ca := renewalCluster(t, 3)
	ctx := context.Background()
	rc.Cluster.Spec.TLS.CA = "userProvided"
	s, _ := load(rc.Cluster)
	names := []string{}
	for _, m := range s.Members {
		names = append(names, m.Name)
	}
	crt, key, err := ca.Issue(pki.CertRequest{CommonName: "supplied", DNSNames: names, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Validity: 24 * time.Hour, Server: true, Client: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string][]byte{"tls/user.crt": crt, "tls/user.key": key} {
		if err := putSecret(ctx, rc, name, value); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range s.Members {
		if err := applyProvidedCertificate(ctx, rc, m); err != nil {
			t.Fatal(err)
		}
	}
	// Apply the material change once, then measure only repeated renew-window Syncs.
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	for _, m := range s.Members {
		activateConfig(rc.Cluster, s, m)
		agents[m.Name].version.Store(m.ConfigVersion)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before := s.ConfigVersion
	for range 4 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		s, _ = load(rc.Cluster)
		if s.ConfigVersion != before || s.Phase != "ready" {
			t.Fatalf("supplied leaf caused renewal loop: version=%d want=%d phase=%s", s.ConfigVersion, before, s.Phase)
		}
		for _, m := range s.Members {
			got, err := secret(ctx, rc, "tls/members/"+m.Name+".pem")
			if err != nil || !bytes.Equal(got, append(crt, key...)) {
				t.Fatalf("supplied certificate replaced for %s: %v", m.Name, err)
			}
			if agents[m.Name].reloads.Load() != 0 {
				t.Fatal("supplied certificate caused a reload")
			}
		}
	}
	// Status can report the PKI warning even when replication is not observed.
	for _, set := range s.Sets {
		set.Stage = "planned"
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range status.Conditions {
		if condition.Type == "TLSCertificate" {
			if condition.Status != "False" || condition.Reason != "ExpiringSoon" || condition.Message == "" {
				t.Fatalf("wrong expiry condition: %+v", condition)
			}
			return
		}
	}
	t.Fatal("missing provided certificate expiry condition")
}

func TestMemberSecretCleanupRemovesStagedRenewals(t *testing.T) {
	_, rc, _, _ := setup(t)
	ctx := context.Background()
	for _, kind := range []string{"members", "agents"} {
		if err := putSecret(ctx, rc, "tls/"+kind+"/demom0.pending.pem", []byte("pending")); err != nil {
			t.Fatal(err)
		}
	}
	if err := deleteMemberSecrets(ctx, rc, "demom0"); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"members", "agents"} {
		_, err := secret(ctx, rc, "tls/"+kind+"/demom0.pending.pem")
		if !missing(err) {
			t.Fatalf("staged %s certificate retained: %v", kind, err)
		}
	}
}

func TestProvidedCAWithoutLeafStillRenewsManagedCertificates(t *testing.T) {
	p, rc, _, ca := renewalCluster(t, 1)
	ctx := context.Background()
	rc.Cluster.Spec.TLS.CA = "userProvided"
	s, _ := load(rc.Cluster)
	m := s.Members[0]
	crt, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name}, Validity: time.Hour, Server: true, Client: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := putSecret(ctx, rc, "tls/members/"+m.Name+".pem", append(crt, key...)); err != nil {
		t.Fatal(err)
	}
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	pending, err := secret(ctx, rc, "tls/members/"+m.Name+".pending.pem")
	if err != nil {
		t.Fatal(err)
	}
	if renew, err := pki.NeedsRenewal(pending, 30*24*time.Hour, time.Now()); err != nil || renew {
		t.Fatalf("managed leaf under provided CA not renewed: %v", err)
	}
	condition, err := providedCertificateCondition(ctx, rc)
	if err != nil || condition != nil {
		t.Fatalf("warning reported for nonexistent supplied leaf: %+v %v", condition, err)
	}
}
