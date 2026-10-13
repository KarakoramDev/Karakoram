// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func recoveredRenewalMember(_ context.Context, _ *engine.Context, _ *internalState, m *member) (mongoadmin.Status, error) {
	return mongoadmin.Status{Members: []mongoadmin.Member{{Name: m.Name + ":27017", State: "SECONDARY", Health: 1, OptimeDate: time.Now()}}}, nil
}

func TestReloadWaitingMemberAcceptsCorrectedEngineConfig(t *testing.T) {
	p, rc, agents, _ := renewalCluster(t, 3)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	first, second := s.Members[0].Name, s.Members[1].Name
	rc.Cluster.Spec.EngineConfig = "setParameter:\n  nonexistentKarakoramParameter: true\n"
	if err := p.Validate(ctx, nil, rc.Cluster); err != nil {
		t.Fatalf("fixture must pass override validation: %v", err)
	}
	gates := []string{}
	prepare := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (bool, error) {
		gates = append(gates, m.Name)
		if m.Name != first {
			t.Fatalf("another member disrupted before repair: %s", m.Name)
		}
		return false, nil
	}
	syncCluster := func() {
		t.Helper()
		if _, err := p.sync(ctx, rc, prepare, recoveredRenewalMember); err != nil {
			t.Fatal(err)
		}
	}
	syncCluster() // desired configuration version
	syncCluster() // durable publication intent
	syncCluster() // first member applies the configuration
	s, _ = load(rc.Cluster)
	badVersion := s.find(first).ConfigVersion
	if s.find(first).Stage != "reload-wait" {
		t.Fatal("member did not enter reload-wait")
	}
	// Model mongod rejecting the unknown parameter after the agent applies it.
	agents[first].version.Store(badVersion)
	agents[first].running.Store(false)
	syncCluster()
	beforeRepairGates := len(gates)
	rc.Cluster.Spec.EngineConfig = ""
	syncCluster() // repair intent, even though the waiting process is stopped
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "reload-publish" || s.ConfigVersion <= badVersion {
		t.Fatalf("repair not scheduled: stage=%s version=%d", s.find(first).Stage, s.ConfigVersion)
	}
	if len(gates) != beforeRepairGates {
		t.Fatal("repair intent performed a disruption before returning")
	}
	syncCluster() // publish the correction on this member only
	config, err := p.MemberConfig(ctx, rc, first)
	if err != nil {
		t.Fatal(err)
	}
	if config.EngineConfig != "" || config.ConfigVersion <= badVersion {
		t.Fatalf("correction did not reach agent configuration: %+v", config)
	}
	other, err := p.MemberConfig(ctx, rc, second)
	if err != nil {
		t.Fatal(err)
	}
	if other.EngineConfig != "" || other.ConfigVersion >= badVersion || agents[second].reloads.Load() != 0 {
		t.Fatal("repair changed another member before first recovered")
	}
	if agents[first].reloads.Load() != 2 {
		t.Fatalf("correction not reloaded: %d", agents[first].reloads.Load())
	}
	syncCluster() // still stopped: no other member may proceed
	agents[first].version.Store(config.ConfigVersion)
	agents[first].running.Store(true)
	syncCluster()
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "active" {
		t.Fatal("repaired member did not recover")
	}
}

func TestCertificatePublicationIntentSurvivesReconcileCrashes(t *testing.T) {
	p, rc, agents, ca := renewalCluster(t, 3)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	if err := st.UpdateClusterSpec(ctx, domain.ClusterRef{Name: rc.Cluster.Name}, rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateClusterState(ctx, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	first, second := s.Members[0].Name, s.Members[1].Name
	original := map[string][]byte{}
	for _, m := range s.Members {
		crt, key, err := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name}, Validity: time.Hour, Server: true, Client: true})
		if err != nil {
			t.Fatal(err)
		}
		original[m.Name] = append(crt, key...)
		if err := putSecret(ctx, rc, "tls/members/"+m.Name+".pem", original[m.Name]); err != nil {
			t.Fatal(err)
		}
	}
	// Pending secrets commit independently. Lose the in-memory version bump
	// to model a crash before the reconciler commits this reconciliation.
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	restartController := func() {
		t.Helper()
		c, err := st.GetCluster(ctx, domain.ClusterRef{Name: rc.Cluster.Name})
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster = c
	}
	commitState := func() {
		t.Helper()
		if err := st.UpdateClusterState(ctx, rc.Cluster); err != nil {
			t.Fatal(err)
		}
	}
	restartController()
	gates := []string{}
	prepare := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (bool, error) {
		gates = append(gates, m.Name)
		if m.Name != first {
			t.Fatalf("another member disrupted before crash recovery: %s", m.Name)
		}
		return false, nil
	}
	syncCluster := func() {
		t.Helper()
		if _, err := p.sync(ctx, rc, prepare, recoveredRenewalMember); err != nil {
			t.Fatal(err)
		}
	}
	syncCluster() // record intent only
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "reload-publish" || s.ConfigVersion <= agents[first].version.Load() {
		t.Fatalf("resumed pending renewal lacks fresh ack version: stage=%s version=%d old=%d", s.find(first).Stage, s.ConfigVersion, agents[first].version.Load())
	}
	targetVersion := s.ConfigVersion
	for _, m := range s.Members {
		got, err := secret(ctx, rc, "tls/members/"+m.Name+".pem")
		if err != nil || !bytes.Equal(got, original[m.Name]) || agents[m.Name].reloads.Load() != 0 {
			t.Fatalf("intent published %s before state commit: %v", m.Name, err)
		}
	}
	commitState()
	syncCluster() // publish active certificate and delete pending copy
	got, err := secret(ctx, rc, "tls/members/"+first+".pem")
	if err != nil || bytes.Equal(got, original[first]) {
		t.Fatalf("approved certificate not published: %v", err)
	}
	if _, err := secret(ctx, rc, "tls/members/"+first+".pending.pem"); !missing(err) {
		t.Fatalf("pending copy still present: %v", err)
	}
	// Lose the in-memory reload-wait/config activation after secret commits.
	// The database still contains the prior durable publication intent.
	restartController()
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "reload-publish" || s.ConfigVersion != targetVersion {
		t.Fatal("publication intent/version did not survive controller crash")
	}
	syncCluster() // resume the same member with the same fresh target version
	commitState()
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "reload-wait" || s.find(first).ConfigVersion != targetVersion {
		t.Fatal("resumed publication lost its acknowledgement version")
	}
	beforeWait := len(gates)
	syncCluster() // old acknowledgement cannot authorize the next member
	if len(gates) != beforeWait {
		t.Fatal("old acknowledgement allowed another disruption")
	}
	got, err = secret(ctx, rc, "tls/members/"+second+".pem")
	if err != nil || !bytes.Equal(got, original[second]) {
		t.Fatalf("second member changed before fresh acknowledgement: %v", err)
	}
	agents[first].version.Store(targetVersion)
	syncCluster()
	s, _ = load(rc.Cluster)
	if s.find(first).Stage != "active" {
		t.Fatal("crashed publication never recovered")
	}
}
