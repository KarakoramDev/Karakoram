// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRelayTargetsIncludesPlannedReplacementsAndObservedRouters(t *testing.T) {
	c := &domain.Cluster{Internal: json.RawMessage(`{"members":[{"name":"demom0","stage":"planned"},{"name":"demom1","removing":true},{"name":"demor","role":"router"}]}`), Status: domain.ClusterStatus{Members: []domain.MemberStatus{{Name: "demom0"}, {Name: "demor2"}, {}}}}
	want := []string{"demom0:27017", "demom0:9443", "demom1:27017", "demom1:9443", "demor2:27017", "demor2:9443", "demor:27017", "demor:9443"}
	if got := RelayTargets(c); !reflect.DeepEqual(got, want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	if got := RelayTargets(nil); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestMemberYAMLUsesNamespaceAgentURL(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.AgentURL = "https://examplerelay:9443"
	text, err := p.zeropsYAML(rc, &member{Name: "demom0"}, rc.Cluster.Spec.Version)
	if err != nil || !strings.Contains(text, rc.AgentURL) {
		t.Fatal(text, err)
	}
	rc.AgentURL = ""
	text, err = p.zeropsYAML(rc, &member{Name: "demom0"}, rc.Cluster.Spec.Version)
	if err != nil || !strings.Contains(text, p.options.ControlPlaneURL) {
		t.Fatal(text, err)
	}
}

func TestCloneUsesSourceNamespaceStorageAndValidation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	p.options.Store = st
	backup := &domain.Backup{Namespace: "production", Name: "base", Cluster: rc.Cluster.Name, Storage: "snapshots", Phase: domain.PhaseSucceeded, SourceTopology: rc.Cluster.Spec.Topology, SourceVersion: rc.Cluster.Spec.Version}
	if err := st.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Namespace: "production", Name: "snapshots", Credentials: domain.StorageCredentials{SecretAccessKey: "source-secret"}}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = ""
	rc.Cluster.Spec.DataSource = &domain.DataSource{Namespace: "production", Cluster: rc.Cluster.Name, Backup: "base"}
	if err := p.Validate(ctx, nil, rc.Cluster); err != nil {
		t.Fatal("same cluster name in another namespace was rejected:", err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	env, err := p.pbmEnv(ctx, rc, s)
	if err != nil || env.Storage.Namespace != "production" || env.Credentials.SecretAccessKey != "source-secret" {
		t.Fatal(env.Storage, err)
	}
	if env.RC.Cluster.Ref().Key() != rc.Cluster.Ref().Key() || len(s.UsedStorages) != 0 {
		t.Fatal("source storage changed target identity or cleanup ownership")
	}
	rc.Cluster.Spec.DataSource.Namespace = "development"
	if err := p.Validate(ctx, nil, rc.Cluster); err == nil {
		t.Fatal("validation looked up backup outside requested source namespace")
	}
}

func TestPublicPortReservationsAreProjectScoped(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := &member{Name: "demom0"}
	first, err := p.allocatePublicPort(ctx, rc, s, m)
	if err != nil {
		t.Fatal(err)
	}
	otherCluster := *rc.Cluster
	otherCluster.Namespace = "production"
	other := *rc
	other.Cluster = &otherCluster
	second, err := p.allocatePublicPort(ctx, &other, s, m)
	if err != nil || first != second {
		t.Fatalf("independent project port pools interfered: %d, %d, %v", first, second, err)
	}
}
