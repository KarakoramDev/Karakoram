// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestNamespacePersistenceAndSealedCredentials(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	n := &domain.Namespace{Name: "testing", ProjectID: "proj-test", CreatedAt: time.Now(), UpdatedAt: time.Now(), ClientSerial: "123"}
	if err := s.CreateNamespaceCredentials(ctx, n, "test-token", []byte("test-private-key")); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetNamespace(ctx, n.Name)
	if err != nil || got.ProjectID != n.ProjectID || got.ClientSerial != "123" {
		t.Fatalf("record: %+v %v", got, err)
	}
	all, err := s.ListNamespaces(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("list: %v %v", all, err)
	}
	token, err := s.GetNamespaceToken(ctx, n.Name)
	if err != nil || token != "test-token" {
		t.Fatalf("token: %v", err)
	}
	key, err := s.GetNamespaceRelayKey(ctx, n.Name)
	if err != nil || string(key) != "test-private-key" {
		t.Fatalf("key: %v", err)
	}
	var sealed string
	if err = s.reader.QueryRowContext(ctx, "SELECT secret FROM records WHERE kind='namespace-secret' AND scope=? AND key='zerops-token'", n.Name).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, "test-token") {
		t.Fatal("token stored in plaintext")
	}
	if _, err = s.sealer.OpenFor("namespace/another/zerops-token", sealed); err == nil {
		t.Fatal("namespace AAD not enforced")
	}
	if _, err = s.sealer.OpenFor("namespace/testing/relay-key", sealed); err == nil {
		t.Fatal("secret-purpose AAD not enforced")
	}
	if err = s.UpdateNamespaceCredentials(ctx, n, "rotated-token", nil); err != nil {
		t.Fatal(err)
	}
	if err = s.UpdateNamespaceCredentials(ctx, n, "rotated-token", []byte("renewed-key")); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteNamespace(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetNamespace(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = s.GetNamespaceToken(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("token survives deletion", err)
	}
	if _, err = s.GetNamespaceRelayKey(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("key survives deletion", err)
	}
	if err = s.DeleteNamespace(ctx, n); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
}
func TestRelayPKISettingIsSealed(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	p := &RelayPKI{CACert: []byte("ca-cert"), CAKey: []byte("ca-private-key"), HubCert: []byte("hub-cert"), HubKey: []byte("hub-private-key")}
	if err := s.PutRelayPKI(ctx, p); err != nil {
		t.Fatal(err)
	}
	raw, err := s.GetSetting(ctx, "server/relay-pki")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-key") {
		t.Fatal("PKI setting is plaintext")
	}
	var ciphertext string
	if err = json.Unmarshal(raw, &ciphertext); err != nil {
		t.Fatal(err)
	}
	if _, err = s.sealer.OpenFor("server/internal-pki", ciphertext); err == nil {
		t.Fatal("PKI AAD not enforced")
	}
	got, err := s.GetRelayPKI(ctx)
	if err != nil || string(got.CAKey) != string(p.CAKey) || string(got.HubKey) != string(p.HubKey) {
		t.Fatalf("PKI round trip: %v", err)
	}
}

func TestNamespaceWritesRejectStaleRegistration(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	old := &domain.Namespace{
		Name: "production", ProjectID: "proj-test", Description: "Original description",
		CreatedAt: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
		Phase:     "Pending", ClientSerial: "original-serial",
	}
	if err := s.CreateNamespaceCredentials(ctx, old, "original-token", []byte("original-key")); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateNamespaceCredentials(ctx, old, "other-token", []byte("other-key")); !errors.Is(err, ErrConflict) {
		t.Fatal("creation replaced an existing registration", err)
	}
	if err := s.BeginNamespaceDeletion(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateNamespace(ctx, old); !errors.Is(err, ErrStale) {
		t.Fatal("old snapshot cleared deletion intent", err)
	}
	if err := s.UpdateNamespaceCredentials(ctx, old, "other-token", []byte("other-key")); !errors.Is(err, ErrStale) {
		t.Fatal("old credential snapshot cleared deletion intent", err)
	}
	if err := s.DeleteNamespace(ctx, old); err != nil {
		t.Fatal(err)
	}
	staleWrites := []struct {
		name  string
		write func() error
	}{
		{"state", func() error { return s.UpdateNamespace(ctx, old) }},
		{"credentials", func() error { return s.UpdateNamespaceCredentials(ctx, old, "other-token", []byte("other-key")) }},
		{"deletion intent", func() error { return s.BeginNamespaceDeletion(ctx, old) }},
		{"final deletion", func() error { return s.DeleteNamespace(ctx, old) }},
		{"token removal", func() error { return s.DeleteNamespaceToken(ctx, old) }},
	}
	checkStale := func() {
		t.Helper()
		for _, test := range staleWrites {
			if err := test.write(); !errors.Is(err, ErrStale) {
				t.Fatalf("%s accepted a stale registration: %v", test.name, err)
			}
		}
	}
	checkStale()
	if _, err := s.GetNamespace(ctx, old.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("old snapshot resurrected the deleted registration", err)
	}
	if _, err := s.GetNamespaceToken(ctx, old.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("old credential write resurrected a token", err)
	}
	fresh := *old
	fresh.Description = "Replacement description"
	fresh.CreatedAt = old.CreatedAt.Add(time.Second)
	fresh.ClientSerial = "replacement-serial"
	if err := s.CreateNamespaceCredentials(ctx, &fresh, "replacement-token", []byte("replacement-key")); err != nil {
		t.Fatal(err)
	}
	checkStale()
	got, err := s.GetNamespace(ctx, fresh.Name)
	if err != nil || !got.CreatedAt.Equal(fresh.CreatedAt) || got.Description != fresh.Description || got.ClientSerial != fresh.ClientSerial || got.Phase != "Pending" {
		t.Fatal("old writer changed the replacement registration", err)
	}
	token, err := s.GetNamespaceToken(ctx, fresh.Name)
	if err != nil || token != "replacement-token" {
		t.Fatal("old writer changed replacement credentials", err)
	}
	key, err := s.GetNamespaceRelayKey(ctx, fresh.Name)
	if err != nil || string(key) != "replacement-key" {
		t.Fatal("old writer changed replacement relay key", err)
	}
}

func TestNamespaceLifecycleReadsIgnorePinnedReaderSnapshot(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	old := &domain.Namespace{
		Name: "production", ProjectID: "proj-test", Description: "Original description",
		CreatedAt: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
		Phase:     "Ready", Relay: domain.NamespaceRelayStatus{Connected: true},
	}
	if err := s.CreateNamespaceCredentials(ctx, old, "original-token", []byte("original-key")); err != nil {
		t.Fatal(err)
	}
	// Model a connection returned to the reader pool with an open read transaction.
	s.reader.SetMaxOpenConns(1)
	if _, err := s.reader.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	defer s.reader.ExecContext(ctx, "ROLLBACK")
	if _, err := s.GetNamespace(ctx, old.Name); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginNamespaceDeletion(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNamespace(ctx, old); err != nil {
		t.Fatal(err)
	}
	fresh := *old
	fresh.CreatedAt = old.CreatedAt.Add(time.Second)
	fresh.Description = "Replacement description"
	fresh.Phase = "Pending"
	fresh.Relay.Connected = false
	if err := s.CreateNamespaceCredentials(ctx, &fresh, "replacement-token", []byte("replacement-key")); err != nil {
		t.Fatal(err)
	}
	stale, err := s.GetNamespace(ctx, fresh.Name)
	if err != nil || !stale.CreatedAt.Equal(old.CreatedAt) || !stale.Relay.Connected {
		t.Fatal("fixture did not preserve the old reader snapshot", err)
	}
	current, err := s.GetNamespaceForUpdate(ctx, fresh.Name)
	if err != nil || !current.CreatedAt.Equal(fresh.CreatedAt) || current.Description != fresh.Description || current.Relay.Connected {
		t.Fatal("lifecycle read trusted a stale reader snapshot", err)
	}
	all, err := s.ListNamespacesForUpdate(ctx)
	if err != nil || len(all) != 1 || !all[0].CreatedAt.Equal(fresh.CreatedAt) {
		t.Fatal("registration inventory trusted a stale reader snapshot", err)
	}
	token, err := s.GetNamespaceTokenForUpdate(ctx, fresh.Name)
	if err != nil || token != "replacement-token" {
		t.Fatal("lifecycle read trusted stale token credentials", err)
	}
	key, err := s.GetNamespaceRelayKeyForUpdate(ctx, fresh.Name)
	if err != nil || string(key) != "replacement-key" {
		t.Fatal("relay provisioning trusted stale enrollment credentials", err)
	}
}

func TestLegacyNamespaceDefaultsToZerops(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	record := `{"name":"legacy","projectId":"project","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","phase":"Ready"}`
	if _, err := s.writer.ExecContext(ctx, "INSERT INTO namespaces(name,data,updated_at) VALUES(?,?,?)", "legacy", record, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	n, err := s.GetNamespace(ctx, "legacy")
	if err != nil || n.Backend != "zerops" {
		t.Fatalf("legacy namespace: %+v %v", n, err)
	}
	all, err := s.ListNamespaces(ctx)
	if err != nil || len(all) != 1 || all[0].Backend != "zerops" {
		t.Fatalf("legacy listing: %+v %v", all, err)
	}
	if err := s.UpdateNamespace(ctx, n); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceBackendCredentialsAndImmutability(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	n := &domain.Namespace{Name: "custom", Backend: "nomad", Config: map[string]string{"region": "example"}, ProjectID: "target", Phase: "Ready", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := s.CreateNamespaceCredentials(ctx, n, "credential", nil); err != nil {
		t.Fatal(err)
	}
	token, err := s.GetNamespaceToken(ctx, n.Name)
	if err != nil || token != "credential" {
		t.Fatalf("credential: %q %v", token, err)
	}
	var sealed string
	if err := s.reader.QueryRowContext(ctx, "SELECT secret FROM records WHERE kind='namespace-secret' AND scope=? AND key=?", n.Name, "nomad-token").Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	plain, err := s.sealer.OpenFor("namespace/custom/nomad-token", sealed)
	if err != nil || string(plain) != token {
		t.Fatal("credential AAD", err)
	}
	n.Backend = "other"
	if err := s.UpdateNamespace(ctx, n); !errors.Is(err, ErrStale) {
		t.Fatal("backend mutation accepted", err)
	}
	n.Backend = "nomad"
	if err := s.DeleteNamespaceToken(ctx, n); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetNamespaceTokenForUpdate(ctx, n.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("credential survived deletion", err)
	}
}
