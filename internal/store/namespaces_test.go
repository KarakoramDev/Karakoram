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
	if err := s.PutNamespaceCredentials(ctx, n, "test-token", []byte("test-private-key")); err != nil {
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
	if err = s.PutNamespaceToken(ctx, n.Name, "rotated-token"); err != nil {
		t.Fatal(err)
	}
	if err = s.PutNamespaceRelayKey(ctx, n.Name, []byte("renewed-key")); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteNamespace(ctx, n.Name); err != nil {
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
	if err = s.DeleteNamespace(ctx, n.Name); !errors.Is(err, ErrNotFound) {
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
