// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package certificates

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func certificate(t *testing.T, expiry time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: expiry.Add(-365 * 24 * time.Hour), NotAfter: expiry, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestInventoryAndDailyDurableWarnings(t *testing.T) {
	ctx := t.Context()
	sealer, err := secrets.New("0123456789abcdef0123456789abcdef-test-key")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	soon := certificate(t, now.Add(30*24*time.Hour))
	expired := certificate(t, now.Add(-time.Hour))
	later := certificate(t, now.Add(90*24*time.Hour))
	identity, err := json.Marshal(struct{ CACert, CAKey, Cert, Key []byte }{soon, []byte("private sentinel"), expired, []byte("private sentinel")})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealer.SealFor("server/internal-pki", identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(sealed)
	if err := st.SetSetting(ctx, "server/internal-pki", raw); err != nil {
		t.Fatal(err)
	}
	if err := st.PutRelayPKI(ctx, &store.RelayPKI{CACert: soon, HubCert: later}); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Name: "db", Spec: domain.ClusterSpec{Provider: "postgres"}, Status: domain.ClusterStatus{Conditions: []domain.Condition{{Type: "TLSCertificate", Reason: "Expired", Message: "private sentinel"}}}}
	if err := st.CreateClusterWithSecrets(ctx, &c, map[string][]byte{"tls/ca.crt": soon, "tls/user.crt": expired, "tls/ca.key": []byte("private sentinel")}); err != nil {
		t.Fatal(err)
	}
	inventory, err := Inventory(ctx, st)
	if err != nil || len(inventory) != 6 {
		t.Fatalf("inventory=%+v err=%v", inventory, err)
	}
	hub := events.NewHub(16)
	live, cancel := hub.Subscribe("", "")
	defer cancel()
	watcher := New(Options{Store: st, Hub: hub, Now: func() time.Time { return now }})
	if err := watcher.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	// Reconstructing the watcher must not reset the durable daily marker.
	watcher = New(Options{Store: st, Hub: hub, Now: func() time.Time { return now }})
	if err := watcher.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	all, err := st.ListEvents(ctx, store.EventFilter{})
	if err != nil || len(all) != 5 {
		t.Fatalf("events=%+v err=%v", all, err)
	}
	for _, event := range all {
		if event.Type != "Warning" || event.Reason != "CertificateExpiring" || !event.CreatedAt.Equal(now) || strings.Contains(event.Message, "private sentinel") || strings.Contains(event.Message, "BEGIN") {
			t.Fatalf("unsafe event: %+v", event)
		}
		select {
		case got := <-live:
			if got.ID != event.ID {
				t.Fatal(got, event)
			}
		default:
			t.Fatal("durable event not published")
		}
	}
	now = now.Add(24 * time.Hour)
	if err := watcher.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	all, err = st.ListEvents(ctx, store.EventFilter{})
	if err != nil || len(all) != 10 {
		t.Fatalf("next day events=%+v err=%v", all, err)
	}
}

func TestConditionWarningWithoutReadableCertificate(t *testing.T) {
	sealer, err := secrets.New("0123456789abcdef0123456789abcdef-test-key")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := domain.Cluster{Name: "db", Spec: domain.ClusterSpec{Provider: "postgres"}, Status: domain.ClusterStatus{Conditions: []domain.Condition{{Type: "TLSCertificate", Reason: "ExpiringSoon", Message: "secret condition text"}}}}
	if err := st.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	watcher := New(Options{Store: st, Now: func() time.Time { return time.Unix(1, 0) }})
	if err := watcher.Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	all, err := st.ListEvents(t.Context(), store.EventFilter{})
	if err != nil || len(all) != 1 || strings.Contains(all[0].Message, "secret condition text") {
		t.Fatal(all, err)
	}
}
