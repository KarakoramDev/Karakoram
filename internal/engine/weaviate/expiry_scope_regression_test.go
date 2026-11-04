// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func installExpiredActiveLeaf(t *testing.T, f *coreFixture, s *state, m *member, serial int64) {
	t.Helper()
	ctx := context.Background()
	leaf, key := expiredPair(t, f, m.Name, serial)
	cfg, err := f.p.MemberConfig(ctx, f.rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := secret(ctx, f.rc, secretCA)
	if err != nil {
		t.Fatal(err)
	}
	cfg.TLS.CertPEM = string(leaf)
	cfg.TLS.KeyPEM = string(key)
	cfg.TLS.CAPEM = string(ca)
	if err = activateConfig(ctx, f.rc, s, m, cfg); err != nil {
		t.Fatal(err)
	}
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
}

func TestRound3ExpiredActiveLeafStrictReadinessAndStatus(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	// The first endpoint remains valid and returns healthy nodes/quorum. Neither
	// native endpoint failover nor agent recovery may hide another expired leaf.
	installExpiredActiveLeaf(t, f, s, s.Members[1], 810)
	assertStrict := func() {
		t.Helper()
		if ready, err := f.p.clusterReady(ctx, f.rc, s); ready || err == nil || !strings.Contains(err.Error(), "native certificate expired") {
			t.Fatalf("ordinary readiness accepted expired active leaf: ready=%v err=%v", ready, err)
		}
	}
	assertStatus := func(renewal bool) {
		t.Helper()
		status, err := f.p.Status(ctx, f.rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady {
			t.Fatal("expired active leaf reported Ready")
		}
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == "NativeTLS" && condition.Status == "False" && condition.Reason == "NativeCertificateExpired" {
				found = true
			}
		}
		if !found || !strings.Contains(status.Message, "Native certificate expired") {
			t.Fatalf("missing explicit expiry condition: %+v", status)
		}
		if renewal && !strings.Contains(status.Message, "renewal in progress") {
			t.Fatalf("missing renewal status: %s", status.Message)
		}
		for _, m := range status.Members {
			if m.Name == s.Members[1].Name && m.Health {
				t.Fatal("expired member reported healthy")
			}
		}
	}
	assertStrict()
	assertStatus(false)
	if err := f.p.startRolling(ctx, f.rc, s, "config", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	assertStrict()
	assertStatus(true)
	if ready, err := f.p.rollingClusterReady(ctx, f.rc, s); err != nil || !ready {
		t.Fatalf("valid replacement roll blocked: ready=%v err=%v", ready, err)
	}
	name := s.Members[1].Name
	target, err := secret(ctx, f.rc, "target/"+name)
	if err != nil {
		t.Fatal(err)
	}
	var invalid rollingTarget
	if err = json.Unmarshal(target, &invalid); err != nil {
		t.Fatal(err)
	}
	active, err := f.p.MemberConfig(ctx, f.rc, name)
	if err != nil {
		t.Fatal(err)
	}
	invalid.Config.TLS = active.TLS
	raw, err := json.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err = putSecret(ctx, f.rc, "target/"+name, raw); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.p.rollingClusterReady(ctx, f.rc, s); ready || err == nil {
		t.Fatalf("non-renewing roll accepted expiry: ready=%v err=%v", ready, err)
	}
	if err = putSecret(ctx, f.rc, "target/"+name, target); err != nil {
		t.Fatal(err)
	}
	// An unrelated subset roll cannot borrow another member's expired-leaf pin.
	s.Rolling.Order = []string{s.Members[0].Name}
	if ready, err := f.p.rollingClusterReady(ctx, f.rc, s); ready || err == nil {
		t.Fatalf("unrelated roll accepted expiry: ready=%v err=%v", ready, err)
	}
}

func TestRound3MultipleExpiredLeavesTLSRotationRecovery(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	for i, m := range s.Members {
		installExpiredActiveLeaf(t, f, s, m, int64(820+i))
	}
	native := httptest.NewUnstartedServer(f.native.Config.Handler)
	native.TLS = &tls.Config{GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cfg, err := f.p.MemberConfig(ctx, f.rc, hello.ServerName)
		if err != nil {
			return nil, err
		}
		pair, err := tls.X509KeyPair([]byte(cfg.TLS.CertPEM), []byte(cfg.TLS.KeyPEM))
		return &pair, err
	}}
	native.StartTLS()
	defer native.Close()
	f.rc.Dialer = coreDialer{native.Listener.Addr().String()}
	root := &domain.Operation{ID: "expired-root-rotation", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"karakoram-admin"}`)}
	if _, err := f.p.RunOperation(ctx, f.rc, root); err == nil {
		t.Fatal("credential rotation admitted expired native leaves")
	}
	op := &domain.Operation{ID: "expired-tls-rotation", Kind: domain.OperationRotateTLS}
	if _, err := f.p.RunOperation(ctx, f.rc, op); err != nil {
		t.Fatalf("TLS replacement preflight blocked: %v", err)
	}
	s, err := load(f.rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rotation == nil {
		t.Fatal("TLS rotation preflight did not checkpoint replacement workflow")
	}
	if ready, err := f.p.clusterReady(ctx, f.rc, s); ready || err == nil {
		t.Fatal("in-progress TLS rotation weakened ordinary readiness")
	}
	done := false
	for range 300 {
		result, err := f.p.RunOperation(ctx, f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("multiple expired native leaves could not rotate sequentially")
	}
	s, err = load(f.rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := f.p.clusterReady(ctx, f.rc, s); err != nil || !ready {
		t.Fatalf("renewed cluster not strictly ready: ready=%v err=%v", ready, err)
	}
	status, err := f.p.Status(ctx, f.rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range status.Conditions {
		if condition.Reason == "NativeCertificateExpired" {
			t.Fatal("expiry condition retained after renewal")
		}
	}
}
