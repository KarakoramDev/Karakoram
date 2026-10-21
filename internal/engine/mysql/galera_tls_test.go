// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"testing"
	"time"
)

func trafficCertificate(t *testing.T, raw []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("missing PEM certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
func TestPXCTrafficTrustIsSharedButSeparateFromSQLAndAgent(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	var previous *myspec.Galera
	sqlRoots := x509.NewCertPool()
	sqlCA, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	if !sqlRoots.AppendCertsFromPEM(sqlCA) {
		t.Fatal("invalid SQL CA")
	}
	for _, m := range s.data() {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		g := spec.Galera
		if g == nil || g.TrafficCert == nil || g.TrafficCAPEM == "" {
			t.Fatal("missing mandatory traffic credentials")
		}
		if bytes.Equal([]byte(g.TrafficCAPEM), sqlCA) {
			t.Fatal("traffic accepts SQL/agent trust")
		}
		if previous != nil && (previous.TrafficCAPEM != g.TrafficCAPEM || *previous.TrafficCert != *g.TrafficCert) {
			t.Fatal("traffic credentials are not identical")
		}
		previous = g
		leaf := trafficCertificate(t, []byte(g.TrafficCert.CertPEM))
		issuer := trafficCertificate(t, []byte(g.TrafficCAPEM))
		if bytes.Equal(leaf.RawSubject, leaf.RawIssuer) || bytes.Equal(leaf.RawSubject, issuer.RawSubject) {
			t.Fatal("traffic leaf is self-issued rather than distinctly named under its CA")
		}
		if !bytes.Equal(leaf.RawIssuer, issuer.RawSubject) {
			t.Fatal("traffic leaf issuer differs from its dedicated CA")
		}
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: sqlRoots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
			t.Fatal("traffic leaf accepted by SQL/agent CA")
		}
		trafficRoots := x509.NewCertPool()
		if !trafficRoots.AppendCertsFromPEM([]byte(g.TrafficCAPEM)) {
			t.Fatal("invalid traffic CA")
		}
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: trafficRoots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
			t.Fatal(err)
		}
		sqlLeaf := trafficCertificate(t, []byte(spec.ServerCert.CertPEM))
		if _, err := sqlLeaf.Verify(x509.VerifyOptions{Roots: trafficRoots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
			t.Fatal("SQL leaf authenticates replication")
		}
	}
}

func TestPXCTrafficTLSRotationStopsAllMembersBeforeRootReplacement(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	before, _ := secret(t.Context(), rc, galeraTrafficBase+".crt")
	beforeCA, _ := secret(t.Context(), rc, galeraTrafficCA+".crt")
	sqlBefore, _ := secret(t.Context(), rc, "tls/servers/demom0.crt")
	s, _ := load(rc.Cluster)
	history, epoch := s.Formation.GroupName, s.Formation.FormationEpoch
	if err := requestGaleraTrafficRotation(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	changed, done := false, false
	for range 120 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, _ := load(rc.Cluster)
		active, _ := secret(t.Context(), rc, galeraTrafficBase+".crt")
		if !bytes.Equal(active, before) && !changed {
			changed = true
			for _, m := range current.data() {
				if !m.Offline || b.native[m.Name].MemberState != "OFFLINE" {
					t.Fatal("traffic credentials changed while member remained live", m.Name)
				}
			}
		}
		b.apply(t, p, rc)
		if current.GaleraTLS == nil {
			done = true
			s = current
			break
		}
		p = New(Options{Provider: "pxc"})
	}
	if !changed || !done || s.Formation.GroupName != history || s.Formation.FormationEpoch != epoch+1 {
		t.Fatal("traffic TLS recovery did not preserve history with new bootstrap epoch", changed, done, s.Formation)
	}
	afterCA, _ := secret(t.Context(), rc, galeraTrafficCA+".crt")
	if bytes.Equal(beforeCA, afterCA) {
		t.Fatal("rotation did not replace compromised traffic trust")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(afterCA)
	if _, err := trafficCertificate(t, before).Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
		t.Fatal("old traffic credential remains trusted")
	}
	sqlAfter, _ := secret(t.Context(), rc, "tls/servers/demom0.crt")
	if !bytes.Equal(sqlBefore, sqlAfter) {
		t.Fatal("traffic renewal changed SQL leaf")
	}
	for _, file := range galeraTrafficFiles {
		if _, err := secret(t.Context(), rc, galeraTrafficPending+file.suffix); !missing(err) {
			t.Fatal("rotation staging retained", file.suffix, err)
		}
	}
}

type trafficRenewalClock struct{ at time.Time }

func (c trafficRenewalClock) Now() time.Time { return c.at }

func enterTrafficRenewalWindow(t *testing.T, rc *engine.Context) {
	t.Helper()
	cert, err := secret(t.Context(), rc, galeraTrafficCA+".crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := secret(t.Context(), rc, galeraTrafficCA+".key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, key, err := ca.Issue(pki.CertRequest{CommonName: rc.Cluster.Name + "-galera", DNSNames: []string{rc.Cluster.Name + "-galera"}, Validity: 45 * 24 * time.Hour, Server: true, Client: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, galeraTrafficBase+".key", key); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, galeraTrafficBase+".crt", leaf); err != nil {
		t.Fatal(err)
	}
	rc.Clock = trafficRenewalClock{at: trafficCertificate(t, leaf).NotAfter.Add(-15 * 24 * time.Hour)}
}

func TestPXCTrafficRenewalDefersToActiveReformation(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	enterTrafficRenewalWindow(t, rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Formation.Reformation = &reformation{State: "observing"}
	for _, m := range s.data() {
		m.Offline = true
	}
	publishRevision(s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.apply(t, p, rc)
	progressed := false
	for range 8 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.GaleraTLS != nil {
			t.Fatal("scheduled renewal claimed active recovery")
		}
		if current.Formation.Reformation.State == "bootstrapping" {
			progressed = true
			break
		}
		b.apply(t, p, rc)
		p = New(Options{Provider: "pxc"})
	}
	if !progressed {
		t.Fatal("reformation did not progress during traffic renewal window")
	}
}

var errTrafficMaintenanceStore = errors.New("traffic maintenance inventory failed")

type trafficMaintenanceFailureStore struct{ engine.ClusterStore }

func (s trafficMaintenanceFailureStore) ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error) {
	return nil, errTrafficMaintenanceStore
}

func TestPXCTrafficRenewalPreservesMaintenanceStoreErrors(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	enterTrafficRenewalWindow(t, rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	rc.Store = trafficMaintenanceFailureStore{ClusterStore: rc.Store}
	if err := ensureGaleraTrafficCertificate(t.Context(), rc, s); !errors.Is(err, errTrafficMaintenanceStore) {
		t.Fatal("maintenance error suppressed", err)
	}
}
