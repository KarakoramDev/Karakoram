// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/cpbackup"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
)

func metricTestCertificate(t *testing.T) []byte {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Unix(100, 0), NotAfter: time.Unix(2000000000, 0)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func metricReader(t *testing.T, s *Server, name, permissions string) string {
	t.Helper()
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: name}, "metrics-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+permissions); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), name, "metrics", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestPollingMetricsExposition(t *testing.T) {
	s, _, admin := testServer(t)
	snapshot := infra.PlatformSnapshot("dokploy|https://target.invalid|private-target")
	snapshot.ReadStart(t.Context())
	snapshot.BeginMutation(t.Context())()
	w := call(s, "GET", "/metrics", "", admin)
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	body := w.Body.String()
	for _, descriptor := range monitoring.PollingMetrics {
		help := "# HELP " + descriptor.Name + " " + descriptor.Help + "\n"
		typ := "# TYPE " + descriptor.Name + " counter\n"
		if strings.Count(body, help) != 1 || strings.Count(body, typ) != 1 {
			t.Errorf("missing or duplicate HELP/TYPE for %s", descriptor.Name)
		}
		found := 0
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(line, descriptor.Name+"{") {
				continue
			}
			found++
			labels, value, ok := strings.Cut(line, "} ")
			if !ok {
				t.Fatalf("malformed sample %q", line)
			}
			if _, err := strconv.ParseUint(value, 10, 64); err != nil {
				t.Fatalf("noninteger counter %q: %v", line, err)
			}
			for _, label := range descriptor.Labels {
				if !strings.Contains(labels, label+`="`) {
					t.Errorf("missing %s in %q", label, line)
				}
			}
		}
		if found == 0 {
			t.Errorf("no samples for %s", descriptor.Name)
		}
	}
	for _, sample := range []string{
		`karakoram_platform_requests_total{provider="dokploy",caller="other",kind="read",source="network"}`,
		`karakoram_platform_requests_total{provider="dokploy",caller="other",kind="read",source="snapshot"}`,
		`karakoram_platform_requests_total{provider="dokploy",caller="other",kind="mutation",source="network"}`,
		`karakoram_platform_listing_checks_total{provider="dokploy",result="unchanged"}`,
		`karakoram_platform_listing_checks_total{provider="dokploy",result="drift"}`,
		`karakoram_platform_listing_checks_total{provider="dokploy",result="error"}`,
		`karakoram_agent_heartbeats_total{result="accepted"}`,
		`karakoram_agent_heartbeats_total{result="token_rejected"}`,
		`karakoram_agent_heartbeats_total{result="invalid"}`,
		`karakoram_observation_steady_total{result="accepted"}`,
		`karakoram_observation_steady_total{result="fallback"}`,
	} {
		if !strings.Contains(body, sample+" ") {
			t.Errorf("missing %s", sample)
		}
	}
	for _, caller := range []string{"other", "listing", "reconcile", "operation", "availability", "backoff-observe", "dial-authorization", "namespace", "orphan-janitor", "api"} {
		for _, labels := range []string{`kind="read",source="network"`, `kind="read",source="snapshot"`, `kind="mutation",source="network"`} {
			if !strings.Contains(body, `karakoram_platform_requests_total{provider="dokploy",caller="`+caller+`",`+labels+`} `) {
				t.Errorf("missing caller combination %s %s", caller, labels)
			}
		}
	}
	for _, private := range []string{"target.invalid", "private-target"} {
		if strings.Contains(body, private) {
			t.Errorf("target leaked: %s", private)
		}
	}
	reader := metricReader(t, s, "pollingreader", "\np, user:pollingreader, monitoring, read, default/prod, allow\n")
	for _, tc := range []struct{ path, credential string }{
		{"/metrics", reader}, {"/metrics?namespace=default", admin},
	} {
		response := call(s, "GET", tc.path, "", tc.credential)
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		for _, descriptor := range monitoring.PollingMetrics {
			if strings.Contains(response.Body.String(), descriptor.Name) {
				t.Errorf("installation counter leaked to filtered scrape: %s", descriptor.Name)
			}
		}
	}
	t.Log("4 counter families; 10 bounded callers x 3 request kinds/sources; installation-only authorization; no target labels")
}

func TestStateMetricsAuthorizationNamespaceAndDisabledMonitoring(t *testing.T) {
	s, sealer, admin := testServer(t)
	ctx := t.Context()
	if err := s.Store.CreateNamespace(ctx, &domain.Namespace{Name: "team", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	other := domain.Cluster{Namespace: "team", Name: "prod", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseError}}
	if err := s.Store.CreateCluster(ctx, &other); err != nil {
		t.Fatal(err)
	}
	cert := metricTestCertificate(t)
	for _, ref := range []domain.ClusterRef{{Namespace: "default", Name: "prod"}, {Namespace: "team", Name: "prod"}} {
		if err := s.Store.SetClusterSecrets(ctx, ref, map[string][]byte{"tls/user.crt": cert, "tls/user.key": []byte("private-key-sentinel")}); err != nil {
			t.Fatal(err)
		}
	}
	identity, _ := json.Marshal(struct{ CACert, Cert, Key []byte }{cert, cert, []byte("private-key-sentinel")})
	sealed, err := sealer.SealFor("server/internal-pki", identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(sealed)
	if err := s.Store.SetSetting(ctx, "server/internal-pki", raw); err != nil {
		t.Fatal(err)
	}
	clusterReader := metricReader(t, s, "metriccluster", "\np, user:metriccluster, monitoring, read, default/prod, allow\n")
	settingsReader := metricReader(t, s, "metricsettings", "\np, user:metricsettings, settings, read, *, allow\n")
	noReader := metricReader(t, s, "metricnone", "")
	if w := call(s, "GET", "/metrics", "", ""); w.Code != 401 {
		t.Fatal("anonymous", w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		name, token, path string
		want, absent      []string
	}{
		{"cluster", clusterReader, "/metrics", []string{`karakoram_cluster_ready{namespace="default",cluster="prod"} 1`, `certificate="tls/user.crt"`}, []string{`cluster="dev"`, `namespace="team"`, `certificate="internal-ca"`, `karakoram_control_plane_backup_enabled`}},
		{"settings", settingsReader, "/metrics", []string{`karakoram_control_plane_backup_enabled 0`, `certificate="internal-ca"`}, []string{`karakoram_cluster_ready`, `certificate="tls/user.crt"`}},
		{"admin", admin, "/metrics", []string{`namespace="team",cluster="prod"`, `cluster="dev"`, `karakoram_control_plane_backup_enabled 0`}, nil},
		{"admin namespace", admin, "/metrics?namespace=team", []string{`namespace="team",cluster="prod"`}, []string{`namespace="default"`, `karakoram_control_plane_backup_enabled`, `certificate="internal-ca"`}},
		{"other namespace", clusterReader, "/metrics?namespace=team", nil, []string{`karakoram_cluster_`, `karakoram_certificate_`, `karakoram_control_plane_`}},
		{"settings namespace", settingsReader, "/metrics?namespace=team", nil, []string{`karakoram_cluster_`, `karakoram_certificate_`, `karakoram_control_plane_`}},
		{"no grants", noReader, "/metrics", nil, []string{`karakoram_`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := call(s, "GET", tc.path, "", tc.token)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			body := w.Body.String()
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("missing %s in %s", want, body)
				}
			}
			for _, absent := range append(tc.absent, "private-key-sentinel", "BEGIN CERTIFICATE") {
				if strings.Contains(body, absent) {
					t.Errorf("leaked %s in %s", absent, body)
				}
			}
		})
	}
	// The fixture clusters have member monitoring disabled. State still works
	// without any collector: installation /metrics is not the history API.
	s.Monitoring = nil
	if w := call(s, "GET", "/metrics", "", clusterReader); w.Code != 200 || !strings.Contains(w.Body.String(), `karakoram_cluster_ready{namespace="default",cluster="prod"} 1`) {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestStateMetricsInventoryErrorsDoNotReturnPartialSuccess(t *testing.T) {
	s, _, admin := testServer(t)
	reader := metricReader(t, s, "metricreader", "\np, user:metricreader, monitoring, read, default/prod, allow\n")
	if err := s.Store.SetSetting(t.Context(), cpbackup.PolicyKey, json.RawMessage(`"invalid-policy-sentinel"`)); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "GET", "/metrics", "", reader); w.Code != 200 {
		t.Fatal("cluster reader must not load installation policy", w.Code, w.Body.String())
	}
	w := call(s, "GET", "/metrics", "", admin)
	if w.Code != 500 || strings.Contains(w.Body.String(), "karakoram_cluster_ready") || strings.Contains(w.Body.String(), "invalid-policy-sentinel") {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.Store.SetSetting(t.Context(), cpbackup.PolicyKey, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: "default", Name: "prod"}, "tls/user.crt", []byte("malformed-certificate-sentinel")); err != nil {
		t.Fatal(err)
	}
	w = call(s, "GET", "/metrics", "", reader)
	if w.Code != 500 || strings.Contains(w.Body.String(), "karakoram_cluster_ready") || strings.Contains(w.Body.String(), "malformed-certificate-sentinel") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestStateMetricsLoadsTerminalDurableResults(t *testing.T) {
	s, _, token := testServer(t)
	ctx := t.Context()
	completed := time.Unix(1700000000, 0).UTC()
	backup := domain.Backup{Name: "metrics-backup", Namespace: "default", Cluster: "prod", Phase: domain.PhaseSucceeded, CompletedAt: &completed}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	drill := domain.BackupDrill{ID: "metrics-drill", Namespace: "default", Cluster: "prod", Phase: "Succeeded", CompletedAt: &completed, RestoreDurationSeconds: 17, CreatedAt: completed.Add(-time.Minute)}
	if err := s.Store.CreateBackupDrill(ctx, drill); err != nil {
		t.Fatal(err)
	}
	cp := domain.ControlPlaneBackup{ID: "metrics-cp", Phase: domain.PhaseFailed, CompletedAt: &completed, CreatedAt: completed.Add(-time.Minute)}
	if err := s.Store.EnqueueControlPlaneBackup(ctx, &cp, nil); err != nil {
		t.Fatal(err)
	}
	w := call(s, "GET", "/metrics", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, want := range []string{
		`karakoram_backup_last_success_timestamp_seconds{namespace="default",cluster="prod",schedule=""} 1.7e+09`,
		`karakoram_backup_drill_last_success_timestamp_seconds{namespace="default",cluster="prod"} 1.7e+09`,
		`karakoram_backup_drill_last_restore_duration_seconds{namespace="default",cluster="prod"} 17`,
		`karakoram_control_plane_backup_last_failure_timestamp_seconds 1.7e+09`,
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %s in %s", want, w.Body.String())
		}
	}
}
