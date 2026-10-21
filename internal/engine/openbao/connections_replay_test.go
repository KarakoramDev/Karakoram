// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

// connectionNative wraps the ordinary fake API without changing the shared
// membership fixture. Every database write can lose its response once.
func connectionNative(t *testing.T, rc *engine.Context, a *fakeAgents, fail string, paths *[]string) {
	t.Helper()
	for _, m := range ordered(state(t, rc)) {
		name := m.Name
		cert, e := memberCertificate(t.Context(), rc, "tls/members/"+name+".pem")
		if e != nil {
			t.Fatal(e)
		}
		pair, e := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
		if e != nil {
			t.Fatal(e)
		}
		failed := false
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			if path == "/v1/sys/mounts" {
				_, _ = w.Write([]byte(`{"data":{}}`))
				return
			}
			if strings.Contains(path, "/database/") || strings.HasPrefix(path, "/v1/sys/mounts/") || strings.HasPrefix(path, "/v1/sys/leases/") {
				*paths = append(*paths, r.Method+" "+path)
				if strings.HasPrefix(path, "/v1/sys/leases/revoke-prefix/") {
					var body struct {
						Sync bool `json:"sync"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !body.Sync {
						t.Error("lease revocation must finish before deleting plugin configuration")
					}
				}
				if !failed && fail != "" && strings.Contains(path, fail) {
					failed = true
					w.WriteHeader(503)
					return
				}
				w.WriteHeader(204)
				return
			}
			a.serveNative(name, w, r)
		}))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
		server.StartTLS()
		t.Cleanup(server.Close)
		a.nativeServers[name] = server
	}
}

func TestConnectionResumeEveryOpenBaoStepAndDeleteOrdering(t *testing.T) {
	for _, step := range []string{"sys/mounts/database", "database/config/orders", "database/roles/orders_reader", "database/rotate-root/orders"} {
		t.Run(step, func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			target, pg := connectionTargetFixture(rc)
			target.Context.Cluster.Status.Connection.Hosts = []string{"targetpx:5432"}
			ca, e := secret(t.Context(), rc, "tls/ca.crt")
			if e != nil {
				t.Fatal(e)
			}
			_ = rc.Secrets.SetClusterSecret(t.Context(), target.Context.Cluster.Ref(), "tls/ca.crt", ca)
			req := connectionWrite()
			c, e := p.CreateDatabaseConnection(t.Context(), rc, target, req)
			if e != nil || c.Phase != ConnectionPending {
				t.Fatalf("pending trust: %+v %v", c, e)
			}
			// Apply the exact trust fingerprint to model membership's completed rolling
			// gate; MemberConfig must carry the CA without the administrative password.
			s := state(t, rc)
			fingerprint, e := p.restartFingerprint(t.Context(), rc)
			if e != nil {
				t.Fatal(e)
			}
			for _, m := range ordered(s) {
				m.RestartFingerprint = fingerprint
			}
			if e = s.save(rc.Cluster); e != nil {
				t.Fatal(e)
			}
			config, e := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
			if e != nil {
				t.Fatal(e)
			}
			var spec baospec.MemberSpec
			_ = json.Unmarshal(config.EngineSpec, &spec)
			if len(spec.TrustCAPEMs) != 1 || spec.TrustCAPEMs[0] != string(ca) {
				t.Fatal("target trust missing from MemberConfig")
			}
			cred, _ := readConnectionCredential(t.Context(), rc, req.Name)
			raw, _ := json.Marshal(config)
			if strings.Contains(string(raw), cred.Password) {
				t.Fatal("target password in MemberConfig")
			}
			paths := []string{}
			connectionNative(t, rc, a, step, &paths)
			_, e = p.CreateDatabaseConnection(t.Context(), rc, target, req)
			if e == nil {
				t.Fatal("lost response not surfaced")
			}
			p = New(p.options)
			a.p = p
			c, e = p.CreateDatabaseConnection(t.Context(), rc, target, req)
			if e != nil || c.Phase != ConnectionReady {
				t.Fatalf("resume: %+v %v", c, e)
			}
			if pg.creates != 1 {
				t.Fatalf("recreated identity %d times", pg.creates)
			}
			cred, _ = readConnectionCredential(t.Context(), rc, req.Name)
			if !cred.Rotated {
				t.Fatal("root not rotated")
			}
			pg.onDelete = func() {
				joined := strings.Join(paths, "\n")
				if !strings.Contains(joined, "DELETE /v1/database/config/orders") || !strings.Contains(joined, "PUT /v1/sys/leases/revoke-prefix/database/creds/orders_") {
					t.Fatal("target identity deleted before OpenBao config/leases")
				}
			}
			if e = p.DeleteDatabaseConnection(t.Context(), rc, target, req.Name); e != nil {
				t.Fatal(e)
			}
			if pg.deletes != 1 || len(pg.roles) != 0 {
				t.Fatal("target not released")
			}
			joined := strings.Join(paths, "\n")
			if !strings.Contains(joined, "DELETE /v1/database/config/orders") || !strings.Contains(joined, "PUT /v1/sys/leases/revoke-prefix/database/creds/orders_") {
				t.Fatal("config or leases not removed before target cleanup")
			}
			records, e := p.ListDatabaseConnections(t.Context(), rc)
			if e != nil || len(records) != 0 {
				t.Fatalf("record not removed: %+v %v", records, e)
			}
		})
	}
}

func TestConnectionDeleteRevokesEveryRoleBeforeConfiguration(t *testing.T) {
	for _, fail := range []string{"", "revoke-prefix/database/creds/orders_writer"} {
		t.Run(fail, func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			target, pg := connectionTargetFixture(rc)
			record := DatabaseConnection{
				Name: "orders", TargetCluster: "target", TargetProvider: "postgres",
				MountPath: "database", Phase: ConnectionReady,
				Roles: []DatabaseRole{
					{DatabaseRoleWrite: DatabaseRoleWrite{Name: "reader"}},
					{DatabaseRoleWrite: DatabaseRoleWrite{Name: "writer"}},
				},
			}
			if err := saveConnections(t.Context(), rc, []DatabaseConnection{record}); err != nil {
				t.Fatal(err)
			}
			if err := saveConnectionCredential(t.Context(), rc, record.Name, connectionCredential{Username: "identity", Step: "ready"}); err != nil {
				t.Fatal(err)
			}
			var paths []string
			connectionNative(t, rc, a, fail, &paths)
			err := p.DeleteDatabaseConnection(t.Context(), rc, target, record.Name)
			revokes := []string{
				"PUT /v1/sys/leases/revoke-prefix/database/creds/orders_reader",
				"PUT /v1/sys/leases/revoke-prefix/database/creds/orders_writer",
			}
			if fail != "" {
				if err == nil || pg.deletes != 0 || !slices.Equal(paths, revokes) {
					t.Fatalf("failed revoke must preserve all configuration and target access: %v %v", paths, err)
				}
				return
			}
			expected := append(revokes,
				"DELETE /v1/database/roles/orders_reader",
				"DELETE /v1/database/roles/orders_writer",
				"DELETE /v1/database/config/orders",
				"DELETE /v1/sys/mounts/database")
			if err != nil || pg.deletes != 1 || !slices.Equal(paths, expected) {
				t.Fatalf("unsafe deletion ordering: %v %v", paths, err)
			}
		})
	}
}
