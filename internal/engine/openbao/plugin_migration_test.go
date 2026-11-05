// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestPluginInstallAndRegistrationSurviveRestart(t *testing.T) {
	for _, seal := range []string{"shamir", "awskms"} {
		t.Run(seal, func(t *testing.T) {
			options := Options{}
			options.Seal.Type = seal
			plugins := memberPlugins(options, pluginRevision)
			if len(plugins) < 2 || plugins[0].Version != "v0.0.1" || plugins[1].Version != mongodbPluginVersion || plugins[0].Name != plugins[1].Name || plugins[0].Command == plugins[1].Command {
				t.Fatalf("unsafe dual registration: %+v", plugins)
			}
			preparation, err := pluginPrepare(options, "runtime")
			if err != nil {
				t.Fatal(err)
			}
			commands := strings.Join(preparation, "\n")
			for _, name := range []string{"openbao-plugin-database-mongodb-0.0.1", "openbao-plugin-database-mongodb"} {
				a, err := nativeartifacts.Get(name)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(commands, a.SHA256) || !strings.Contains(commands, a.UpstreamURL) {
					t.Fatalf("missing verified binary %s", name)
				}
			}
			if !strings.Contains(commands, pluginDirectory+"/mongodb-0.0.2") || !strings.Contains(commands, pluginDirectory+"/"+mongodbPluginCommand) {
				t.Fatal("new binary overwrites old protocol filename")
			}
			// OpenBao resolves symlinks then requires the executable's immediate
			// parent to be plugin_directory for non-OCI registrations.
			publication := preparation[2]
			want := "sudo -u 'runtime' install -m 0755 " + pluginDirectory + "/mongodb-0.0.2/openbao-plugin-database-mongodb_linux_amd64_v1 " + pluginDirectory + "/" + mongodbPluginCommand
			if publication != want {
				t.Fatalf("new plugin must be a real root-directory executable: %s", publication)
			}
			// Member configuration serialization/restart retains both independent commands.
			raw, err := json.Marshal(baospec.MemberSpec{Plugins: plugins})
			if err != nil {
				t.Fatal(err)
			}
			var restored baospec.MemberSpec
			if err = json.Unmarshal(raw, &restored); err != nil || len(restored.Plugins) != len(plugins) {
				t.Fatal(err)
			}
			if seal == "awskms" && restored.Plugins[2].Version != "v0.1.1" {
				t.Fatal("KMS pin lost")
			}
		})
	}
}

func TestLegacyMemberPluginDeclarationsWaitForPreparationReceipt(t *testing.T) {
	for _, seal := range []string{"shamir", "awskms"} {
		t.Run(seal, func(t *testing.T) {
			// Unlike ready(), this fixture starts with persisted pre-upgrade state:
			// OpenBao 2.7.1, no plugin revision, and no current-artifact receipt.
			p, rc, s := edgeFixture(t)
			m := s.Members[0]
			m.EngineOptions = json.RawMessage(`{"seal":{"type":"` + seal + `"}}`)
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			legacy := []baospec.Plugin{{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_linux_amd64_v1", Version: "v0.0.1"}}
			if seal == "awskms" {
				legacy = append(legacy, baospec.Plugin{Type: "kms", Name: "awskms", Command: "openbao-plugin-kms-aws_linux_amd64_v1", Version: "v0.1.0"})
			}
			for _, plugin := range legacy {
				if err := os.WriteFile(filepath.Join(directory, plugin.Command), []byte("legacy artifact"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			pull := func(want []baospec.Plugin) {
				t.Helper()
				// Recreate the provider to exercise only durable installed state.
				p = New(p.options)
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec baospec.MemberSpec
				if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(spec.Plugins, want) {
					t.Fatalf("published unprepared plugin declarations: got %+v, want %+v", spec.Plugins, want)
				}
				// Model the executable lookup at restart/unseal without launching
				// native binaries: every cached declaration must resolve locally.
				for _, plugin := range spec.Plugins {
					info, err := os.Stat(filepath.Join(directory, plugin.Command))
					if err != nil || info.Mode()&0111 == 0 {
						t.Fatalf("restart declaration %s has no executable: %v", plugin.Command, err)
					}
				}
			}
			pull(legacy)
			if _, err := os.Stat(filepath.Join(directory, mongodbPluginCommand)); !os.IsNotExist(err) {
				t.Fatalf("legacy fixture unexpectedly has new executable: %v", err)
			}
			// Submitting preparation does not activate its declarations. Only the
			// successful platform receipt used by rollout may advance the set.
			f := &fakeInfra{}
			rc.Backend = f
			v := &pending{Action: "deploy", Member: m.Name, Op: infra.Op{ID: "preparing"}, PluginRevision: pluginRevision}
			if done, err := p.observeReceipt(t.Context(), rc, s, v); err != nil || done {
				t.Fatalf("incomplete preparation: %v %v", done, err)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			pull(legacy)
			options := Options{}
			options.Seal.Type = seal
			current := memberPlugins(options, pluginRevision)
			for _, plugin := range current {
				if err := os.WriteFile(filepath.Join(directory, plugin.Command), []byte("prepared artifact"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			f.done = true
			if done, err := p.observeReceipt(t.Context(), rc, s, v); err != nil || !done {
				t.Fatalf("completed preparation: %v %v", done, err)
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			pull(current)
			// Preparing this member must not publish new declarations on a peer.
			cfg, err := p.MemberConfig(t.Context(), rc, s.Members[1].Name)
			if err != nil {
				t.Fatal(err)
			}
			var peer baospec.MemberSpec
			if err = json.Unmarshal(cfg.EngineSpec, &peer); err != nil || len(peer.Plugins) != 1 || peer.Plugins[0].Version != "v0.0.1" {
				t.Fatalf("preparation leaked to unprepared peer: %+v %v", peer.Plugins, err)
			}
		})
	}
}

func TestFreshMemberActivatesPreparedPluginDeclarations(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	m := state(t, rc).Members[0]
	if m.PluginRevision != pluginRevision || len(f.deployments) != 1 {
		t.Fatalf("fresh member has no preparation receipt: %+v", m)
	}
	preparation := strings.Join(f.deployments[0].Runtime.Prepare, "\n")
	if !strings.Contains(preparation, pluginDirectory+"/"+mongodbPluginCommand) {
		t.Fatal("fresh deployment did not prepare the new executable")
	}
	cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec baospec.MemberSpec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil || !reflect.DeepEqual(spec.Plugins, memberPlugins(Options{}, pluginRevision)) {
		t.Fatalf("fresh member did not activate prepared plugins: %+v %v", spec.Plugins, err)
	}
}

func TestReadyMongoDBConnectionMigration(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	record := DatabaseConnection{Name: "orders", TargetProvider: "psmdb", MountPath: "database", Phase: ConnectionReady}
	if err := saveConnections(t.Context(), rc, []DatabaseConnection{record, {Name: "pg", TargetProvider: "postgres", Phase: ConnectionReady}}); err != nil {
		t.Fatal(err)
	}
	name := state(t, rc).Members[0].Name
	cert, err := memberCertificate(t.Context(), rc, "tls/members/"+name+".pem")
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	version, writes, fail := "v0.0.1", 0, true
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sys/plugins/catalog/database/mongodb-database-plugin":
			if r.URL.Query().Get("version") != mongodbPluginVersion {
				t.Error("missing catalog version gate")
			}
			w.WriteHeader(204)
		case "/v1/database/config/orders":
			if r.Method == "GET" {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"plugin_name": "mongodb-database-plugin", "plugin_version": version}})
				return
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if len(body) != 2 || body["plugin_version"] != mongodbPluginVersion || body["verify_connection"] != true {
				t.Errorf("not a partial version migration: %v", body)
			}
			if fail {
				w.WriteHeader(503)
				return
			}
			writes++
			version = mongodbPluginVersion
			w.WriteHeader(204)
		default:
			if strings.Contains(r.URL.Path, "/database/") || r.URL.Path == "/v1/sys/mounts" || strings.Contains(r.URL.Path, "/sys/leases") {
				t.Errorf("migration touched unmanaged mounts or leases: %s", r.URL.Path)
			}
			a.serveNative(name, w, r)
		}
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	server.StartTLS()
	t.Cleanup(server.Close)
	a.nativeServers[name] = server
	// Mixed-runtime rollout cannot migrate a config that a surviving old member
	// might need after failover, even if the shared catalog already has v0.0.2.
	s := state(t, rc)
	s.Members[0].PluginRevision = ""
	if err = s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if busy, err := p.reconcileConnections(t.Context(), rc); err != nil || busy || writes != 0 {
		t.Fatalf("premature migration: %v %v", busy, err)
	}
	receipt := &pending{Action: "deploy", Member: name, Op: infra.Op{ID: "prepared"}, PluginRevision: pluginRevision}
	if done, err := p.observeReceipt(t.Context(), rc, s, receipt); err != nil || !done {
		t.Fatalf("preparation receipt: %v %v", done, err)
	}
	// Installed declarations alone do not release the managed migration gate:
	// the member must also finish its rollout and recover health.
	s.Members[0].ResumeDeploy = receipt
	if err = s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if busy, err := p.reconcileConnections(t.Context(), rc); err != nil || busy || writes != 0 {
		t.Fatalf("migration during member recovery: %v %v", busy, err)
	}
	s.Members[0].ResumeDeploy = nil
	if err = s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if busy, err := p.reconcileConnections(t.Context(), rc); !busy || err == nil || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("per-connection error missing: %v %v", busy, err)
	}
	fail = false
	// A fresh provider retries from native config, not an in-memory marker.
	p = New(p.options)
	if busy, err := p.reconcileConnections(t.Context(), rc); err != nil || !busy || writes != 1 {
		t.Fatalf("migration failed: %v %v %d", busy, err, writes)
	}
	if busy, err := p.reconcileConnections(t.Context(), rc); err != nil || busy || writes != 1 {
		t.Fatalf("migration not idempotent: %v %v %d", busy, err, writes)
	}
}

func TestNewMongoDBConnectionPinsCurrentPlugin(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	target, _ := connectionTargetFixture(rc)
	target.Context.Cluster.Status.Connection.Hosts = []string{"mongo"}
	ca, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	if err = rc.Secrets.SetClusterSecret(t.Context(), target.Context.Cluster.Ref(), "tls/ca.crt", ca); err != nil {
		t.Fatal(err)
	}
	found := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sys/mounts" {
			_, _ = w.Write([]byte(`{"data":{"database/":{}}}`))
			return
		}
		if r.URL.Path == "/v1/database/config/orders" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			found = body["plugin_name"] == "mongodb-database-plugin" && body["plugin_version"] == mongodbPluginVersion
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(server.Close)
	client, err := baoapi.New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err = p.configureConnection(t.Context(), rc, target, client, DatabaseConnection{Name: "orders", TargetProvider: "psmdb", MountPath: "database"}, connectionCredential{}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("new connection did not pin current plugin")
	}
}
