// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func TestOfflineJobsRemainStartableAfterConfigPull(t *testing.T) {
	for _, kind := range []string{"backup", "restore", "clone"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			converge(t, p, rc)
			storage, s3 := setupDumpStore(t, rc)
			b := successfulDump(t, rc, storage, s3)
			var op *domain.Operation
			if kind == "backup" {
				b = newDumpBackup(t, rc, storage, "next")
				op = &domain.Operation{ID: "backup-config-pull", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"next"}`)}
			} else {
				name := "restore"
				if kind == "clone" {
					name = "initial-restore"
					rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
				}
				op = createRestore(t, rc, b, name)
			}
			var applied agentapi.MemberConfig
			pull := func() {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Error(err)
					return
				}
				applied, err = p.MemberConfig(t.Context(), rc, s.Member.Name)
				if err != nil {
					t.Error(err)
				}
			}
			pull()
			manifest := manifestFor(b.PBMPrefix)
			pulls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/neo4j/backup", "/v1/neo4j/restore":
					// Model a member restart/config pull while the receipt owns its stop hold.
					// Node.Start and readiness both honor the latest member maintenance flag.
					pull()
					pulls++
					if applied.Maintenance || !reflect.DeepEqual(applied.DesiredProcesses, []string{neo4jspec.Process}) {
						t.Error("control-plane operation fence prevented the member job from restarting Neo4j")
					}
					json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobDone, Manifest: &manifest})
				case "/v1/neo4j/readiness":
					pull()
					pulls++
					startable := !applied.Maintenance && reflect.DeepEqual(applied.DesiredProcesses, []string{neo4jspec.Process})
					json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: startable, Version: rc.Cluster.Spec.Version})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			runDumpOperation(t, p, rc, op)
			if pulls == 0 {
				t.Fatal("job never pulled member configuration")
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.Maintenance != "" || s.RestoreRecovery != nil {
				t.Fatal("operation did not release its control-plane fence")
			}
			if kind == "clone" && !s.CloneComplete {
				t.Fatal("clone did not complete")
			}
		})
	}
}

func TestExplicitOfflineMemberStillStopsDesiredProcess(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Member.Offline = true
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Maintenance || len(cfg.DesiredProcesses) != 0 {
		t.Fatal("explicit offline intent no longer fences member startup")
	}
}
