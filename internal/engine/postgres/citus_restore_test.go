// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestCitusRestoreFencesWholeFormationUntilMetadataBarrier(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	s.Members = append(s.Members, &member{Name: "proxy", Role: "proxy", Stage: "joined"})
	save(rc.Cluster, s)
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "base", Provider: "postgres", Storage: "archive", Type: "full", SourceVersion: rc.Cluster.Spec.Version, SourceTopology: rc.Cluster.Spec.Topology, Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "coordinator", Repository: "source/g0", RestorePoint: "marker", Groups: []domain.PostgresBackupGroup{{Group: 0, Backup: domain.PostgresBackup{Label: "coordinator", Repository: "source/g0"}, LSN: "0/100"}, {Group: 1, Backup: domain.PostgresBackup{Label: "worker", Repository: "source/g1"}, LSN: "0/200"}}}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "recover", Source: domain.RestoreSource{RestorePoint: "marker"}, Phase: domain.PhasePending}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	validMetadata := false
	stopped, wiped, resets, starts := 0, 0, 0, 0
	stanzas := map[string]bool{}
	finished := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "), Timeline: 1, Version: rc.Cluster.Spec.Version})
		case "/v1/postgres/prepare-restore":
			var input pgspec.PrepareRestoreRequest
			json.NewDecoder(req.Body).Decode(&input)
			if input.Wipe {
				if stopped != 2 || resets != 2 {
					t.Error("wipe before whole formation stopped and reset")
				}
				wiped++
			} else {
				stopped++
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/dcs-reset":
			if stopped != 2 {
				t.Error("DCS reset before all groups fenced")
			}
			resets++
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/processes/patroni/start":
			if wiped != 2 {
				t.Error("start before every group wiped")
			}
			starts++
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/stanza-create":
			var input pgspec.RepositoryRequest
			if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			stanzas[input.Repository.Path] = true
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/finish-restore":
			if len(stanzas) != 2 {
				t.Error("archiving enabled before every fresh group stanza exists", stanzas)
			}
			finished++
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/reload", "/v1/postgres/rekey":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/citus-status":
			nodes := []pgspec.CitusNode{{NodeID: 1, GroupID: 0, Host: s.data()[0].Name, Port: 5432, Active: true}, {NodeID: 2, GroupID: 1, Host: "worker1", Port: 5432, Active: true}}
			if !validMetadata {
				nodes[1].Host = "wrong-worker"
			}
			json.NewEncoder(w).Encode(pgspec.CitusStatusResponse{IsCoordinator: true, Nodes: nodes})
		default:
			t.Errorf("unexpected action %s", req.URL.Path)
			http.NotFound(w, req)
		}
	})
	op := &domain.Operation{ID: "formation-restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
	reachedBarrier := false
	for range 40 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			if result.Step != "citus-metadata" {
				t.Fatal(err)
			}
			reachedBarrier = true
			break
		}
		op.Checkpoint = result.Checkpoint
		if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
			t.Fatal("restore does not own formation", err)
		}
	}
	if !reachedBarrier || starts != 2 {
		t.Fatal("did not restore every group before metadata barrier", starts)
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil || !bs.ManagedRolesHeld {
		t.Fatal("access released before metadata verification", err)
	}
	if _, err = p.RunOperation(t.Context(), rc, &domain.Operation{ID: "other", Kind: "rebalance"}); err == nil {
		t.Fatal("concurrent operation bypassed formation fence")
	}
	validMetadata = true
	for range 15 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			break
		}
	}
	r, err = rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
	if err != nil || r.Phase != domain.PhaseSucceeded {
		t.Fatal(r, err)
	}
	if finished != 2 {
		t.Fatal("did not enable archiving on every group", finished)
	}
	bs, err = loadBackupState(rc.Cluster)
	if err != nil || bs.ManagedRolesHeld {
		t.Fatal("access hold leaked", err)
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
		t.Fatal("formation fence leaked", err)
	}
}

func TestCitusRestoreRejectsTimestampAndMismatchedClone(t *testing.T) {
	p, rc, _ := citusOpsFixture(t)
	if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Time: time.Now().UTC().Format(time.RFC3339Nano)}); err == nil {
		t.Fatal("timestamp PITR admitted")
	}
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "different-groups", Provider: "postgres", Storage: "archive", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, SourceTopology: rc.Cluster.Spec.Topology, Postgres: &domain.PostgresBackup{Label: "base", Repository: "source/g0", RestorePoint: "marker", Groups: []domain.PostgresBackupGroup{{Group: 0, Backup: domain.PostgresBackup{Label: "base", Repository: "source/g0"}}}}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Backup: b.Name}); err == nil {
		t.Fatal("mismatched clone group count admitted")
	}
	b.Postgres.Groups = append(b.Postgres.Groups, domain.PostgresBackupGroup{Group: 1, Backup: domain.PostgresBackup{Label: "worker-base", Repository: "source/g1"}})
	sp := pgspec.Spec{Citus: &pgspec.Citus{Group: 1}}
	var restore pgspec.Restore
	if err := p.configureCitusRestore(t.Context(), rc, &sp, b, &restore); err != nil {
		t.Fatal(err)
	}
	if restore.Set != "worker-base" || restore.TargetName != "marker" || restore.TargetTime != "" || restore.Source.Path != "/source/g1" {
		t.Fatal("restore did not select worker named target", restore)
	}
	status := pgspec.CitusStatusResponse{Nodes: []pgspec.CitusNode{{Host: "foreign", Port: 5432, Active: true}}}
	s, _ := load(rc.Cluster)
	if err := verifyCitusRestoreMetadata(s, status); err == nil {
		t.Fatal("foreign metadata admitted")
	}
}
