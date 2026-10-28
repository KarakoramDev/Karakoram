// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestCitusInitialRestoreCanClaimBeforeFormationReceipt(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
	s.Formed = false
	s.GroupFormed = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := saveBackupState(rc.Cluster, backupState{InitialQueued: true, ManagedRolesHeld: true}); err != nil {
		t.Fatal(err)
	}
	allowed, err := p.CanClaimOperation(rc.Cluster)
	if err != nil || !allowed {
		t.Fatalf("queued initial restore cannot run before its own formation/application receipt: allowed=%v err=%v", allowed, err)
	}
}

func TestCitusInitialRestoreRewritesEveryGroupBeforeValidationAndRelease(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "deleted-source", Backup: "base"}
	s.Formed, s.GroupFormed = false, nil
	worker := *s.data()[1]
	worker.Name, worker.Group = "worker2", 2
	s.Members = append(s.Members, &worker)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := saveBackupState(rc.Cluster, backupState{InitialQueued: true, CloneCredentials: true, ManagedRolesHeld: true}); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: "deleted-source", Name: "base", Provider: "postgres", Storage: "archive", Type: "full", SourceVersion: rc.Cluster.Spec.Version, SourceTopology: rc.Cluster.Spec.Topology, Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "base", Repository: "source/g0", RestorePoint: "marker"}}
	for _, group := range s.groups() {
		b.Postgres.Groups = append(b.Postgres.Groups, domain.PostgresBackupGroup{Group: group.id, Backup: domain.PostgresBackup{Label: "base", Repository: "source/group"}})
	}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "initial-restore", SourceNamespace: rc.Cluster.Namespace, Source: domain.RestoreSource{SourceCluster: "deleted-source", Backup: "base"}}
	op := &domain.Operation{ID: "initial-clone", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"initial-restore"}`)}
	if err := rc.Store.EnqueueRestore(t.Context(), r, op); err != nil {
		t.Fatal(err)
	}
	rewritten, validated := map[string]bool{}, map[string]bool{}
	loseWorker := true
	installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
		name := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		bs, _ := loadBackupState(rc.Cluster)
		switch req.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: name, Version: rc.Cluster.Spec.Version})
		case "/v1/postgres/citus-restore-metadata":
			if !bs.ManagedRolesHeld || bs.InitialApplied {
				t.Error("rewrite ran outside recovery fences")
			}
			var input pgspec.CitusRestoreMetadataRequest
			if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if len(input.Nodes) != 3 {
				t.Error("missing target group mappings", input)
			}
			for _, node := range input.Nodes {
				if member := s.find(node.Host); member == nil || member.Group != node.GroupID {
					t.Error("source hostname or incorrect group in rewrite", node)
				}
			}
			rewritten[name] = true
			if name == "worker2" && loseWorker {
				loseWorker = false
				http.Error(w, "response lost after local commit", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/citus-status":
			if len(rewritten) != 3 || !bs.ManagedRolesHeld {
				t.Error("validation preceded whole-formation rewrite or followed access release")
			}
			var nodes []pgspec.CitusNode
			for _, member := range s.data() {
				nodes = append(nodes, pgspec.CitusNode{GroupID: member.Group, Host: member.Name, Port: 5432, Active: true, Role: "primary"})
			}
			validated[name] = true
			json.NewEncoder(w).Encode(pgspec.CitusStatusResponse{Nodes: nodes})
		case "/v1/postgres/finish-restore":
			if len(validated) != 3 || !bs.ManagedRolesHeld {
				t.Error("archive/access transition preceded validation")
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/rekey", "/v1/postgres/stanza-create", "/v1/reload":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			t.Error("unexpected clone action", req.URL.Path)
			http.NotFound(w, req)
		}
	})
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "other", Kind: domain.OperationRestart}); err == nil {
		t.Fatal("unformed initial target admitted unrelated imperative work")
	}
	done, lost := false, false
	for range 40 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			if lost || result.Step != "citus-metadata" {
				t.Fatal(err)
			}
			lost = true
			continue
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	if !done || !lost || len(validated) != 3 {
		t.Fatal("clone did not resume the exact metadata barrier", done, lost, validated)
	}
	bs, _ := loadBackupState(rc.Cluster)
	if !bs.InitialApplied || !bs.CloneComplete || bs.ManagedRolesHeld || bs.RestoreOperation != "" {
		t.Fatal("clone did not publish final application receipt after releasing access", bs)
	}
}

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
	rekeyed := map[string]bool{}
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
		case "/v1/postgres/rekey":
			if starts != 2 {
				t.Error("rekey before every group seed started")
			}
			rekeyed[strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")] = true
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/reload":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/citus-restore-metadata":
			bs, _ := loadBackupState(rc.Cluster)
			if !bs.ManagedRolesHeld || len(rekeyed) != 2 {
				t.Error("catalog rewrite must follow local rekey under client fences")
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/citus-status":
			if len(rekeyed) != 2 {
				t.Error("metadata/access barrier before every group rekey", rekeyed)
			}
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

func TestCitusRecoveredPasswordsReachEveryPrimaryBeforeRelease(t *testing.T) {
	p, rc, _ := citusOpsFixture(t)
	if err := saveBackupState(rc.Cluster, backupState{ManagedRolesHeld: true}); err != nil {
		t.Fatal(err)
	}
	applied := map[string]string{}
	failWorker := true
	installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")})
		case "/v1/postgres/roles":
			var input pgspec.AccessRequest
			if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.Operation != "recover-password-local" || input.Role.Password == nil {
				t.Error("recovery overwrote role attributes")
			}
			member := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
			if member == "worker1" && failWorker {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			applied[member] = *input.Role.Password
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			t.Error("unexpected action", req.URL.Path)
		}
	})
	verifier := "same-target-verifier"
	request := pgspec.AccessRequest{Operation: "password", Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "application"}, Password: &verifier}}
	if err := p.recoveredRolePassword(t.Context(), rc, request); err == nil {
		t.Fatal("partial recovery was accepted")
	}
	bs, _ := loadBackupState(rc.Cluster)
	if !bs.ManagedRolesHeld {
		t.Fatal("failed worker rekey released client fences")
	}
	failWorker = false
	if err := p.recoveredRolePassword(t.Context(), rc, request); err != nil {
		t.Fatal(err)
	}
	if len(applied) != 2 {
		t.Fatal("not every group received credentials", applied)
	}
	for _, value := range applied {
		if value != verifier {
			t.Fatal("group credentials differ")
		}
	}
}

func TestRekeyNativeRejectionIsPermanentButUnavailableRetries(t *testing.T) {
	for _, code := range []int{http.StatusUnprocessableEntity, http.StatusServiceUnavailable, http.StatusConflict} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			_, rc, s := citusOpsFixture(t)
			installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
				http.Error(w, "private native SQL diagnostic", code)
			})
			err := pgAction(t.Context(), rc, s.data()[0].Name, "rekey", struct{}{}, nil)
			if err == nil || errors.Is(err, engine.ErrInvalid) != (code == http.StatusUnprocessableEntity) {
				t.Fatalf("incorrect permanent rekey classification: %v", err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("native diagnostics leaked")
			}
		})
	}
}

func TestCitusRecoveryWithLegacyAgentFailsClosedWithoutDowngrade(t *testing.T) {
	p, rc, _ := citusOpsFixture(t)
	if err := saveBackupState(rc.Cluster, backupState{ManagedRolesHeld: true}); err != nil {
		t.Fatal(err)
	}
	requests := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")})
		case "/v1/postgres/roles":
			requests++
			var input pgspec.AccessRequest
			if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			// Model the old agent's strict operation validation: the new
			// operation is rejected before SQL, rather than ignored.
			if input.Operation == "recover-password-local" {
				http.Error(w, "invalid PostgreSQL access action", http.StatusBadRequest)
				return
			}
			t.Error("new control plane downgraded coordinated-local intent")
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			t.Error("unexpected action", req.URL.Path)
		}
	})
	verifier := "target-verifier"
	for _, operation := range []string{"password", "recover-password"} {
		request := pgspec.AccessRequest{Operation: operation, Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "application"}, Password: &verifier}}
		if err := p.recoveredRolePassword(t.Context(), rc, request); err == nil {
			t.Fatal("old-agent rejection accepted as successful repair")
		}
		bs, err := loadBackupState(rc.Cluster)
		if err != nil || !bs.ManagedRolesHeld {
			t.Fatal("legacy agent rejection released recovery fences", err)
		}
	}
	if requests != 2 {
		t.Fatal("unexpected fallback or fan-out after rejection", requests)
	}
}
