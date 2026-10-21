// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func citusOpsFixture(t *testing.T) (*Core, *engine.Context, *state) {
	t.Helper()
	p, rc, s := backupFixture(t, "single")
	s.Topology = "distributed"
	s.GroupFormed = map[int]bool{0: true, 1: true}
	s.Formed = true
	for _, m := range s.Members {
		m.Topology = "distributed"
	}
	rc.Cluster.Spec.Topology.Type = "distributed"
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1}}`)
	worker := *s.data()[0]
	worker.Name = "worker1"
	worker.Group = 1
	s.Members = append(s.Members, &worker)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	return p, rc, s
}

func TestCitusBackupOrdersAllBasesBeforeMarkerAndArchiveBarrier(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1},"profiles":["postgis","postgis"]}`)
	backups, points, switches, archives := 0, 0, 0, 0
	cover := false
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), Timeline: 1, Version: "17.11.1", SystemID: "system"})
		case "/v1/postgres/stanza-create", "/v1/reload":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/backup":
			var req pgspec.BackupRequest
			json.NewDecoder(r.Body).Decode(&req)
			if req.Type != "full" || req.Repository == nil || !strings.Contains(req.Repository.Path, "/g") {
				t.Error("unscoped group backup", req)
			}
			backups++
			json.NewEncoder(w).Encode(pgspec.BackupResponse{State: "done", Label: "base"})
		case "/v1/postgres/backup-info":
			json.NewEncoder(w).Encode(pgspec.BackupInfoResponse{Backups: []pgspec.BackupInfo{{Label: "base", Version: "17", SystemID: "system"}}})
		case "/v1/postgres/citus-restore-point":
			if backups != 2 {
				t.Error("marker before all group backups", backups)
			}
			points++
			json.NewEncoder(w).Encode(pgspec.CitusRestorePointResponse{Nodes: []pgspec.CitusRestorePoint{{Host: s.data()[0].Name, LSN: "0/100"}, {Host: "worker1", LSN: "0/200"}}})
		case "/v1/postgres/wal-switch":
			if points != 1 {
				t.Error("WAL switch before marker")
			}
			switches++
			json.NewEncoder(w).Encode(pgspec.WALSwitchResponse{LSN: "0/300"})
		case "/v1/postgres/archive-covers":
			if switches != 2 {
				t.Error("archive check before all switches")
			}
			archives++
			json.NewEncoder(w).Encode(pgspec.ArchiveCoversResponse{Covers: cover})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "coordinated", Storage: "archive", Type: "full", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "coordinated", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"coordinated"}`)}
	for range 20 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if archives > 0 {
			break
		}
	}
	b, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil {
		t.Fatal(err)
	}
	if b.Phase == domain.PhaseSucceeded {
		t.Fatal("published backup before all archives cover the marker")
	}
	cover = true
	for range 10 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			break
		}
	}
	b, err = rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil || b.Phase != domain.PhaseSucceeded || b.Postgres == nil || b.Postgres.RestorePoint == "" || len(b.Postgres.Groups) != 2 {
		t.Fatal(b, err)
	}
	if b.Postgres.Groups[1].LSN != "0/200" {
		t.Fatal("worker WAL upper bound lost")
	}
	if strings.Join(b.Postgres.Profiles, ",") != "postgis" || strings.Join(b.Postgres.Groups[1].Backup.Profiles, ",") != "postgis" {
		t.Fatal("coordinated profile provenance lost", b.Postgres)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1},"profiles":["pgvector","postgis"]}`)
	// A quarter-hour PITR target reuses group bases rather than running more backups.
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	next := b.CompletedAt.Add(16 * time.Minute)
	rc.Clock = importReplayClock{at: next}
	if _, err := p.scheduleCitusPoint(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	items, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
	if err != nil || len(items) != 2 {
		t.Fatal(items, err)
	}
	for _, item := range items {
		if item.Trigger == "pitr" && (strings.Join(item.Postgres.Profiles, ",") != "pgvector,postgis" || strings.Join(item.Postgres.Groups[1].Backup.Profiles, ",") != "pgvector,postgis") {
			t.Fatal("scheduled PITR point lost profile requirements", item.Postgres)
		}
	}
	if _, err := p.scheduleCitusPoint(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	items, err = rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
	if err != nil || len(items) != 2 {
		t.Fatal("duplicate scheduled point", err)
	}
}

func TestCitusPointDeletionKeepsSharedGroupBases(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	base := domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "base", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "base0", Repository: "source/g0", Groups: []domain.PostgresBackupGroup{{Group: 0, Backup: domain.PostgresBackup{Label: "base0", Repository: "source/g0"}}, {Group: 1, Backup: domain.PostgresBackup{Label: "base1", Repository: "source/g1"}}}}}
	if err := rc.Store.CreateBackup(t.Context(), &base); err != nil {
		t.Fatal(err)
	}
	point := base
	point.Name = "point"
	point.Trigger = "pitr"
	if err := rc.Store.CreateBackup(t.Context(), &point); err != nil {
		t.Fatal(err)
	}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		t.Error("point deletion touched a native shared base")
		http.NotFound(w, r)
	})
	result, err := p.runDeleteCitusBackup(t.Context(), rc, &domain.Operation{}, backupCheckpoint{}, s, &point, &domain.BackupStorage{})
	if err != nil || !result.Done {
		t.Fatal(result, err)
	}
	if _, err = rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), base.Name); err != nil {
		t.Fatal("shared base removed", err)
	}
	if _, err = rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), point.Name); !missing(err) {
		t.Fatal("point not removed", err)
	}
}

func TestCitusBackupPromotionAbortsAfterOutstandingReceipts(t *testing.T) {
	p, rc, _ := citusOpsFixture(t)
	timeline := 1
	running, started := true, false
	observations := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), Timeline: timeline, Version: "17.11.1", SystemID: "system"})
		case "/v1/postgres/stanza-create", "/v1/reload":
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/postgres/backup":
			var req pgspec.BackupRequest
			json.NewDecoder(r.Body).Decode(&req)
			state := "absent"
			if !req.ObserveOnly {
				started = true
				state = "running"
			} else {
				observations++
				if strings.HasSuffix(req.Receipt, "/g0") {
					state = "failed"
					if running {
						state = "running"
					}
				}
			}
			json.NewEncoder(w).Encode(pgspec.BackupResponse{State: state})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "promoted", Storage: "archive", Type: "full", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: b.Name, Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"promoted"}`)}
	for range 10 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if started {
			break
		}
	}
	if !started {
		t.Fatal("native backup not started")
	}
	timeline = 2
	result, err := p.RunOperation(t.Context(), rc, op)
	if err == nil {
		t.Fatal("promotion was accepted")
	}
	op.Checkpoint = result.Checkpoint
	op.Message = err.Error()
	if _, err = p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil || s.CitusOperation != op.ID || s.RestoreRecovery == nil || observations == 0 {
		t.Fatal("outstanding receipt lost its formation fence", s, err)
	}
	running = false
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil || s.CitusOperation != "" || s.RestoreRecovery != nil {
		t.Fatal("terminal receipts did not release formation", s, err)
	}
	failed, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil || failed.Phase != domain.PhaseFailed || failed.CompletedAt == nil || failed.Error == "" {
		t.Fatal("failed aggregate not recorded", failed, err)
	}
	next := *b
	next.Name = "after-promotion"
	if err = rc.Store.CreateBackup(t.Context(), &next); err != nil {
		t.Fatal(err)
	}
	nextOp := &domain.Operation{ID: next.Name, Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"after-promotion"}`)}
	for range 2 {
		result, err = p.RunOperation(t.Context(), rc, nextOp)
		if err != nil {
			t.Fatal("new backup blocked after terminal abort", err)
		}
		nextOp.Checkpoint = result.Checkpoint
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(nextOp.Checkpoint, &cp); err != nil || cp.Citus == nil || cp.Citus.Timelines[0] != 2 {
		t.Fatal("new backup did not capture promoted timeline", err)
	}
}

func TestCitusPointDefersObsoleteFormationBases(t *testing.T) {
	for _, layout := range []string{"missing", "extra", "duplicate", "retired", "current"} {
		t.Run(layout, func(t *testing.T) {
			p, rc, s := citusOpsFixture(t)
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			storage, err := p.backupStorage(t.Context(), rc, "archive", rc.Cluster.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			groups := []domain.PostgresBackupGroup{
				{Group: 0, Backup: domain.PostgresBackup{Label: "base0", Repository: groupRepoPrefix(storage, rc.Cluster, s, 0)}},
				{Group: 1, Backup: domain.PostgresBackup{Label: "base1", Repository: groupRepoPrefix(storage, rc.Cluster, s, 1)}},
			}
			switch layout {
			case "missing":
				groups = groups[:1]
			case "extra":
				groups = append(groups, domain.PostgresBackupGroup{Group: 2, Backup: domain.PostgresBackup{Label: "base2", Repository: groupRepoPrefix(storage, rc.Cluster, s, 2)}})
			case "duplicate":
				groups[1] = groups[0]
			case "retired":
				s.GroupRepositoryIncarnations = map[int]int{1: 1}
			}
			completed := now(rc).Add(-time.Hour)
			b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "base", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded, CompletedAt: &completed, Postgres: &domain.PostgresBackup{Groups: groups}}
			if err = rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if _, err = p.scheduleCitusPoint(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			items, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
			want := 1
			if layout == "current" {
				want = 2
			}
			if err != nil || len(items) != want {
				t.Fatal("scheduler selected obsolete formation bases", len(items), want, err)
			}
		})
	}
}

func TestCitusRecreatedWorkerBackupCapturesFreshRepository(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	storage, err := p.backupStorage(t.Context(), rc, "archive", rc.Cluster.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	old := groupRepoPrefix(storage, rc.Cluster, s, 1)
	worker := s.group(1).data()[0]
	s.Members = s.group(0).data()
	s.CitusScale = &citusScaleState{Group: 1, Stage: "retire"}
	if _, err = p.reconcileCitusScaleDown(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	s.Members = append(s.Members, worker)
	s.GroupFormed[1] = true
	s.CitusOperation = "fresh"
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/postgres/status" {
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), Timeline: 1, Version: "17.11.1", SystemID: "new-system"})
	})
	b := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "fresh", Storage: "archive", Type: "full", Phase: domain.PhasePending}
	if err = rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	result, err := p.runCitusBackup(t.Context(), rc, &domain.Operation{ID: "fresh"}, backupCheckpoint{}, s, b)
	if err != nil {
		t.Fatal(err)
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Citus.Groups[1].Backup.Repository == old || !strings.HasSuffix(cp.Citus.Groups[1].Backup.Repository, "/g1-i1") || cp.Citus.Groups[1].Backup.SystemID != "new-system" {
		t.Fatal("recreated group backup reused old repository", cp.Citus.Groups[1])
	}
}

func TestCitusExpirationKeepsRecreatedWorkerInventory(t *testing.T) {
	_, rc, _ := citusOpsFixture(t)
	old := domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "retired", Storage: "archive", Postgres: &domain.PostgresBackup{Groups: []domain.PostgresBackupGroup{
		{Group: 0, Backup: domain.PostgresBackup{Label: "old-coordinator", Repository: "formation/g0"}},
		{Group: 1, Backup: domain.PostgresBackup{Label: "same-label", Repository: "formation/g1"}},
	}}}
	current := old
	current.Name = "current"
	current.Postgres = &domain.PostgresBackup{Groups: []domain.PostgresBackupGroup{
		{Group: 0, Backup: domain.PostgresBackup{Label: "new-coordinator", Repository: "formation/g0"}},
		{Group: 1, Backup: domain.PostgresBackup{Label: "same-label", Repository: "formation/g1-i1"}},
	}}
	for _, b := range []*domain.Backup{&old, &current} {
		if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeCitusBackupInventory(t.Context(), old, rc.Store, rc.Secrets); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), old.Name); !missing(err) {
		t.Fatal("expired aggregate retained", err)
	}
	if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), current.Name); err != nil {
		t.Fatal("new incarnation removed by identical retired label", err)
	}
}

func TestCitusMixedIncarnationExpirationUsesNativeForActiveGroup(t *testing.T) {
	p, rc, s := citusOpsFixture(t)
	storage, err := p.backupStorage(t.Context(), rc, "archive", rc.Cluster.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	groups := []domain.PostgresBackupGroup{
		{Group: 0, Backup: domain.PostgresBackup{Label: "old", Repository: groupRepoPrefix(storage, rc.Cluster, s, 0)}},
		{Group: 1, Backup: domain.PostgresBackup{Label: "old", Repository: groupRepoPrefix(storage, rc.Cluster, s, 1)}},
	}
	s.GroupRepositoryIncarnations = map[int]int{1: 1}
	observed := false
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "primary", Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		case "/v1/postgres/backup-info":
			observed = true
			json.NewEncoder(w).Encode(pgspec.BackupInfoResponse{})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	b := &domain.Backup{Storage: "archive", Type: "full", Postgres: &domain.PostgresBackup{Groups: groups}}
	result, err := p.runDeleteCitusBackup(t.Context(), rc, &domain.Operation{}, backupCheckpoint{}, s, b, storage)
	if err != nil || !observed || result.Done {
		t.Fatal("active coordinator incorrectly routed to offline expiration", result, err)
	}
}

func TestCitusRetentionProtectsEveryRepositoryChain(t *testing.T) {
	const a = "20261017-010000F"
	const b = "20261017-020000F"
	const c = "20261017-020000F_20261017-030000D"
	const d = "20261017-040000F"
	const e = "20261017-020000F_20261017-050000I"
	for _, groups := range []int{2, 5} {
		for _, scenario := range []string{
			"manual-diff", "retained-incr", "joint-candidates", "fixed-point",
			"other-storage", "other-namespace", "other-source",
			"other-generation", "other-incarnation", "other-group",
			"worker-needs-newer-full",
		} {
			t.Run(fmt.Sprintf("%d-groups/%s", groups, scenario), func(t *testing.T) {
				makeBackup := func(name, label, kind string) domain.Backup {
					backup := domain.Backup{
						Namespace: "default", Cluster: "demo", Name: name, Storage: "archive",
						Type: kind, Phase: domain.PhaseSucceeded,
						Postgres: &domain.PostgresBackup{Label: label, Repository: "formation/g0"},
					}
					for group := range groups {
						backup.Postgres.Groups = append(backup.Postgres.Groups, domain.PostgresBackupGroup{
							Group: group, Backup: domain.PostgresBackup{
								Label: label, Repository: fmt.Sprintf("formation/g%d", group),
							},
						})
					}
					return backup
				}
				fullA := makeBackup("A", a, "full")
				partialB := makeBackup("B", b, "full")
				partialB.Phase = domain.PhaseFailed
				// B finished the coordinator and earlier workers, then failed
				// before the final worker. C therefore has different full bases.
				partialB.Postgres.Groups = partialB.Postgres.Groups[:groups-1]
				diffC := makeBackup("C", c, "diff")
				diffC.Trigger = "manual"
				diffC.Postgres.Prior = b
				for i := range diffC.Postgres.Groups {
					diffC.Postgres.Groups[i].Backup.Prior = b
				}
				last := &diffC.Postgres.Groups[groups-1]
				last.Backup.Prior = a
				newFull := makeBackup("D", d, "full")
				inventory := []domain.Backup{fullA, partialB, diffC, newFull}
				candidates := []domain.Backup{fullA}
				want := ""
				switch scenario {
				case "joint-candidates":
					candidates = append(candidates, diffC)
					want = "A,C"
				case "retained-incr", "fixed-point":
					incr := makeBackup("E", e, "incr")
					incr.Postgres.Prior = d
					for i := range incr.Postgres.Groups {
						incr.Postgres.Groups[i].Backup.Prior = d
					}
					// References, not only Prior, must participate in closure.
					incr.Postgres.Groups[groups-1].Backup.References = []string{c}
					inventory = append(inventory, incr)
					if scenario == "fixed-point" {
						candidates = append(candidates, diffC)
					}
				case "other-storage":
					diffC.Storage = "other"
					want = "A"
				case "other-namespace":
					diffC.Namespace = "other"
					want = "A"
				case "other-source":
					diffC.Cluster = "other"
					want = "A"
				case "other-generation":
					last.Backup.Repository = fmt.Sprintf("formation-g1/g%d", last.Group)
					want = "A"
				case "other-incarnation":
					last.Backup.Repository += "-i1"
					want = "A"
				case "other-group":
					last.Group = groups
					want = "A"
				case "worker-needs-newer-full":
					// A newer coordinator full is insufficient for an old
					// worker incarnation with no successful successor.
					newFull.Postgres.Groups[groups-1].Backup.Repository += "-i1"
					diffC.Storage = "other"
				}
				inventory[2] = diffC
				inventory[3] = newFull
				got := New(Options{}).FilterRetention(inventory, candidates)
				var names []string
				for _, candidate := range got {
					names = append(names, candidate.Name)
				}
				if strings.Join(names, ",") != want {
					t.Fatalf("retention candidates = %v, want %q", names, want)
				}
				if scenario == "joint-candidates" {
					_, rc, _ := citusOpsFixture(t)
					for i := range inventory {
						if err := rc.Store.CreateBackup(t.Context(), &inventory[i]); err != nil {
							t.Fatal(err)
						}
					}
					if err := removeCitusBackupInventory(t.Context(), fullA, rc.Store, rc.Secrets); err != nil {
						t.Fatal(err)
					}
					for _, name := range []string{"A", "C"} {
						if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), name); !missing(err) {
							t.Fatalf("jointly eligible aggregate %s survived: %v", name, err)
						}
					}
					for _, name := range []string{"B", "D"} {
						if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), name); err != nil {
							t.Fatalf("independent aggregate %s removed: %v", name, err)
						}
					}
				}
			})
		}
	}
}
