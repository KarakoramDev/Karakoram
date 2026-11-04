// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package drills

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var _ Repository = (*store.Store)(nil)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type fakeStore struct {
	clusters map[string]domain.Cluster
	drills   map[string]domain.BackupDrill
	settings map[string]json.RawMessage
	backups  []domain.Backup
	restores []domain.Restore
	orphans  []domain.OrphanedVolume
	phases   []string
}

func (s *fakeStore) ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error) {
	return s.backups, nil
}
func (s *fakeStore) GetCluster(_ context.Context, r domain.ClusterRef) (*domain.Cluster, error) {
	v, ok := s.clusters[r.Key()]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &v, nil
}
func (s *fakeStore) ListClusters(context.Context, string) ([]domain.Cluster, error) {
	var out []domain.Cluster
	for _, v := range s.clusters {
		out = append(out, v)
	}
	return out, nil
}
func (s *fakeStore) ListRestores(context.Context, domain.ClusterRef) ([]domain.Restore, error) {
	return s.restores, nil
}
func (s *fakeStore) ListOrphanedVolumes(context.Context, string) ([]domain.OrphanedVolume, error) {
	return s.orphans, nil
}
func (s *fakeStore) CreateBackupDrill(_ context.Context, d domain.BackupDrill) error {
	if _, ok := s.drills[d.ID]; !ok {
		s.drills[d.ID] = d
	}
	return nil
}
func (s *fakeStore) UpdateBackupDrill(_ context.Context, d domain.BackupDrill, p string) (bool, error) {
	if s.drills[d.ID].Phase != p {
		return false, nil
	}
	s.drills[d.ID] = d
	s.phases = append(s.phases, d.Phase)
	return true, nil
}
func (s *fakeStore) ListActiveBackupDrills(context.Context) ([]domain.BackupDrill, error) {
	var out []domain.BackupDrill
	for _, v := range s.drills {
		if v.Phase != "Succeeded" && v.Phase != "Failed" {
			out = append(out, v)
		}
	}
	return out, nil
}
func (s *fakeStore) GetSetting(_ context.Context, k string) (json.RawMessage, error) {
	v, ok := s.settings[k]
	if !ok {
		return nil, store.ErrNotFound
	}
	return v, nil
}
func (s *fakeStore) SetSetting(_ context.Context, k string, v json.RawMessage) error {
	s.settings[k] = v
	return nil
}

type fixture struct {
	s                                *fakeStore
	clock                            *fakeClock
	opts                             Options
	events                           []domain.Event
	creates, deletes, deleteFailures int
}

func setup(t *testing.T) *fixture {
	t.Helper()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	started := now.Add(-20 * time.Minute)
	completed := now.Add(-10 * time.Minute)
	source := domain.Cluster{Namespace: "default", Name: "source", CreatedAt: now.Add(-time.Hour), Spec: domain.ClusterSpec{Provider: "psmdb"}}
	f := &fixture{s: &fakeStore{clusters: map[string]domain.Cluster{source.Ref().Key(): source}, drills: map[string]domain.BackupDrill{}, settings: map[string]json.RawMessage{}, backups: []domain.Backup{{Name: "backup", Provider: "psmdb", Phase: domain.PhaseSucceeded, Type: "logical", CompletedAt: &completed, LastWriteTime: &started}}, restores: []domain.Restore{{Source: domain.RestoreSource{Backup: "backup", SourceCluster: "source"}, Phase: domain.PhaseSucceeded, StartedAt: &started, CompletedAt: &completed}}}, clock: &fakeClock{now: now}}
	f.opts = Options{Store: f.s, Clock: f.clock, Concurrency: 1,
		Create: func(_ context.Context, source domain.Cluster, d domain.BackupDrill) error {
			f.creates++
			target := domain.Cluster{Namespace: d.Namespace, Name: d.TargetCluster, Spec: source.Spec, ManagedBy: &domain.ManagedBy{Kind: "backup-drill", Cluster: d.Cluster, ID: d.ID}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
			f.s.clusters[target.Ref().Key()] = target
			return nil
		},
		Delete: func(_ context.Context, target domain.Cluster) error {
			f.deletes++
			if f.s.drills[target.ManagedBy.ID].CompletedAt == nil {
				t.Fatal("cleanup before report persisted")
			}
			if f.deleteFailures > 0 {
				f.deleteFailures--
				return errors.New("temporary delete failure")
			}
			delete(f.s.clusters, target.Ref().Key())
			return nil
		},
		Record: func(_ context.Context, e domain.Event) error { f.events = append(f.events, e); return nil }}
	f.s.drills["one"] = domain.BackupDrill{ID: "one", Namespace: "default", Cluster: "source", Phase: "Pending", CreatedAt: now, Backup: "backup"}
	return f
}
func (f *fixture) tick(t *testing.T) error {
	t.Helper()
	c, err := New(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	return c.Reconcile(context.Background())
}
func (f *fixture) finish(t *testing.T) {
	t.Helper()
	for range 12 {
		if err := f.tick(t); err != nil {
			t.Fatal(err)
		}
		d := f.s.drills["one"]
		if d.Phase == "Succeeded" || d.Phase == "Failed" {
			return
		}
	}
	t.Fatal("drill did not terminate")
}

func TestSuccessAndCrashResumeEveryPhase(t *testing.T) {
	// A fresh controller is constructed before every pass, including after create
	// side effects and before recording the next phase; no in-memory work survives.
	f := setup(t)
	f.s.orphans = []domain.OrphanedVolume{{Cluster: TargetName("one"), Hostname: "disk"}, {Cluster: "source", Hostname: "source-disk"}}
	f.finish(t)
	d := f.s.drills["one"]
	if d.Phase != "Succeeded" || d.Cleanup.Phase != "Done" || f.creates != 1 || f.deletes != 1 {
		t.Fatalf("report=%+v creates=%d deletes=%d", d, f.creates, f.deletes)
	}
	if got := strings.Join(f.s.phases, ","); got != "Provisioning,Restoring,Verifying,CleaningUp,Succeeded" {
		t.Fatal(got)
	}
	if d.RestoreDurationSeconds != 600 || d.DataAgeSeconds != 1200 || len(d.Checks) != 2 || len(d.Cleanup.OrphanedVolumes) != 1 {
		t.Fatalf("report %+v", d)
	}
	if len(f.events) != 1 || f.events[0].Reason != "BackupDrillSucceeded" || f.events[0].Cluster != "source" {
		t.Fatal(f.events)
	}
}
func TestRestoreFailure(t *testing.T) {
	f := setup(t)
	f.s.restores[0].Phase = domain.PhaseFailed
	f.s.restores[0].Error = "restore failed"
	f.finish(t)
	d := f.s.drills["one"]
	if d.Phase != "Failed" || d.Error != "restore failed" || f.deletes != 1 || f.events[0].Reason != "BackupDrillFailed" {
		t.Fatalf("%+v", d)
	}
}
func TestTimeout(t *testing.T) {
	f := setup(t)
	if err := f.tick(t); err != nil {
		t.Fatal(err)
	}
	f.clock.now = f.clock.now.Add(7 * time.Hour)
	f.finish(t)
	if d := f.s.drills["one"]; d.Phase != "Failed" || !strings.Contains(d.Error, "timed out") {
		t.Fatalf("%+v", d)
	}
}
func TestCleanupOwnershipSafety(t *testing.T) {
	for _, owner := range []*domain.ManagedBy{nil, {Kind: "backup-drill", ID: "other"}, {Kind: "other", ID: "one"}} {
		f := setup(t)
		d := f.s.drills["one"]
		d.Phase = "CleaningUp"
		d.TargetCluster = TargetName(d.ID)
		d.CompletedAt = &f.clock.now
		f.s.drills[d.ID] = d
		target := domain.Cluster{Namespace: "default", Name: d.TargetCluster, ManagedBy: owner}
		f.s.clusters[target.Ref().Key()] = target
		f.finish(t)
		if f.deletes != 0 || f.s.drills[d.ID].Cleanup.Phase != "Failed" {
			t.Fatal("unsafe cleanup")
		}
	}
}

func TestSourceDeletionRetainsDurableCleanup(t *testing.T) {
	for _, phase := range []string{"Provisioning", "Restoring", "CleaningUp"} {
		for _, ownership := range []string{"matching", "different-id", "different-kind", "unmanaged", "absent"} {
			t.Run(phase+"/"+ownership, func(t *testing.T) {
				ctx := t.Context()
				key, err := secrets.New(strings.Repeat("m", 32))
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "state.db")
				st, err := store.Open(path, key)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { st.Close() })
				now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
				source := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "psmdb"}}
				if err = st.CreateCluster(ctx, &source); err != nil {
					t.Fatal(err)
				}
				d := domain.BackupDrill{ID: "one", Namespace: source.Ref().Namespace, Cluster: source.Name, Phase: phase, Trigger: "manual", CreatedAt: now, StartedAt: &now, TargetCluster: TargetName("one"), Backup: "backup"}
				if phase == "CleaningUp" {
					d.CompletedAt = &now
					d.Cleanup.Phase = "Pending"
				}
				if err = st.CreateBackupDrill(ctx, d); err != nil {
					t.Fatal(err)
				}
				target := domain.Cluster{Namespace: d.Namespace, Name: d.TargetCluster, Spec: source.Spec, ManagedBy: &domain.ManagedBy{Kind: "backup-drill", Cluster: d.Cluster, ID: d.ID}}
				switch ownership {
				case "different-id":
					target.ManagedBy.ID = "another-drill"
				case "different-kind":
					target.ManagedBy.Kind = "another-controller"
				case "unmanaged":
					target.ManagedBy = nil
				}
				if ownership != "absent" {
					if err = st.CreateCluster(ctx, &target); err != nil {
						t.Fatal(err)
					}
				}
				if err = st.DeleteCluster(ctx, source.Ref()); err != nil {
					t.Fatal(err)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				st, err = store.Open(path, key)
				if err != nil {
					t.Fatal(err)
				}
				deletes := 0
				opts := Options{Store: st, Clock: &fakeClock{now: now},
					Create: func(context.Context, domain.Cluster, domain.BackupDrill) error {
						t.Fatal("created target after source deletion")
						return nil
					},
					Delete: func(ctx context.Context, got domain.Cluster) error {
						deletes++
						if !owned(got, d) {
							t.Fatal("deleted target without matching ownership")
						}
						report, getErr := st.GetBackupDrill(ctx, d.ID)
						if getErr != nil || report.CompletedAt == nil || report.Phase != "CleaningUp" {
							t.Fatal("target deleted before durable report", report, getErr)
						}
						return st.DeleteCluster(ctx, got.Ref())
					},
				}
				for range 4 {
					c, newErr := New(opts)
					if newErr != nil {
						t.Fatal(newErr)
					}
					if err = c.Reconcile(ctx); err != nil {
						t.Fatal(err)
					}
				}
				report, err := st.GetBackupDrill(ctx, d.ID)
				if err != nil || report.CompletedAt == nil {
					t.Fatal("lost report after source deletion", report, err)
				}
				active, err := st.ListActiveBackupDrills(ctx)
				if err != nil || len(active) != 0 {
					t.Fatal("cleanup did not finish", active, err)
				}
				_, targetErr := st.GetCluster(ctx, target.Ref())
				if ownership == "matching" || ownership == "absent" {
					wantDeletes := 0
					if ownership == "matching" {
						wantDeletes = 1
					}
					if deletes != wantDeletes || !errors.Is(targetErr, store.ErrNotFound) || report.Cleanup.Phase != "Done" {
						t.Fatalf("deletes=%d target=%v report=%+v", deletes, targetErr, report)
					}
					if phase == "CleaningUp" {
						if report.Phase != "Succeeded" || report.Error != "" {
							t.Fatalf("cleanup changed previously recorded outcome: %+v", report)
						}
					} else if report.Phase != "Failed" || report.Error != "source cluster no longer exists" {
						t.Fatalf("source disappearance not reported: %+v", report)
					}
				} else if deletes != 0 || targetErr != nil || report.Phase != "Failed" || report.Cleanup.Phase != "Failed" {
					t.Fatalf("ownership fence failed: deletes=%d target=%v report=%+v", deletes, targetErr, report)
				}
			})
		}
	}
}

func TestDeletingSourceDoesNotProvisionTarget(t *testing.T) {
	f := setup(t)
	if err := f.tick(t); err != nil {
		t.Fatal(err)
	}
	source := f.s.clusters["source"]
	source.Deleting = true
	f.s.clusters[source.Ref().Key()] = source
	f.finish(t)
	if d := f.s.drills["one"]; f.creates != 0 || d.Phase != "Failed" || d.Cleanup.Phase != "Done" || d.Error != "source cluster is deleting" {
		t.Fatalf("creates=%d report=%+v", f.creates, d)
	}
}
func TestCleanupRetrySurvivesRestart(t *testing.T) {
	f := setup(t)
	f.deleteFailures = 1
	for f.s.drills["one"].Phase != "CleaningUp" {
		if err := f.tick(t); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.tick(t); err == nil {
		t.Fatal("expected delete error")
	}
	f.finish(t)
	if f.deletes != 2 || f.s.drills["one"].Phase != "Succeeded" {
		t.Fatal(f.s.drills["one"])
	}
}
func TestCleanupRetryBound(t *testing.T) {
	f := setup(t)
	f.deleteFailures = 10
	for range 12 {
		_ = f.tick(t)
	}
	if f.deletes != 3 || f.s.drills["one"].Cleanup.Phase != "Failed" {
		t.Fatalf("deletes=%d report=%+v", f.deletes, f.s.drills["one"])
	}
}
func TestConcurrencyCap(t *testing.T) {
	f := setup(t)
	second := f.s.drills["one"]
	second.ID = "two"
	second.CreatedAt = second.CreatedAt.Add(time.Second)
	f.s.drills[second.ID] = second
	if err := f.tick(t); err != nil {
		t.Fatal(err)
	}
	if f.s.drills["one"].Phase != "Provisioning" || f.s.drills["two"].Phase != "Pending" {
		t.Fatal(f.s.drills)
	}
	f.opts.Concurrency = 2
	if err := f.tick(t); err != nil {
		t.Fatal(err)
	}
	if f.s.drills["two"].Phase != "Provisioning" {
		t.Fatal(f.s.drills)
	}
}
func TestRabbitMQRequiresDefinitionsImport(t *testing.T) {
	f := setup(t)
	source := f.s.clusters["source"]
	source.Spec.Provider = "rabbitmq"
	f.s.clusters[source.Ref().Key()] = source
	f.s.backups[0].Provider = "rabbitmq"
	f.s.backups[0].Type = "definitions"
	f.s.backups[0].Location = "object"
	for range 4 {
		if err := f.tick(t); err != nil {
			t.Fatal(err)
		}
	}
	if f.s.drills["one"].Phase != "Restoring" {
		t.Fatal(f.s.drills)
	}
	target := f.s.clusters[TargetName("one")]
	target.Internal = json.RawMessage(`{"cloneImported":true}`)
	f.s.clusters[target.Ref().Key()] = target
	f.finish(t)
	d := f.s.drills["one"]
	if d.Phase != "Succeeded" || d.Limitations != "definitions only, no messages" || d.Checks[1].Name != "definitions-imported" {
		t.Fatalf("%+v", d)
	}
}
func TestScheduleCatchupAndSkipRules(t *testing.T) {
	for _, rule := range []string{"enabled", "busy", "deleting", "suspended", "managed", "disabled"} {
		t.Run(rule, func(t *testing.T) {
			f := setup(t)
			delete(f.s.drills, "one")
			source := f.s.clusters["source"]
			source.Spec.Backup.Drill = &domain.BackupDrillPolicy{Enabled: true, Schedule: "* * * * *", BackupSchedule: "daily"}
			f.s.backups[0].Schedule = "daily"
			f.s.clusters[source.Ref().Key()] = source
			if err := f.tick(t); err != nil {
				t.Fatal(err)
			}
			f.clock.now = f.clock.now.Add(10 * time.Hour)
			switch rule {
			case "busy":
				f.s.drills["busy"] = domain.BackupDrill{ID: "busy", Namespace: "default", Cluster: "source", Phase: "Restoring", StartedAt: &f.clock.now}
			case "deleting":
				source.Deleting = true
			case "suspended":
				source.Status.Phase = domain.PhaseSuspended
			case "managed":
				source.ManagedBy = &domain.ManagedBy{ID: "owner"}
			case "disabled":
				source.Spec.Backup.Drill.Enabled = false
			}
			f.s.clusters[source.Ref().Key()] = source
			// Invoke scheduling alone so the skip test is independent of phase work.
			c, err := New(f.opts)
			if err != nil {
				t.Fatal(err)
			}
			active, _ := f.s.ListActiveBackupDrills(context.Background())
			if err = c.schedule(context.Background(), active); err != nil {
				t.Fatal(err)
			}
			expected := 0
			if rule == "enabled" || rule == "busy" {
				expected = 1
			}
			if len(f.s.drills) != expected {
				t.Fatalf("%s: %+v", rule, f.s.drills)
			}
			if rule == "enabled" {
				for id := range f.s.drills {
					delete(f.s.drills, id)
				}
				if err = c.schedule(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				if len(f.s.drills) != 0 {
					t.Fatal("multiple catch-up runs")
				}
			}
		})
	}
}
func TestSelectBackupScheduleAndRestorability(t *testing.T) {
	f := setup(t)
	source := f.s.clusters["source"]
	source.Spec.Backup.Drill = &domain.BackupDrillPolicy{BackupSchedule: "daily"}
	f.s.backups[0].Schedule = "weekly"
	if _, err := SelectBackup(context.Background(), f.s, source, ""); err == nil {
		t.Fatal("wrong schedule selected")
	}
	f.s.backups[0].Schedule = "daily"
	f.s.backups[0].Phase = domain.PhaseFailed
	if _, err := SelectBackup(context.Background(), f.s, source, "backup"); err == nil {
		t.Fatal("failed backup selected")
	}
}
func TestConcurrencyEnvironment(t *testing.T) {
	for _, value := range []string{"0", "5", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("KARAKORAM_BACKUP_DRILL_CONCURRENCY", value)
			f := setup(t)
			f.opts.Concurrency = 0
			if _, err := New(f.opts); err == nil {
				t.Fatal("accepted invalid concurrency")
			}
		})
	}
}

func TestUnsupportedProvidersRejectBeforeProvisioning(t *testing.T) {
	for _, name := range []string{"keycloak", "openbao", "temporal", "mysql", "pxc", "neo4j", "weaviate", "unknown"} {
		t.Run(name, func(t *testing.T) {
			for _, phase := range []string{"Pending", "Provisioning", "Restoring", "Verifying"} {
				t.Run(phase, func(t *testing.T) {
					f := setup(t)
					ref := domain.ClusterRef{Namespace: "default", Name: "source"}
					source := f.s.clusters[ref.Key()]
					source.Spec.Provider = name
					f.s.clusters[ref.Key()] = source
					f.s.backups[0].Provider = name
					if _, err := SelectBackup(t.Context(), f.s, source, "backup"); err == nil || !strings.Contains(err.Error(), "unsupported") {
						t.Fatalf("unsupported provider accepted backup selection: %v", err)
					}
					d := f.s.drills["one"]
					d.Phase = phase
					f.s.drills[d.ID] = d
					if err := f.tick(t); err != nil {
						t.Fatal(err)
					}
					d = f.s.drills[d.ID]
					if f.creates != 0 || d.CompletedAt == nil || !strings.Contains(d.Error, "unsupported") || !strings.Contains(d.Error, name) {
						t.Fatalf("unsupported drill was not safely rejected: creates=%d report=%+v", f.creates, d)
					}
				})
			}
		})
	}
}
