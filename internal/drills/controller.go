// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package drills verifies backups using isolated, server-managed restore targets.
package drills

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/cronutil"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type BackupRepository interface {
	ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error)
}
type Repository interface {
	BackupRepository
	GetCluster(context.Context, domain.ClusterRef) (*domain.Cluster, error)
	ListClusters(context.Context, string) ([]domain.Cluster, error)
	ListRestores(context.Context, domain.ClusterRef) ([]domain.Restore, error)
	ListOrphanedVolumes(context.Context, string) ([]domain.OrphanedVolume, error)
	CreateBackupDrill(context.Context, domain.BackupDrill) error
	UpdateBackupDrill(context.Context, domain.BackupDrill, string) (bool, error)
	ListActiveBackupDrills(context.Context) ([]domain.BackupDrill, error)
	GetSetting(context.Context, string) (json.RawMessage, error)
	SetSetting(context.Context, string, json.RawMessage) error
}

type Options struct {
	Store Repository
	// Create must use the normal validated cluster create path and mark the target
	// ManagedBy backup-drill with this report's ID. It must tolerate replay.
	Create func(context.Context, domain.Cluster, domain.BackupDrill) error
	// Delete must use normal deletion with keepVolumes=false, keepBackups=false.
	Delete      func(context.Context, domain.Cluster) error
	Record      func(context.Context, domain.Event) error
	Clock       engine.Clock
	Interval    time.Duration
	Concurrency int
	Logger      *slog.Logger
}
type Controller struct{ opts Options }

func New(opts Options) (*Controller, error) {
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
		if raw := os.Getenv("KARAKORAM_BACKUP_DRILL_CONCURRENCY"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("KARAKORAM_BACKUP_DRILL_CONCURRENCY must be 1..4")
			}
			opts.Concurrency = n
		}
	}
	if opts.Concurrency < 1 || opts.Concurrency > 4 {
		return nil, errors.New("KARAKORAM_BACKUP_DRILL_CONCURRENCY must be 1..4")
	}
	if opts.Store == nil || opts.Create == nil || opts.Delete == nil {
		return nil, errors.New("backup drill store, create and delete callbacks are required")
	}
	if opts.Clock == nil {
		opts.Clock = engine.RealClock{}
	}
	if opts.Interval <= 0 {
		opts.Interval = 15 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Controller{opts}, nil
}

func TargetName(id string) string {
	digest := sha256.Sum256([]byte(id))
	return "dr" + hex.EncodeToString(digest[:])[:10]
}

// SelectBackup selects only successful, engine-compatible restorable inventory.
// An explicit selection is still subject to the policy's schedule filter.
func SelectBackup(ctx context.Context, st BackupRepository, source domain.Cluster, requested string) (domain.Backup, error) {
	backups, err := st.ListBackups(ctx, source.Ref())
	if err != nil {
		return domain.Backup{}, err
	}
	var selected *domain.Backup
	for _, b := range backups {
		if requested != "" && requested != b.Name {
			continue
		}
		if b.Phase != domain.PhaseSucceeded || !b.ProviderMatches(source.Spec.Provider) {
			continue
		}
		if p := source.Spec.Backup.Drill; p != nil && p.BackupSchedule != "" && p.BackupSchedule != b.Schedule {
			continue
		}
		switch source.Spec.Provider {
		case "rabbitmq":
			if b.Type != "definitions" || b.Location == "" {
				continue
			}
		case "elasticsearch", "elastic":
			if b.Type != "snapshot" {
				continue
			}
		case "postgres", "postgresql", "supabase":
			if b.Postgres == nil {
				continue
			}
		}
		if selected == nil || backupTime(b).After(backupTime(*selected)) || (backupTime(b).Equal(backupTime(*selected)) && b.Name > selected.Name) {
			copy := b
			selected = &copy
		}
	}
	if selected == nil {
		return domain.Backup{}, fmt.Errorf("no succeeded restorable backup matches the drill policy: %w", store.ErrNotFound)
	}
	return *selected, nil
}
func backupTime(b domain.Backup) time.Time {
	if b.CompletedAt != nil {
		return *b.CompletedAt
	}
	return b.CreatedAt
}

func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.Interval)
	defer ticker.Stop()
	for {
		if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
			c.opts.Logger.Error("Backup drill reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Reconcile performs one bounded pass; all work is represented by durable phases.
// Only this runner admits Pending reports, so the installation-wide cap includes
// provisioning, restore, verification and cleanup, not just running goroutines.
func (c *Controller) Reconcile(ctx context.Context) error {
	rows, err := c.opts.Store.ListActiveBackupDrills(ctx)
	if err != nil {
		return err
	}
	if err = c.schedule(ctx, rows); err != nil {
		return err
	}
	rows, err = c.opts.Store.ListActiveBackupDrills(ctx)
	if err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].CreatedAt.Before(rows[j].CreatedAt)
	})
	running := 0
	for _, d := range rows {
		if d.Phase != "Pending" {
			running++
		}
	}
	var failures []error
	for _, d := range rows {
		if d.Phase == "Pending" {
			if running >= c.opts.Concurrency {
				continue
			}
			running++
		}
		// Bound calls even if a provider/server callback stalls. Durable phases retry
		// on the next poll, with the policy deadline bounding the whole drill.
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = c.step(callCtx, d)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("drill %s: %w", d.ID, err))
		}
	}
	return errors.Join(failures...)
}
func (c *Controller) save(ctx context.Context, d domain.BackupDrill, previous string) error {
	ok, err := c.opts.Store.UpdateBackupDrill(ctx, d, previous)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("drill changed concurrently")
	}
	return nil
}
func (c *Controller) step(ctx context.Context, d domain.BackupDrill) error {
	old := d.Phase
	now := c.opts.Clock.Now().UTC()
	if d.Phase == "CleaningUp" {
		return c.cleanup(ctx, d)
	}
	source, err := c.opts.Store.GetCluster(ctx, domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return c.report(ctx, d, "source cluster no longer exists")
		}
		return err
	}
	if source.Deleting {
		return c.report(ctx, d, "source cluster is deleting")
	}
	timeout := 6 * time.Hour
	if p := source.Spec.Backup.Drill; p != nil && p.Timeout != "" {
		timeout, err = time.ParseDuration(p.Timeout)
		if err != nil {
			return c.report(ctx, d, "invalid drill timeout")
		}
	}
	if d.StartedAt != nil && !now.Before(d.StartedAt.Add(timeout)) {
		return c.report(ctx, d, "backup drill timed out")
	}
	if d.Phase == "Pending" {
		if source.Status.Phase == domain.PhaseSuspended || source.ManagedBy != nil {
			return c.report(ctx, d, "source cluster is suspended or managed")
		}
		b, err := SelectBackup(ctx, c.opts.Store, *source, d.Backup)
		if err != nil {
			return c.report(ctx, d, err.Error())
		}
		d.Backup = b.Name
		d.BackupType = b.Type
		d.TargetCluster = TargetName(d.ID)
		d.StartedAt = &now
		age := b.LastWriteTime
		if age == nil {
			age = b.CompletedAt
		}
		if age != nil {
			d.DataAgeSeconds = seconds(now.Sub(*age))
		}
		if source.Spec.Provider == "rabbitmq" {
			d.Limitations = "definitions only, no messages"
		}
		d.Phase = "Provisioning"
		d.Cleanup.Phase = "Pending"
		return c.save(ctx, d, old)
	}
	target, err := c.opts.Store.GetCluster(ctx, domain.ClusterRef{Namespace: d.Namespace, Name: d.TargetCluster})
	if errors.Is(err, store.ErrNotFound) {
		if d.Phase != "Provisioning" {
			return c.report(ctx, d, "restore target disappeared")
		}
		if err = c.opts.Create(ctx, *source, d); err != nil {
			return c.report(ctx, d, "target creation failed: "+err.Error())
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !owned(*target, d) {
		return c.report(ctx, d, "target ownership does not match drill")
	}
	if d.Phase == "Provisioning" {
		d.Phase = "Restoring"
		return c.save(ctx, d, old)
	}
	if d.Phase == "Restoring" {
		restore, err := c.restore(ctx, *target, d)
		if err != nil {
			return err
		}
		if restore.Phase == domain.PhaseFailed || target.Status.Phase == domain.PhaseError {
			message := restore.Error
			if message == "" {
				message = "restore target failed"
			}
			return c.report(ctx, d, message)
		}
		if restore.Phase != domain.PhaseSucceeded || target.Status.Phase != domain.PhaseReady {
			return nil
		}
		d.ReadyAt = &now
		d.RestoreDurationSeconds = seconds(now.Sub(*d.StartedAt))
		if restore.StartedAt != nil && restore.CompletedAt != nil {
			d.RestoreDurationSeconds = seconds(restore.CompletedAt.Sub(*restore.StartedAt))
		}
		d.Checks = []domain.BackupDrillCheck{{Name: "target-ready", Passed: true}, {Name: "restore-succeeded", Passed: true}}
		if target.Spec.Provider == "rabbitmq" {
			d.Checks[1].Name = "definitions-imported"
			d.Limitations = "definitions only, no messages"
		}
		d.Phase = "Verifying"
		return c.save(ctx, d, old)
	}
	if d.Phase == "Verifying" {
		return c.report(ctx, d, "")
	}
	return fmt.Errorf("unknown backup drill phase %q", d.Phase)
}
func seconds(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return int64(d / time.Second)
}
func owned(target domain.Cluster, d domain.BackupDrill) bool {
	return target.ManagedBy != nil && target.ManagedBy.Kind == "backup-drill" && target.ManagedBy.ID == d.ID
}
func (c *Controller) restore(ctx context.Context, target domain.Cluster, d domain.BackupDrill) (domain.Restore, error) {
	if target.Spec.Provider == "rabbitmq" {
		var state struct {
			CloneImported bool `json:"cloneImported"`
		}
		if len(target.Internal) > 0 {
			if err := json.Unmarshal(target.Internal, &state); err != nil {
				return domain.Restore{}, err
			}
		}
		if state.CloneImported {
			return domain.Restore{Phase: domain.PhaseSucceeded}, nil
		}
		return domain.Restore{}, nil
	}
	rows, err := c.opts.Store.ListRestores(ctx, target.Ref())
	if err != nil {
		return domain.Restore{}, err
	}
	for _, r := range rows {
		if r.Source.Backup == d.Backup && (r.Source.SourceCluster == "" || r.Source.SourceCluster == d.Cluster) {
			return r, nil
		}
	}
	return domain.Restore{}, nil
}
func (c *Controller) report(ctx context.Context, d domain.BackupDrill, message string) error {
	now := c.opts.Clock.Now().UTC()
	previous := d.Phase
	d.Error = message
	d.CompletedAt = &now
	if d.StartedAt != nil {
		d.TotalDurationSeconds = seconds(now.Sub(*d.StartedAt))
	}
	if message != "" {
		d.Checks = append(d.Checks, domain.BackupDrillCheck{Name: "drill-completed", Passed: false, Message: message})
	}
	d.Phase = "CleaningUp"
	d.Cleanup.Phase = "Pending"
	// Persist the complete report before deletion can destroy restore inventory.
	if err := c.save(ctx, d, previous); err != nil {
		return err
	}
	return c.recordReport(ctx, d)
}
func (c *Controller) event(ctx context.Context, d domain.BackupDrill) error {
	if c.opts.Record == nil {
		return nil
	}
	event := domain.Event{Namespace: d.Namespace, Cluster: d.Cluster, Type: "Normal", Reason: "BackupDrillSucceeded", Message: "Backup drill " + d.ID + " succeeded", CreatedAt: c.opts.Clock.Now(), Actor: "system:backup-drill"}
	if d.Error != "" {
		event.Type = "Warning"
		event.Reason = "BackupDrillFailed"
		event.Message = "Backup drill " + d.ID + " failed: " + d.Error
	}
	return c.opts.Record(ctx, event)
}

func (c *Controller) recordReport(ctx context.Context, d domain.BackupDrill) error {
	key := "drills/event/" + d.ID
	if _, err := c.opts.Store.GetSetting(ctx, key); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if err := c.event(ctx, d); err != nil {
		return err
	}
	return c.setting(ctx, key, true)
}

type cleanupState struct {
	Attempts int `json:"attempts"`
}

func (c *Controller) cleanup(ctx context.Context, d domain.BackupDrill) error {
	if err := c.recordReport(ctx, d); err != nil {
		return err
	}
	key := "drills/cleanup/" + d.ID
	var retry cleanupState
	raw, err := c.opts.Store.GetSetting(ctx, key)
	if err == nil {
		if err = json.Unmarshal(raw, &retry); err != nil {
			return err
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	target, err := c.opts.Store.GetCluster(ctx, domain.ClusterRef{Namespace: d.Namespace, Name: d.TargetCluster})
	// An uncreated target has no name, and must never resolve to a source cluster.
	if d.TargetCluster == "" {
		err = store.ErrNotFound
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if err == nil && !owned(*target, d) {
		d.Cleanup.Phase = "Failed"
		return c.finish(ctx, d, "cleanup refused: target ownership does not match drill")
	}
	if err == nil {
		if retry.Attempts >= 3 || (d.CompletedAt != nil && c.opts.Clock.Now().Sub(*d.CompletedAt) >= 30*time.Minute) {
			d.Cleanup.Phase = "Failed"
			return c.finish(ctx, d, "target cleanup retry budget exhausted")
		}
		if !target.Deleting {
			retry.Attempts++
			if err = c.setting(ctx, key, retry); err != nil {
				return err
			}
			if err = c.opts.Delete(ctx, *target); err != nil {
				return err
			}
		}
		return nil
	}
	orphans, err := c.opts.Store.ListOrphanedVolumes(ctx, d.Namespace)
	if err != nil {
		return err
	}
	d.Cleanup.OrphanedVolumes = []string{}
	for _, v := range orphans {
		if v.Cluster == d.TargetCluster {
			d.Cleanup.OrphanedVolumes = append(d.Cleanup.OrphanedVolumes, v.Hostname)
		}
	}
	sort.Strings(d.Cleanup.OrphanedVolumes)
	d.Cleanup.Phase = "Done"
	return c.finish(ctx, d, "")
}
func (c *Controller) finish(ctx context.Context, d domain.BackupDrill, cleanupError string) error {
	if cleanupError != "" {
		if d.Error != "" {
			d.Error += "; "
		}
		d.Error += cleanupError
	}
	d.Phase = "Succeeded"
	if d.Error != "" {
		d.Phase = "Failed"
	}
	if err := c.save(ctx, d, "CleaningUp"); err != nil {
		return err
	}
	if cleanupError != "" {
		return c.event(ctx, d)
	}
	return nil
}

type scheduleCursor struct {
	ClusterCreatedAt time.Time                `json:"clusterCreatedAt"`
	Spec             domain.BackupDrillPolicy `json:"spec"`
	LastDueAt        time.Time                `json:"lastDueAt"`
}

func (c *Controller) schedule(ctx context.Context, active []domain.BackupDrill) error {
	clusters, err := c.opts.Store.ListClusters(ctx, "")
	if err != nil {
		return err
	}
	now := c.opts.Clock.Now().UTC()
	for _, source := range clusters {
		key := "drills/schedule/" + source.Ref().Key()
		p := source.Spec.Backup.Drill
		if p == nil || !p.Enabled { // A disabled observation ends the old scheduling epoch.
			if err = c.setting(ctx, key, scheduleCursor{}); err != nil {
				return err
			}
			continue
		}
		if source.Deleting || source.ManagedBy != nil || source.Status.Phase == domain.PhaseSuspended {
			continue
		}
		parsed, err := cronutil.Parser().Parse(p.Schedule)
		if err != nil {
			return err
		}
		var cursor scheduleCursor
		raw, err := c.opts.Store.GetSetting(ctx, key)
		if err == nil {
			if err = json.Unmarshal(raw, &cursor); err != nil {
				return err
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if cursor.Spec != *p || !cursor.ClusterCreatedAt.Equal(source.CreatedAt) || cursor.LastDueAt.IsZero() {
			cursor = scheduleCursor{source.CreatedAt, *p, now}
			if err = c.setting(ctx, key, cursor); err != nil {
				return err
			}
			continue
		}
		due := parsed.Next(cursor.LastDueAt.UTC())
		if due.After(now) {
			continue
		}
		busy := false
		for _, d := range active {
			if domain.NormalizedNamespace(d.Namespace) == source.Ref().Namespace && d.Cluster == source.Name {
				busy = true
				break
			}
		}
		if busy {
			continue
		}
		b, err := SelectBackup(ctx, c.opts.Store, source, "")
		if err != nil {
			continue
		}
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s/%s", source.Ref().Key(), source.CreatedAt.UnixNano(), p.Schedule, due.Format(time.RFC3339Nano))))
		d := domain.BackupDrill{ID: "drill-" + hex.EncodeToString(digest[:])[:24], Namespace: source.Ref().Namespace, Cluster: source.Name, Backup: b.Name, BackupType: b.Type, Trigger: "schedule", Phase: "Pending", CreatedAt: now, Cleanup: domain.BackupDrillCleanup{Phase: "Pending"}}
		if err = c.opts.Store.CreateBackupDrill(ctx, d); err != nil {
			return err
		}
		// Skip the rest of the outage, admitting at most one catch-up occurrence.
		cursor.LastDueAt = now
		if err = c.setting(ctx, key, cursor); err != nil {
			return err
		}
		active = append(active, d)
	}
	return nil
}

func (c *Controller) setting(ctx context.Context, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.opts.Store.SetSetting(ctx, key, raw)
}
