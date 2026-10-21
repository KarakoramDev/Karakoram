// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/cronutil"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/robfig/cron/v3"
)

// Trigger coalesces reconciliation requests for a cluster without blocking.
type Trigger interface{ Trigger(domain.ClusterRef) }

// Options configures schedule persistence, clock, and reconciliation triggering.
type Options struct {
	Store          *store.Store
	Registry       *engine.Registry
	Trigger        Trigger
	Clock          engine.Clock
	ReloadInterval time.Duration
	Logger         *slog.Logger
	Events         engine.Recorder
}
type entry struct {
	id   cron.EntryID
	spec domain.BackupSchedule
}

// Scheduler reloads cron schedules and enqueues durable backup and retention operations.
type Scheduler struct {
	opts    Options
	cron    *cron.Cron
	parser  cron.Parser
	mu      sync.Mutex
	entries map[string]entry
	ctx     context.Context
}

// New constructs a UTC backup scheduler.
func New(opts Options) (*Scheduler, error) {
	if opts.Store == nil {
		return nil, errors.New("scheduler store is required")
	}
	if opts.Clock == nil {
		opts.Clock = engine.RealClock{}
	}
	if opts.ReloadInterval <= 0 {
		opts.ReloadInterval = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	parser := cronutil.Parser()
	return &Scheduler{opts: opts, parser: parser, cron: cron.New(cron.WithParser(parser), cron.WithLocation(time.UTC)), entries: make(map[string]entry)}, nil
}

// ValidateCron accepts standard five-field cron expressions and descriptors.
func ValidateCron(spec string) error {
	_, err := cronutil.Parser().Parse(spec)
	return err
}

// Run runs workers until cancellation and waits for them to stop.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.ctx != nil {
		s.mu.Unlock()
		return errors.New("scheduler already started")
	}
	s.ctx = ctx
	s.mu.Unlock()
	if err := s.Reload(ctx); err != nil {
		s.opts.Logger.Error("load backup schedules; preserving state and retrying", "error", err)
	}
	s.cron.Start()
	defer func() { <-s.cron.Stop().Done() }()
	ticker := time.NewTicker(s.opts.ReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Reload(ctx); err != nil {
				s.opts.Logger.Error("reload backup schedules", "error", err)
			}
		}
	}
}

// Reload is safe to call immediately after a cluster spec or backup status edit.
func (s *Scheduler) Reload(ctx context.Context) error {
	clusters, err := s.opts.Store.ListClusters(ctx, "")
	if err != nil {
		var damaged *store.ClusterDataError
		if !errors.As(err, &damaged) {
			return err
		}
		s.opts.Logger.Error("isolating unreadable clusters from scheduling; restore verified control-plane state", "error", err)
	}
	desired := map[string]struct {
		cluster domain.ClusterRef
		spec    domain.BackupSchedule
	}{}
	for _, c := range clusters {
		if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
			continue
		}
		for _, spec := range c.Spec.Backup.Schedules {
			// Persist disabled policy too, so re-enabling it starts a new
			// schedule epoch rather than replaying the disabled period.
			if !spec.Enabled {
				if _, err := s.opts.Store.LoadScheduleCursor(ctx, &c, spec, s.opts.Clock.Now()); err != nil {
					return err
				}
			}
			if !spec.Enabled {
				continue
			}
			if _, err = s.parser.Parse(spec.Cron); err != nil {
				return fmt.Errorf("cluster %s schedule %s: %w", c.Name, spec.Name, err)
			}
			desired[c.Ref().Key()+"/"+spec.Name] = struct {
				cluster domain.ClusterRef
				spec    domain.BackupSchedule
			}{c.Ref(), spec}
		}
	}
	s.mu.Lock()
	for key, e := range s.entries {
		d, ok := desired[key]
		if !ok || d.spec != e.spec {
			s.cron.Remove(e.id)
			delete(s.entries, key)
		}
	}
	for key, d := range desired {
		if _, ok := s.entries[key]; ok {
			continue
		}
		cluster, name := d.cluster, d.spec.Name
		id, err := s.cron.AddFunc(d.spec.Cron, func() {
			s.mu.Lock()
			runCtx := s.ctx
			s.mu.Unlock()
			if runCtx == nil {
				runCtx = context.Background()
			}
			if err := s.catchUp(runCtx, cluster, name); err != nil {
				s.opts.Logger.Error("scheduled backup", "cluster", cluster, "schedule", name, "error", err)
			}
		})
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.entries[key] = entry{id: id, spec: d.spec}
	}
	s.mu.Unlock()
	var failures error
	for _, d := range desired {
		if err := s.catchUp(ctx, d.cluster, d.spec.Name); err != nil {
			failures = errors.Join(failures, err)
		}
	}
	for _, c := range clusters {
		if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
			continue
		}
		if err = s.EnforceRetention(ctx, c.Ref()); err != nil {
			return err
		}
	}
	return failures
}

// Fire re-reads the current spec, so removed or disabled schedules cannot fire
// from a stale cron callback. Clock and Trigger are injectable for tests.
func (s *Scheduler) Fire(ctx context.Context, cluster domain.ClusterRef, schedule string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, s.scheduleFailed(ctx, cluster, schedule))
		}
	}()
	c, err := s.opts.Store.GetCluster(ctx, cluster)
	if err != nil {
		return err
	}
	ctx = store.WithClusterSnapshot(ctx, c)
	if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
		return nil
	}
	var spec *domain.BackupSchedule
	for i := range c.Spec.Backup.Schedules {
		v := &c.Spec.Backup.Schedules[i]
		if v.Name == schedule && v.Enabled {
			spec = v
			break
		}
	}
	if spec == nil {
		return nil
	}
	return s.fireBackup(ctx, c, spec, nil, time.Time{})
}

// catchUp admits at most one overdue occurrence per reload/callback. Its cursor
// advances only with a committed backup operation, including after CP restart.
func (s *Scheduler) catchUp(ctx context.Context, cluster domain.ClusterRef, schedule string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, s.scheduleFailed(ctx, cluster, schedule))
		}
	}()
	c, err := s.opts.Store.GetCluster(ctx, cluster)
	if err != nil {
		return err
	}
	if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
		return nil
	}
	for i := range c.Spec.Backup.Schedules {
		spec := &c.Spec.Backup.Schedules[i]
		if spec.Name != schedule || !spec.Enabled {
			continue
		}
		now := s.opts.Clock.Now()
		cursor, err := s.opts.Store.LoadScheduleCursor(ctx, c, *spec, now)
		if err != nil {
			return err
		}
		parsed, err := s.parser.Parse(spec.Cron)
		if err != nil {
			return err
		}
		due := parsed.Next(cursor.LastDueAt)
		if due.IsZero() || due.After(now) {
			return nil
		}
		return s.fireBackup(store.WithClusterSnapshot(ctx, c), c, spec, &cursor, due)
	}
	return nil
}

func (s *Scheduler) fireBackup(ctx context.Context, c *domain.Cluster, spec *domain.BackupSchedule, cursor *store.ScheduleCursor, due time.Time) error {
	suffix, err := secrets.RandomString(8)
	if err != nil {
		return err
	}
	now := s.opts.Clock.Now()
	storage := spec.Storage
	if storage == "" {
		storage = c.Spec.Backup.Storage
	}
	name := "scheduled-" + now.Format("20060102t150405") + "-" + strings.ToLower(suffix)
	backup := domain.Backup{Namespace: c.Namespace, Name: name, Cluster: c.Name, Storage: storage, Type: spec.Type, Trigger: "schedule", Schedule: spec.Name, Phase: domain.PhasePending, CreatedAt: now}
	if c.Spec.Provider != "" && c.Spec.Provider != "psmdb" {
		backup.Provider = c.Spec.Provider
	}
	payload, _ := json.Marshal(map[string]string{"backup": name})
	op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationBackup, Payload: payload, Actor: "scheduler", CreatedAt: now}
	if cursor != nil {
		err = s.opts.Store.EnqueueScheduledBackup(ctx, &backup, &op, *cursor, due)
	} else {
		err = s.opts.Store.EnqueueBackup(ctx, &backup, &op)
	}
	if err != nil {
		return err
	}
	if s.opts.Trigger != nil {
		s.opts.Trigger.Trigger(c.Ref())
	}
	return nil
}

// EnforceRetention enqueues deletion of the oldest successful scheduled backups.
func (s *Scheduler) EnforceRetention(ctx context.Context, cluster domain.ClusterRef) error {
	c, err := s.opts.Store.GetCluster(ctx, cluster)
	if err != nil {
		return err
	}
	ctx = store.WithClusterSnapshot(ctx, c)
	backups, err := s.opts.Store.ListBackups(ctx, cluster)
	if err != nil {
		return err
	}
	ops, err := s.opts.Store.ListOperations(ctx, cluster, domain.PhasePending, domain.PhaseRunning)
	if err != nil {
		return err
	}
	deleting := map[string]bool{}
	for _, op := range ops {
		if op.Kind != domain.OperationDeleteBackup {
			continue
		}
		var payload struct {
			Backup string `json:"backup"`
		}
		if err = json.Unmarshal(op.Payload, &payload); err != nil {
			return err
		}
		deleting[payload.Backup] = true
	}
	var candidates []domain.Backup
	for _, spec := range c.Spec.Backup.Schedules {
		if spec.RetainCount <= 0 {
			continue
		}
		successful := make([]domain.Backup, 0)
		for _, b := range backups {
			if b.Trigger == "schedule" && b.Schedule == spec.Name && b.Phase == domain.PhaseSucceeded && !deleting[b.Name] {
				successful = append(successful, b)
			}
		}
		sort.Slice(successful, func(i, j int) bool {
			a, b := successful[i], successful[j]
			at, bt := a.CreatedAt, b.CreatedAt
			if a.CompletedAt != nil {
				at = *a.CompletedAt
			}
			if b.CompletedAt != nil {
				bt = *b.CompletedAt
			}
			if at.Equal(bt) {
				return a.Name < b.Name
			}
			return at.Before(bt)
		})
		for i := 0; i < len(successful)-spec.RetainCount; i++ {
			candidates = append(candidates, successful[i])
		}
	}
	if s.opts.Registry != nil {
		provider, err := s.opts.Registry.Get(c.Spec.Provider)
		if err != nil {
			return err
		}
		if policy, ok := provider.(engine.RetentionPolicy); ok {
			candidates = policy.FilterRetention(backups, candidates)
		}
	}
	for _, b := range candidates {
		payload, _ := json.Marshal(map[string]string{"backup": b.Name})
		op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationDeleteBackup, Payload: payload, Actor: "scheduler", CreatedAt: s.opts.Clock.Now()}
		if err = s.opts.Store.EnqueueBackupDeletion(ctx, &b, &op); errors.Is(err, store.ErrConflict) {
			return nil // Abandon this candidate set after its snapshot changed.
		} else if err != nil {
			return err
		}
		if s.opts.Trigger != nil {
			s.opts.Trigger.Trigger(cluster)
		}
	}
	return nil
}

func (s *Scheduler) scheduleFailed(ctx context.Context, cluster domain.ClusterRef, schedule string) error {
	event := domain.Event{Namespace: domain.NormalizedNamespace(cluster.Namespace), Cluster: cluster.Name, Type: "Warning", Reason: "BackupScheduleFailed", Message: "Scheduled backup could not be admitted: " + schedule, Actor: "scheduler", CreatedAt: s.opts.Clock.Now()}
	if s.opts.Events != nil {
		return s.opts.Events.Record(ctx, event)
	}
	return s.opts.Store.AppendEvent(ctx, &event)
}
