// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package orphanvolumes retries deletion of explicitly recorded, ID-fenced volumes.
package orphanvolumes

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const (
	InitialBackoff = 2 * time.Minute
	MaxBackoff     = time.Hour
	PollInterval   = 15 * time.Second
)

type Store interface {
	ListOrphanedVolumes(context.Context) ([]domain.OrphanedVolume, error)
	PutOrphanedVolume(context.Context, *domain.OrphanedVolume) error
	DeleteOrphanedVolume(context.Context, string) error
}

type Backend interface {
	GetService(context.Context, string) (*infra.Service, error)
	DeleteServiceByID(context.Context, string, string) (infra.Op, error)
	OpStatus(context.Context, infra.Op) (infra.OpState, error)
}

type Options struct {
	Store   Store
	Backend Backend
	Events  engine.Recorder
	Logger  *slog.Logger
	Now     func() time.Time
}

type Janitor struct{ Options }

func New(opts Options) *Janitor {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Janitor{Options: opts}
}

// Backoff starts with the first janitor failure, independently of engine attempts.
func Backoff(failures int) time.Duration {
	d := InitialBackoff
	for i := 1; i < failures && d < MaxBackoff; i++ {
		d *= 2
	}
	if d > MaxBackoff {
		return MaxBackoff
	}
	return d
}

func (j *Janitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := j.Tick(ctx); err != nil && ctx.Err() == nil {
			j.Logger.Error("orphaned volume janitor", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Tick executes due records once; the clock and backend are injectable for tests.
func (j *Janitor) Tick(ctx context.Context) error {
	items, err := j.Store.ListOrphanedVolumes(ctx)
	if err != nil {
		return err
	}
	var first error
	for i := range items {
		v := &items[i]
		if j.Now().Before(v.NextAttemptAt) {
			continue
		}
		if err := j.attempt(ctx, v); err != nil {
			j.Logger.Error("orphaned volume attempt", "hostname", v.Hostname, "error", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

func (j *Janitor) event(ctx context.Context, v *domain.OrphanedVolume, kind, reason, message string) error {
	if j.Events == nil {
		return nil
	}
	return j.Events.Record(ctx, domain.Event{Cluster: v.Cluster, Type: kind, Reason: reason, Message: message, CreatedAt: j.Now()})
}

func (j *Janitor) removed(ctx context.Context, v *domain.OrphanedVolume, replaced bool) error {
	message := fmt.Sprintf("orphaned volume %s deleted", v.Hostname)
	kind := "Normal"
	if replaced {
		message = fmt.Sprintf("orphaned volume %s was replaced by a different service; dropping record without deleting the replacement", v.Hostname)
		j.Logger.Warn(message, "serviceId", v.ServiceID)
		return j.Store.DeleteOrphanedVolume(ctx, v.Hostname)
	}
	if err := j.event(ctx, v, kind, "OrphanedVolumeDeleted", message); err != nil {
		return err
	}
	return j.Store.DeleteOrphanedVolume(ctx, v.Hostname)
}

func (j *Janitor) failed(ctx context.Context, v *domain.OrphanedVolume, message string) error {
	v.ProcessPending = false
	v.JanitorFailures++
	v.LastError = message
	v.NextAttemptAt = j.Now().Add(Backoff(v.JanitorFailures))
	if err := j.Store.PutOrphanedVolume(ctx, v); err != nil {
		return err
	}
	return j.event(ctx, v, "Warning", "OrphanedVolumeDeleteFailed", fmt.Sprintf("orphaned volume %s delete failed: %s; retry at %s", v.Hostname, message, v.NextAttemptAt.Format(time.RFC3339)))
}

func (j *Janitor) attempt(ctx context.Context, v *domain.OrphanedVolume) error {
	service, err := j.Backend.GetService(ctx, v.Hostname)
	if err != nil {
		if v.ProcessPending {
			v.NextAttemptAt = j.Now().Add(InitialBackoff)
			if saveErr := j.Store.PutOrphanedVolume(ctx, v); saveErr != nil {
				return saveErr
			}
			return err
		}
		return j.failed(ctx, v, err.Error())
	}
	if service == nil {
		return j.removed(ctx, v, false)
	}
	if service.ID != v.ServiceID {
		return j.removed(ctx, v, true)
	}
	if v.ProcessPending && v.LastProcessID != "" {
		st, err := j.Backend.OpStatus(ctx, infra.Op{ID: v.LastProcessID, Kind: "delete", ServiceID: v.ServiceID})
		if err != nil {
			// A polling error is not a failed delete: preserve the in-flight fence.
			v.NextAttemptAt = j.Now().Add(InitialBackoff)
			if saveErr := j.Store.PutOrphanedVolume(ctx, v); saveErr != nil {
				return saveErr
			}
			return err
		}
		if st.Failed {
			return j.failed(ctx, v, st.Message)
		}
		if st.Done {
			service, err = j.Backend.GetService(ctx, v.Hostname)
			if err != nil {
				return err
			}
			if service == nil {
				return j.removed(ctx, v, false)
			}
			if service.ID != v.ServiceID {
				return j.removed(ctx, v, true)
			}
			return j.failed(ctx, v, "delete process completed but volume still exists")
		}
		v.NextAttemptAt = j.Now().Add(PollInterval)
		return j.Store.PutOrphanedVolume(ctx, v)
	}
	v.Attempts++
	v.LastAttemptAt = j.Now()
	op, err := j.Backend.DeleteServiceByID(ctx, v.Hostname, v.ServiceID)
	if op.ID != "" {
		v.LastProcessID = op.ID
	}
	if err != nil {
		return j.failed(ctx, v, err.Error())
	}
	v.ProcessPending = op.ID != ""
	v.NextAttemptAt = j.Now().Add(PollInterval)
	return j.Store.PutOrphanedVolume(ctx, v)
}
