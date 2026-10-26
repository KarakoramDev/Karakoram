// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const platformRetryPersistentFailures = 10

func platformRetryKey(v *pending) string { return v.Member + "/" + v.Action }

func platformRetryDelay(failures int) time.Duration {
	return engine.PlatformRetryDelay(failures)
}

func platformRetryMessage(v *pending, r *platformRetry) string {
	if r.CPUModeCooldown {
		return fmt.Sprintf("CPU-mode changes may be rate-limited by the platform; retrying at %s (%s)", r.RetryAfter.Format(time.RFC3339), v.Member)
	}
	if volumeDelete(v.Action) && strings.Contains(strings.ToLower(r.Message), "still mounted") {
		return fmt.Sprintf("platform action %s %s failed (%d consecutive volume-deletion failures): %s; retry after %s; volume will be recorded as orphaned after at least %d failures and %s with its runtime absent", v.Action, v.Member, r.StillMountedFailures, r.Message, r.RetryAfter.Format(time.RFC3339), orphanVolumeFailures, orphanVolumeAge)
	}
	if r.Failures >= platformRetryPersistentFailures {
		return fmt.Sprintf("platform action %s %s keeps failing (%d consecutive failures): %s; continuing retries after %s", v.Action, v.Member, r.Failures, r.Message, r.RetryAfter.Format(time.RFC3339))
	}
	return fmt.Sprintf("platform action %s %s failed (attempt %d): %s; retry after %s", v.Action, v.Member, r.Failures, r.Message, r.RetryAfter.Format(time.RFC3339))
}

// failPending drops only re-issuable platform processes, never their proposed
// settings. Retry retains the exact member/target so rolling work cannot advance
// another member or mistake a failed deployment for an applied version.
func (s *internalState) failPending(rc *engine.Context, message string) error {
	v := s.Pending
	switch v.Action {
	case "deploy", "upgrade", "resources", "disk", "containers", "import", "delete-runtime", "delete-volume", "cleanup-runtime", "cleanup-volume":
	default:
		return fmt.Errorf("%s %s failed: %s", v.Action, v.Member, message)
	}
	if s.PlatformRetries == nil {
		s.PlatformRetries = make(map[string]*platformRetry)
	}
	key := platformRetryKey(v)
	r := s.PlatformRetries[key]
	if r == nil {
		r = &platformRetry{}
		s.PlatformRetries[key] = r
	}
	if volumeDelete(v.Action) && r.FirstFailedAt.IsZero() && strings.Contains(strings.ToLower(r.Message), "still mounted") {
		r.StillMountedFailures = r.Failures
	}
	r.Failures++
	if volumeDelete(v.Action) && strings.Contains(strings.ToLower(message), "still mounted") {
		if r.FirstFailedAt.IsZero() {
			r.FirstFailedAt, r.FirstError = now(rc), message
		}
		r.StillMountedFailures++
	} else {
		r.StillMountedFailures, r.FirstError, r.FirstFailedAt = 0, "", time.Time{}
	}
	r.LastAttemptAt = now(rc)
	if v.Op.ID != "" {
		r.LastProcessID = v.Op.ID
	}
	r.CPUModeCooldown = false
	r.Message = message
	recorded := now(rc)
	r.RetryAfter = r.Schedule(recorded, recorded.Add(platformRetryDelay(r.Failures)))
	v.Op = infra.Op{}
	s.Retry, s.Pending = v, nil
	return fmt.Errorf("%s", platformRetryMessage(v, r))
}

// failPendingCause retains backend scheduling metadata before converting the
// failure into durable platform retry state.
func (s *internalState) failPendingCause(rc *engine.Context, cause error) error {
	v := s.Pending
	var wait *engine.PlatformRetryWait
	if errors.As(cause, &wait) {
		s.Pending = nil
		s.resetPlatformRetry(v)
		return cause
	}
	if errors.Is(cause, infra.ErrDeploymentRetriesExhausted) {
		s.Pending = nil
		s.resetPlatformRetry(v)
		return cause
	}
	failure := s.failPending(rc, cause.Error())
	if at := infra.RetryAt(cause); !at.IsZero() {
		if r := s.PlatformRetries[platformRetryKey(v)]; r != nil && at.After(r.RetryAfter) {
			r.RetryAfter = r.Schedule(now(rc), at)
		}
		return errors.Join(failure, cause)
	}
	return failure
}

func (s *internalState) resetPlatformRetry(v *pending) {
	key := platformRetryKey(v)
	delete(s.PlatformRetries, key)
	if s.Retry != nil && platformRetryKey(s.Retry) == key {
		s.Retry = nil
	}
}

// retryPending runs before ordinary convergence and preserves one-action-at-a-
// time ordering. Ownership is checked anew, including partial import adoption.
func (p *Provider) retryPending(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	v := s.Retry
	if v == nil {
		return false, nil
	}
	r := s.PlatformRetries[platformRetryKey(v)]
	if r == nil {
		return true, fmt.Errorf("missing retry state for %s %s", v.Action, v.Member)
	}
	if volumeDelete(v.Action) {
		// Old releases persisted only Failures/Message. Do not infer elapsed
		// time from the failure count: start the conservative clock now.
		if r.FirstFailedAt.IsZero() && strings.Contains(strings.ToLower(r.Message), "still mounted") {
			r.FirstFailedAt, r.FirstError = now(rc), r.Message
			r.StillMountedFailures = r.Failures
		}
		if done, err := p.orphanVolume(ctx, rc, s, v, r); done || err != nil {
			return true, err
		}
	}
	if r.Waiting(now(rc), r.RetryAfter) {
		return true, &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: platformRetryMessage(v, r)}
	}
	m := s.find(v.Member)
	if m == nil {
		return true, engine.ErrNotFound
	}
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return true, err
	}
	if v.Action == "upgrade" || v.Action == "resources" {
		stepped, err := p.prepareDisruption(ctx, rc, s, m)
		if err != nil || stepped {
			return true, err
		}
	}
	var op infra.Op
	var err error
	switch v.Action {
	case "import":
		err = p.importMember(ctx, rc, s, m)
	case "deploy", "upgrade":
		err = p.deploy(ctx, rc, s, m, v.Action, v.Version)
	case "resources":
		op, err = rc.Backend.SetResources(ctx, m.Name, v.Resources)
	case "disk":
		if m.Disk.MinDisk != v.Resources.MinDisk || m.Disk.MaxDisk != v.Resources.MaxDisk {
			if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
				return true, err
			}
		}
		op, err = rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: v.Resources.MinDisk, MaxDisk: v.Resources.MaxDisk})
	case "containers":
		if err := requireRouterScaling(rc.Backend, v.Containers); err != nil {
			return true, err
		}
		op, err = rc.Backend.SetContainerCount(ctx, m.Name, v.Containers, v.Containers)
	case "delete-runtime", "delete-volume", "cleanup-runtime", "cleanup-volume":
		name, recordedID := m.Name, m.ServiceID
		if v.Action == "delete-volume" || v.Action == "cleanup-volume" {
			name, recordedID = m.Volume, m.VolumeID
		}
		// A failed delete may already have removed the service. Observe the
		// existing absence as completion instead of deleting it a second time.
		var service *infra.Service
		service, err = engine.ObserveRetiringService(ctx, rc.Backend, name, recordedID, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost)
		if err == nil && service != nil {
			op, err = rc.Backend.DeleteService(ctx, name)
		}
	default:
		return true, fmt.Errorf("unsupported platform retry action %q", v.Action)
	}
	if err != nil {
		if errors.Is(err, engine.ErrNotSupported) || errors.Is(err, infra.ErrUnsupported) {
			return true, err
		}
		if wait := s.scheduleCPUModeCooldown(rc, v, err); wait != nil {
			return true, wait
		}
		if op.ID != "" {
			v.Op = op
		}
		s.Pending = v
		return true, s.failPendingCause(rc, err)
	}
	if v.Action != "import" && v.Action != "deploy" && v.Action != "upgrade" {
		v.Op = op
		s.Pending = v
	}
	s.Retry = nil
	return true, nil
}

func (s *internalState) platformFailureMessage() string {
	v := s.Retry
	if v == nil {
		v = s.Pending
	}
	if v != nil {
		if r := s.PlatformRetries[platformRetryKey(v)]; r != nil {
			return platformRetryMessage(v, r)
		}
	}
	return ""
}

// Retain the observed successful CPU-mode change time when scheduling a
// platform rate-limit retry. For pre-existing state without it, conservatively
// wait one hour from the rejection rather than hot-looping.
func (s *internalState) scheduleCPUModeCooldown(rc *engine.Context, v *pending, err error) error {
	if v.Action != "resources" || !errors.Is(err, infra.ErrResourceChangeCooldown) {
		return nil
	}
	at := now(rc).Add(time.Hour)
	if m := s.find(v.Member); m != nil && !m.CPUModeChangedAt.IsZero() {
		known := m.CPUModeChangedAt.Add(time.Hour)
		if known.After(now(rc)) {
			at = known
		}
	}
	if s.PlatformRetries == nil {
		s.PlatformRetries = make(map[string]*platformRetry)
	}
	r := &platformRetry{RetryAfter: at, Message: err.Error(), CPUModeCooldown: true}
	r.RetryAfter = r.Schedule(now(rc), at)
	s.PlatformRetries[platformRetryKey(v)] = r
	v.Op = infra.Op{}
	s.Retry, s.Pending = v, nil
	return &engine.PlatformRetryWait{RetryAt: r.RetryAfter, Message: platformRetryMessage(v, r)}
}
