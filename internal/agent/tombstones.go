// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent/tombstone"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func installResolutionTombstones(ctx context.Context, writer tombstone.Writer, hosts []string, verify func(context.Context, []string) error) agentapi.ResolutionTombstoneStatus {
	status := agentapi.ResolutionTombstoneStatus{}
	if writer == nil {
		status.Reason = "no resolver mechanism on this platform"
		return status
	}
	status.Supported = true
	status.Writer = writer.Kind()
	if err := writer.Preflight(ctx); err != nil {
		status.Reason = err.Error()
		return status
	}
	if err := writer.Install(ctx, hosts); err != nil {
		status.Reason = err.Error()
		return status
	}
	if err := verify(ctx, hosts); err != nil {
		status.Reason = err.Error()
		return status
	}
	status.Hosts, status.VerifiedAt = slices.Clone(hosts), time.Now().UTC()
	return status
}

func (a *nodeAgent) preserveResolutionTombstones(c agentapi.MemberConfig) agentapi.MemberConfig {
	if !c.ResolutionTombstonesPresent && c.ResolutionTombstones == nil {
		old := a.config()
		a.resolutionMu.Lock()
		if a.resolutionKnown && len(a.resolutionDesired) > 0 {
			old.ResolutionTombstones = slices.Clone(a.resolutionDesired)
			old.ResolutionTombstonesPresent = true
		}
		a.resolutionMu.Unlock()
		c.ResolutionTombstones = slices.Clone(old.ResolutionTombstones)
		c.ResolutionTombstonesPresent = old.ResolutionTombstonesPresent
	}
	return c
}

const resolutionVerificationTimeout = 3 * time.Second

func (a *nodeAgent) applyResolutionTombstones(ctx context.Context, hosts []string) error {
	return a.scheduleResolutionTombstones(ctx, hosts, true)
}

// Refresh schedules one bounded attempt, never a writer call on the caller.
func (a *nodeAgent) refreshResolutionTombstones(ctx context.Context) error {
	return a.scheduleResolutionTombstones(ctx, nil, false)
}

func (a *nodeAgent) scheduleResolutionTombstones(ctx context.Context, hosts []string, apply bool) error {
	// Include admission in the deadline. No resolver I/O runs under either lock.
	deadline := time.Now().Add(resolutionVerificationTimeout)
	lockCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := a.resolutionMu.LockContext(lockCtx); err != nil {
		return err
	}
	defer a.resolutionMu.Unlock()
	changed := apply && (!a.resolutionKnown || !slices.Equal(hosts, a.resolutionDesired))
	if changed {
		a.resolutionDesired, a.resolutionKnown = slices.Clone(hosts), true
		a.resolutionEpoch++
	}
	if !a.resolutionKnown {
		return nil
	}
	writer := a.resolutionWriter
	var err error
	if writer == nil {
		writer, err = tombstone.FromEnv(os.Getenv)
	}
	if writer == nil || err != nil {
		status := agentapi.ResolutionTombstoneStatus{Reason: "no resolver mechanism on this platform"}
		if err != nil {
			status.Reason = err.Error()
		}
		a.mu.Lock()
		a.resolutionTombstones = &status
		a.mu.Unlock()
		return nil
	}
	a.mu.Lock()
	if changed || a.resolutionTombstones == nil {
		a.resolutionTombstones = &agentapi.ResolutionTombstoneStatus{
			Supported: true, Writer: writer.Kind(), Reason: "resolver verification pending",
		}
	}
	status := *a.resolutionTombstones
	a.mu.Unlock()
	installed := status.Reason == "" && status.SetHash == agentapi.ResolutionSetHash(a.resolutionDesired) && !status.VerifiedAt.IsZero()
	verified := installed && time.Since(status.VerifiedAt) <= agentapi.ResolutionProofLease
	if apply && verified {
		return nil
	}
	if !a.resolutionRunning {
		a.resolutionRunning = true
		verify := a.resolutionVerify
		if verify == nil {
			verify = tombstone.Verify
		}
		// The attempt survives its initiating HTTP response, but not shutdown.
		lifetime := a.resolutionContext
		if lifetime == nil {
			lifetime = context.WithoutCancel(ctx)
		}
		workCtx, workCancel := context.WithDeadline(lifetime, deadline)
		go a.runResolutionTombstones(workCtx, workCancel, a.resolutionEpoch, a.resolutionDesired, installed, writer, verify)
	}
	if apply {
		reason := status.Reason
		if reason == "" {
			reason = "resolver verification pending"
		}
		return &RetryableApplyError{Err: fmt.Errorf("resolver tombstones: %s", reason), MaxDelay: time.Second}
	}
	return nil
}

func (a *nodeAgent) runResolutionTombstones(ctx context.Context, cancel context.CancelFunc, epoch uint64, hosts []string, installed bool, writer tombstone.Writer, verify func(context.Context, []string) error) {
	defer cancel()
	result := make(chan agentapi.ResolutionTombstoneStatus, 1)
	go func() {
		var status agentapi.ResolutionTombstoneStatus
		if installed && len(hosts) > 0 && verify(ctx, hosts) == nil {
			status = agentapi.ResolutionTombstoneStatus{Supported: true, Writer: writer.Kind(), Hosts: slices.Clone(hosts), VerifiedAt: time.Now().UTC()}
		} else {
			status = installResolutionTombstones(ctx, writer, hosts, verify)
		}
		if !status.VerifiedAt.IsZero() {
			status.SetHash = agentapi.ResolutionSetHash(hosts)
			states, _ := a.sup.snapshot()
			status.ProcessGeneration = agentapi.ResolutionProcessGeneration(states["mysqld"])
			status.Generation = fmt.Sprintf("%s/%s", a.started.UTC().Format(time.RFC3339Nano), status.ProcessGeneration)
		}
		result <- status
	}()
	var status agentapi.ResolutionTombstoneStatus
	select {
	case status = <-result:
		if ctx.Err() != nil {
			status = agentapi.ResolutionTombstoneStatus{Supported: true, Writer: writer.Kind(), Reason: "resolver verification deadline exceeded"}
		}
		a.publishResolutionTombstones(epoch, status)
	case <-ctx.Done():
		a.publishResolutionTombstones(epoch, agentapi.ResolutionTombstoneStatus{
			Supported: true, Writer: writer.Kind(), Reason: "resolver verification deadline exceeded",
		})
		// A writer that ignores cancellation must not accumulate more flights.
		// Keep admission occupied until it actually exits; status stays readable.
		<-result
	}
	a.resolutionMu.Lock()
	a.resolutionRunning = false
	superseded := a.resolutionEpoch != epoch
	a.resolutionMu.Unlock()
	if superseded {
		lifetime := a.resolutionContext
		if lifetime == nil {
			lifetime = context.Background()
		}
		_ = a.refreshResolutionTombstones(lifetime)
	}
}

func (a *nodeAgent) publishResolutionTombstones(epoch uint64, status agentapi.ResolutionTombstoneStatus) {
	a.resolutionMu.Lock()
	defer a.resolutionMu.Unlock()
	if epoch != a.resolutionEpoch {
		// Never advertise an old set after a newer config was accepted.
		if status.Reason == "" {
			return
		}
		status = agentapi.ResolutionTombstoneStatus{Supported: status.Supported, Writer: status.Writer, Reason: "previous resolver verification failed; waiting to retry current set: " + status.Reason}
	}
	a.mu.Lock()
	a.resolutionTombstones = &status
	a.mu.Unlock()
}

func (a *nodeAgent) refreshResolutionLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = a.refreshResolutionTombstones(ctx)
		}
	}
}

func (a *nodeAgent) attachResolutionTombstones(status *agentapi.Status) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.resolutionTombstones != nil {
		copy := *a.resolutionTombstones
		copy.Hosts = slices.Clone(copy.Hosts)
		status.ResolutionTombstones = &copy
	}
}

// Skip the native renderer only for a pure resolver-set change on an applied
// configuration. Cached startup always runs Apply to reconstruct supervision.
func (a *nodeAgent) applyEngineWithTombstones(ctx context.Context, c agentapi.MemberConfig) ([]string, error) {
	old := a.config()
	if old.ConfigVersion != 0 && (c.ResolutionTombstonesPresent != old.ResolutionTombstonesPresent || len(c.ResolutionTombstones) > 0 || !slices.Equal(old.ResolutionTombstones, c.ResolutionTombstones)) {
		left, right := old, c
		left.ResolutionTombstones, right.ResolutionTombstones = nil, nil
		left.ResolutionTombstonesPresent, right.ResolutionTombstonesPresent = false, false
		left.ConfigVersion, right.ConfigVersion = 0, 0
		left.PullRevision, right.PullRevision = "", ""
		left.ChangeRevision, right.ChangeRevision = "", ""
		left.ContentRevision, right.ContentRevision = "", ""
		if reflect.DeepEqual(left, right) {
			return nil, nil
		}
	}
	return a.engine.Apply(ctx, &Node{a: a, applyConfig: &c}, c)
}
