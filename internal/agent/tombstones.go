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

func (a *nodeAgent) applyResolutionTombstones(ctx context.Context, hosts []string) error {
	a.resolutionMu.Lock()
	defer a.resolutionMu.Unlock()
	a.resolutionDesired, a.resolutionKnown = slices.Clone(hosts), true
	return a.verifyResolutionTombstones(ctx, true)
}

// Resolver observations are independent of native rendering and conditional pulls.
// A vanished projection is repaired before a new proof can be advertised.
func (a *nodeAgent) refreshResolutionTombstones(ctx context.Context) error {
	a.resolutionMu.Lock()
	defer a.resolutionMu.Unlock()
	if !a.resolutionKnown {
		return nil
	}
	return a.verifyResolutionTombstones(ctx, false)
}

func (a *nodeAgent) verifyResolutionTombstones(ctx context.Context, install bool) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	writer := a.resolutionWriter
	var err error
	if writer == nil {
		writer, err = tombstone.FromEnv(os.Getenv)
	}
	verify := a.resolutionVerify
	if verify == nil {
		verify = tombstone.Verify
	}
	hosts := a.resolutionDesired
	desiredHash := agentapi.ResolutionSetHash(hosts)
	var status agentapi.ResolutionTombstoneStatus
	if err != nil {
		status.Reason = err.Error()
	} else if !install && a.resolutionInstalledHash == desiredHash && writer != nil && len(hosts) > 0 && verify(ctx, hosts) == nil {
		status = agentapi.ResolutionTombstoneStatus{Writer: writer.Kind(), Supported: true, Hosts: slices.Clone(hosts), VerifiedAt: time.Now().UTC()}
	} else {
		status = installResolutionTombstones(ctx, writer, hosts, verify)
	}
	if !status.VerifiedAt.IsZero() {
		status.SetHash = desiredHash
		states, _ := a.sup.snapshot()
		status.ProcessGeneration = agentapi.ResolutionProcessGeneration(states["mysqld"])
		status.Generation = fmt.Sprintf("%s/%s", a.started.UTC().Format(time.RFC3339Nano), status.ProcessGeneration)
	}
	// A lookup proves presence of desired names, not absence of removed names.
	// Never let refresh certify a complete set whose last Install failed.
	a.resolutionInstalledHash = status.SetHash
	a.mu.Lock()
	a.resolutionTombstones = &status
	a.mu.Unlock()
	if status.Supported && status.Reason != "" {
		return &RetryableApplyError{Err: fmt.Errorf("resolver tombstones: %s", status.Reason)}
	}
	return nil
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
