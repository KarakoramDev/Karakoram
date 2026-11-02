// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var (
	errOfflineNotDeployed      = errors.New("member not deployed")
	errOfflineNotStopped       = errors.New("member not stopped")
	errOfflineIdentityMismatch = errors.New("server identity mismatch")
	errOfflineUninitialized    = errors.New("stopped initialized identity required")
)

func offlineRecoveryTimeout(rc *engine.Context, _ *member) time.Duration {
	// Match the native agent routes: GR permits 40s for private startup and
	// 60s total; Galera already permits 180s. RAM scaling beyond these route
	// ceilings would promise time that the agent cannot actually use.
	if rc.Cluster.Spec.Topology.Type == "cluster" {
		return 3 * time.Minute
	}
	return time.Minute
}

// collectOfflineRecovery deliberately bypasses mysqlReadContext's shared health
// sweep deadline. It inherits the operation context (including cancellation),
// and bounds each member independently, including dialing. No failed member
// suppresses another member's evidence; only a complete current sweep may select
// a seed. Three workers bound simultaneous native recovery work.
func collectOfflineRecovery[T any](ctx context.Context, rc *engine.Context, members []*member, probe func(context.Context, *member) (T, error)) (map[string]T, string) {
	type result struct {
		value T
		err   error
	}
	results := make([]result, len(members))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(3, len(members)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				memberCtx, cancel := context.WithTimeout(ctx, offlineRecoveryTimeout(rc, members[i]))
				results[i].value, results[i].err = probe(memberCtx, members[i])
				cancel()
			}
		}()
	}
	for i := range members {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	observations := make(map[string]T, len(members))
	var failures []string
	for i, m := range members {
		if err := results[i].err; err != nil {
			// Never include arbitrary transport/agent errors, which may contain secrets.
			reason := "offline recovery unavailable or invalid"
			if errors.Is(err, context.DeadlineExceeded) {
				reason = "offline recovery timed out"
			} else if errors.Is(err, context.Canceled) {
				reason = "offline recovery canceled"
			} else if errors.Is(err, errOfflineNotDeployed) || errors.Is(err, errOfflineNotStopped) || errors.Is(err, errOfflineIdentityMismatch) || errors.Is(err, errOfflineUninitialized) {
				reason = err.Error()
			}
			failures = append(failures, fmt.Sprintf("%s: %s", m.Name, reason))
		} else {
			observations[m.Name] = results[i].value
		}
	}
	return observations, strings.Join(failures, "; ")
}

type offlineRecoveryRetry struct {
	uid   string
	epoch uint64
	next  time.Time
	delay time.Duration
}

// This throttle lives in the provider because reconciler requeues are capped
// at five seconds. It carries no proof and is intentionally lost on CP restart.
func (p *Core) recoveryReady(rc *engine.Context, s *state) bool {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	key := rc.Cluster.Namespace + "/" + rc.Cluster.Name
	r := p.recoveryRetries[key]
	if s.Formation.Reformation == nil || s.Formation.Reformation.State != "observing" || r.uid != s.UID || r.epoch != s.Formation.FormationEpoch {
		delete(p.recoveryRetries, key)
		return true
	}
	return !now(rc).Before(r.next)
}

func (p *Core) finishRecoverySweep(rc *engine.Context, s *state) {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	key := rc.Cluster.Namespace + "/" + rc.Cluster.Name
	if s.Formation.Reformation == nil || s.Formation.Reformation.State != "observing" || s.Formation.Reformation.Blocked == "" {
		delete(p.recoveryRetries, key)
		return
	}
	if p.recoveryRetries == nil {
		p.recoveryRetries = make(map[string]offlineRecoveryRetry)
	}
	r := p.recoveryRetries[key]
	r.uid, r.epoch = s.UID, s.Formation.FormationEpoch
	r.delay = min(5*time.Minute, max(15*time.Second, 2*r.delay))
	r.next = now(rc).Add(r.delay)
	p.recoveryRetries[key] = r
}

func precheckOfflineRecovery(ctx context.Context, rc *engine.Context, s *state) string {
	_, blocked := collectOfflineRecovery(ctx, rc, s.data(), func(probe context.Context, m *member) (bool, error) {
		if m.Stage != "deployed" && m.Stage != "joined" {
			return false, errOfflineNotDeployed
		}
		a, err := rc.Agents.Dial(probe, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		status, err := engine.ObserveAgentStatus(probe, a, "mysql", m.Name)
		if err != nil {
			return false, err
		}
		if status.Processes["mysqld"].PID != 0 {
			return false, errOfflineNotStopped
		}
		return true, nil
	})
	return blocked
}
