// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func offlineRecoveryTimeout(rc *engine.Context, m *member) time.Duration {
	ram := m.Resources.MinRAM
	if ram <= 0 {
		ram = rc.Cluster.Spec.Components.Engine.Resources.MinRAM
	}
	// These are native recovery operations, not health reads: private mysqld
	// startup, redo recovery and graceful shutdown must all finish before proof
	// is safe. A 60s floor gives ample headroom over the measured ~4s shutdown
	// envelope on 2GiB members; add 15s per additional GiB for larger caches,
	// up to the agent client's existing three-minute engine-action ceiling.
	// This is a retry budget, not a guarantee for arbitrary crash recovery/I/O.
	return 60*time.Second + time.Duration(math.Ceil(min(8, max(0, ram-2))))*15*time.Second
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
			}
			failures = append(failures, fmt.Sprintf("%s: %s", m.Name, reason))
		} else {
			observations[m.Name] = results[i].value
		}
	}
	return observations, strings.Join(failures, "; ")
}
