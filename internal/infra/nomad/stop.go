// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const stopCompletionGrace = 15 * time.Second

type stoppingAllocation struct {
	ID, NodeID, NodeName, ClientStatus, DesiredStatus string
	TaskStates                                        map[string]struct {
		Events []struct {
			Type string
			Time int64
		}
	}
	Job struct {
		observedJob
		TaskGroups []struct {
			Tasks []struct {
				Name        string
				KillTimeout time.Duration
			}
		}
	}
}

func (b *Backend) stalledStops(ctx context.Context, r *record, as []allocation) ([]infra.RuntimeStopFailure, error) {
	var failures []infra.RuntimeStopFailure
	for _, a := range as {
		if allocationTerminal(a) || a.DesiredStatus != "stop" {
			continue
		}
		// Job allocation listings may omit task events and the original jobspec.
		// Hydrate only stopping allocations, then recheck their current state.
		var detail stoppingAllocation
		if _, err := b.c.get(ctx, "/v1/allocation/"+url.PathEscape(a.ID), nil, &detail); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if detail.DesiredStatus != "stop" || allocationTerminal(allocation{ClientStatus: detail.ClientStatus}) {
			continue
		}
		if !b.ownsJob(r, detail.Job.observedJob) {
			return nil, jobOwnershipConflict()
		}
		stalled := false
		for _, group := range detail.Job.TaskGroups {
			for _, task := range group.Tasks {
				timeout := task.KillTimeout
				if timeout <= 0 {
					timeout = 30 * time.Second
				}
				// A retained historical Killing event must not diagnose a restarted task.
				var killing int64
				for _, event := range detail.TaskStates[task.Name].Events {
					if event.Type == "Started" {
						killing = 0
					}
					if event.Type == "Killing" {
						killing = event.Time
					}
				}
				if killing > 0 && b.now().After(time.Unix(0, killing).Add(timeout+stopCompletionGrace)) {
					stalled = true
				}
			}
		}
		if !stalled {
			continue
		}
		node := detail.NodeName
		if node == "" {
			node = detail.NodeID
		}
		message := sanitizedMessage(fmt.Sprintf("member %s, allocation %s, node %s: Nomad stop stalled beyond kill_timeout plus 15s grace; allocation remains %s/desired-stop and is not fenced; close any open nomad alloc exec session (use -i=false -t=false for noninteractive commands), then inspect the client/executor if stopping persists", r.Hostname, a.ID, node, detail.ClientStatus), b.c.currentToken())
		failures = append(failures, infra.RuntimeStopFailure{Member: r.Hostname, Allocation: a.ID, Node: node, Message: message})
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].Allocation < failures[j].Allocation })
	return failures, nil
}

func (b *Backend) RuntimeStopFailures(ctx context.Context, members []string) ([]infra.RuntimeStopFailure, error) {
	ctx = infra.ObservationContext(ctx)
	results := make([][]infra.RuntimeStopFailure, len(members))
	err := infra.ObserveParallel(ctx, len(members), func(i int) error {
		as, err := b.allocations(ctx, members[i])
		if err != nil {
			return err
		}
		stopping := false
		for _, a := range as {
			stopping = stopping || a.DesiredStatus == "stop" && !allocationTerminal(a)
		}
		if !stopping {
			return nil
		}
		// Healthy members need only one inventory read. Fetch record ownership
		// and the original allocation jobspec only for a stop candidate.
		r, err := b.load(ctx, members[i])
		if err != nil || r == nil {
			return err
		}
		if r.Kind != infra.KindRuntime {
			return nil
		}
		results[i], err = b.stalledStops(ctx, r, as)
		return err
	})
	if err != nil {
		return nil, err
	}
	var failures []infra.RuntimeStopFailure
	for _, result := range results {
		failures = append(failures, result...)
	}
	sort.Slice(failures, func(i, j int) bool {
		return failures[i].Member < failures[j].Member || failures[i].Member == failures[j].Member && failures[i].Allocation < failures[j].Allocation
	})
	return failures, nil
}
