// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type evaluation struct {
	ID, Status, BlockedEval, PreviousEval string
	JobModifyIndex, CreateIndex           uint64
	FailedTGAllocs                        map[string]struct {
		NodesEvaluated, NodesFiltered, NodesExhausted         int
		ConstraintFiltered, ClassFiltered, DimensionExhausted map[string]int
	}
}

// currentEvaluation uses the latest evaluation of this exact job registration:
// the registration evaluation retains historical failures after a blocked eval
// is retried. Neither another job version nor an old rejection is current proof.
func (b *Backend) currentEvaluation(ctx context.Context, r *record, index uint64) (evaluation, error) {
	var evaluations []evaluation
	_, err := b.c.get(ctx, "/v1/job/"+r.Hostname+"/evaluations", nil, &evaluations)
	if err != nil {
		return evaluation{}, err
	}
	var latest evaluation
	for _, e := range evaluations {
		if e.JobModifyIndex == index && (latest.ID == "" || e.CreateIndex > latest.CreateIndex) {
			latest = e
		}
	}
	if latest.ID == "" {
		return latest, infra.ErrNotFound
	}
	// Read the selected evaluation itself: job evaluation listings can be
	// stubs, and its status may have advanced since the listing was sampled.
	_, err = b.c.get(ctx, "/v1/evaluation/"+latest.ID, nil, &latest)
	return latest, err
}

func (b *Backend) placementMessage(r *record, e evaluation) string {
	if e.Status != "blocked" && e.BlockedEval == "" && len(e.FailedTGAllocs) == 0 {
		return ""
	}
	groups := make([]string, 0, len(e.FailedTGAllocs))
	for group, f := range e.FailedTGAllocs {
		details := []string{fmt.Sprintf("evaluated=%d filtered=%d exhausted=%d", f.NodesEvaluated, f.NodesFiltered, f.NodesExhausted)}
		for dimension, count := range f.DimensionExhausted {
			details = append(details, fmt.Sprintf("dimension %q exhausted on %d nodes", dimension, count))
		}
		for constraint, count := range f.ConstraintFiltered {
			details = append(details, fmt.Sprintf("constraint %q filtered %d nodes", constraint, count))
		}
		for class, count := range f.ClassFiltered {
			details = append(details, fmt.Sprintf("class %q filtered %d nodes", class, count))
		}
		sort.Strings(details[1:])
		groups = append(groups, group+": "+strings.Join(details, ", "))
	}
	sort.Strings(groups)
	node := r.Node
	if node == "" {
		node = "scheduler-selected"
	}
	return sanitizedMessage(fmt.Sprintf("member %s, node %s: Nomad placement blocked (%s); FailedTGAllocs: %s", r.Hostname, node, e.Status, strings.Join(groups, "; ")), b.c.currentToken())
}

func currentAllocationsPlaced(as []allocation, version, index uint64, count int) bool {
	placed := 0
	for _, a := range as {
		if !allocationTerminal(a) && a.JobVersion == version && a.CreateIndex > index {
			placed++
		}
	}
	return placed >= count
}

func (b *Backend) PlacementFailures(ctx context.Context, members []string) ([]infra.PlacementFailure, error) {
	ctx = infra.ObservationContext(ctx)
	results := make([]infra.PlacementFailure, len(members))
	err := infra.ObserveParallel(ctx, len(members), func(i int) error {
		r, err := b.load(ctx, members[i])
		if err != nil {
			return err
		}
		if r == nil || r.Kind != infra.KindRuntime || r.Desired != "running" || r.Deployment == nil {
			return nil
		}
		_, job, err := b.operationJob(ctx, r)
		if errors.Is(err, infra.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if job.Stop {
			return nil
		}
		as, err := b.allocations(ctx, r.Hostname)
		if err != nil {
			return err
		}
		if currentAllocationsPlaced(as, job.Version, job.JobModifyIndex, r.Max) {
			return nil
		}
		e, err := b.currentEvaluation(ctx, r, job.JobModifyIndex)
		if errors.Is(err, infra.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if message := b.placementMessage(r, e); message != "" {
			results[i] = infra.PlacementFailure{Member: r.Hostname, Node: r.Node, Message: message}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := results[:0]
	for _, result := range results {
		if result.Member != "" {
			out = append(out, result)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Member < out[j].Member })
	return out, nil
}

// begin's durable CAS lease fences registrations before stop/purge, even when
// the previous deployment never allocated a task. Readiness is not a fence.
func (*Backend) SupersedesDeploymentsOnDelete() bool { return true }
