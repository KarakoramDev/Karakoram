// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Inventory reads have no member-local receipt or side effects. Race eligible
// members so a reachable agent whose local PBM cannot reach the primary cannot
// hold up healthy peers (or the serialized cluster worker's readiness lease).
// Mutating commands and their durable receipt ownership still use command.
func inventoryCommand(ctx context.Context, env Env, name string) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	var probes sync.WaitGroup
	defer func() {
		cancel()
		probes.Wait()
	}()
	type result struct {
		output []byte
		err    error
	}
	results := make(chan result, len(env.Nodes))
	count := 0
	for _, node := range env.Nodes {
		if env.Sharded && node.ReplSet != "cfg" {
			continue
		}
		count++
		probes.Go(func() {
			probe, stop := context.WithTimeout(ctx, engine.ObservationTimeout)
			defer stop()
			out, err := commandMember(probe, env, node.Member, "", name, nil, "--out=json")
			if err != nil {
				err = fmt.Errorf("member %s: %w", node.Member, err)
			}
			results <- result{out, err}
		})
	}
	if count == 0 {
		return nil, errors.New("no eligible PBM inventory member")
	}
	var failures []error
	for range count {
		select {
		case result := <-results:
			if result.err == nil {
				return result.output, nil
			}
			failures = append(failures, result.err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("PBM %s inventory unavailable: %w", name, errors.Join(failures...))
}
