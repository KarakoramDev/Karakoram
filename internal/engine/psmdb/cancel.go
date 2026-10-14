// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
)

// CanCancelOperation refuses any checkpoint that may already have changed data.
// FCV executes atomically in one call; cancellation is honored before that call.
func (*Provider) CanCancelOperation(op *domain.Operation) error {
	var cp struct {
		Step       string `json:"step"`
		Stage      string `json:"stage"`
		Cancelling bool   `json:"cancelling"`
	}
	if err := decodeJSON(op.Checkpoint, &cp); err != nil {
		return err
	}
	if cp.Cancelling {
		return nil
	}
	switch op.Kind {
	case domain.OperationRaiseFCV:
		if cp.Stage != "apply-fcv" {
			return nil
		}
	case domain.OperationBackup:
		switch cp.Step {
		case "", "configure", "start", "poll":
			return nil
		}
	case domain.OperationRestore:
		switch cp.Step {
		case "", "configure-source", "source-resync", "source-inventory", "disable-pitr":
			return nil
		}
	}
	checkpoint := cp.Step
	if checkpoint == "" {
		checkpoint = cp.Stage
	}
	return fmt.Errorf("Operation %s (%s) cannot be cancelled safely at checkpoint %q: data mutation or cleanup may already be in progress", op.ID, op.Kind, checkpoint)
}

func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	// The request was authorized against an unchanged durable checkpoint. An
	// in-flight safe step may have advanced once before persisting the request;
	// the worker must honor it before executing the next (possibly unsafe) step.
	if op.Kind != domain.OperationBackup && op.Kind != domain.OperationRestore && op.Kind != domain.OperationRaiseFCV {
		return engine.OpResult{}, fmt.Errorf("Operation kind %s does not support safe cancellation", op.Kind)
	}
	if op.Kind == domain.OperationRaiseFCV {
		return engine.OpResult{Done: true, Step: "before-fcv"}, nil
	}
	beforeStart, err := pbm.PrepareCancellation(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if beforeStart {
		return engine.OpResult{Done: true, Step: "before-start"}, nil
	}
	result, runErr := p.RunOperation(ctx, rc, op)
	if result.Done {
		var final struct {
			Failure string `json:"failure"`
		}
		if err = json.Unmarshal(result.Checkpoint, &final); err != nil {
			return result, err
		}
		if runErr != nil && final.Failure == "" {
			return result, runErr
		}
		if op.Kind == domain.OperationRestore {
			s, err := load(rc.Cluster)
			if err != nil {
				return result, err
			}
			s.Phase = "ready"
			if err = save(rc.Cluster, s); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	return result, runErr
}
