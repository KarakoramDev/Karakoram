// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Terminal evidence is separate from the submission fence: recovery may stop or
// redeploy the runtime without forgetting why an earlier receipt failed.
const autoscalingFailuresKey = "KARAKORAM_AUTOSCALING_FAILURES"

type autoscalingFailure struct {
	Receipt  operationReceipt `json:"receipt"`
	State    infra.OpState    `json:"state"`
	Observed bool             `json:"observed"`
}

func (b *Backend) autoscalingFailures(ctx context.Context, id string) (map[string]autoscalingFailure, string, error) {
	entries, err := b.client.Env(ctx, id)
	if err != nil {
		return nil, "", err
	}
	failures := map[string]autoscalingFailure{}
	entryID := ""
	for _, entry := range entries {
		if entry.Key != autoscalingFailuresKey || entry.Content == "" {
			continue
		}
		if entryID != "" || entry.Sensitive || entry.Type != "USER" || json.Unmarshal([]byte(entry.Content), &failures) != nil || failures == nil {
			return nil, "", &infra.RejectedError{Message: "zerops: invalid autoscaling failure evidence; inspect the service environment before recovery"}
		}
		entryID = entry.ID
	}
	return failures, entryID, nil
}

func (b *Backend) finishFailedAutoscaling(ctx context.Context, op infra.Op, receipt operationReceipt, state infra.OpState) (infra.OpState, error) {
	_, entryID, err := b.autoscalingFailures(ctx, op.ServiceID)
	if err != nil {
		return infra.OpState{}, err
	}
	// Only the latest failure is retained. Observation is persisted before the
	// response; a subsequent explicit request may retry the failed target.
	failures := map[string]autoscalingFailure{op.ID: {Receipt: receipt, State: state, Observed: true}}
	raw, err := json.Marshal(failures)
	if err != nil {
		return infra.OpState{}, err
	}
	process, err := b.client.SetUserData(ctx, op.ServiceID, entryID, autoscalingFailuresKey, string(raw), false)
	if err != nil {
		// An ambiguous evidence write is safe only after exact read-back.
		observed, _, readErr := b.autoscalingFailures(ctx, op.ServiceID)
		observedRaw, _ := json.Marshal(observed)
		if readErr != nil || string(observedRaw) != string(raw) {
			return infra.OpState{}, err
		}
	} else if err = b.waitOp(ctx, processOp(&process, "receipt", op.ServiceID)); err != nil {
		return infra.OpState{}, err
	}
	if err = b.replaceReceipt(ctx, op.ServiceID, operationReceipt{}, &receipt); err != nil {
		return infra.OpState{}, err
	}
	return state, nil
}

func (b *Backend) clearAutoscalingFailures(ctx context.Context, id string) error {
	failures, entryID, err := b.autoscalingFailures(ctx, id)
	if err != nil || len(failures) == 0 {
		return err
	}
	process, err := b.client.SetUserData(ctx, id, entryID, autoscalingFailuresKey, "", false)
	if err != nil {
		observed, _, readErr := b.autoscalingFailures(ctx, id)
		if readErr == nil && len(observed) == 0 {
			return nil
		}
		return err
	}
	return b.waitOp(ctx, processOp(&process, "receipt", id))
}
