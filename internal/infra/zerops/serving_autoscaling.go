// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// CPU-mode switches are deliberately excluded: unlike changing bounds within a
// Linux container, their placement/restart behavior is not a serving guarantee.
func (*Backend) ResourceUpdatePreservesServingFor(current, target infra.Resources) bool {
	return current.CPUMode == target.CPUMode && current.MinDisk == target.MinDisk && current.MaxDisk == target.MaxDisk
}

func servingAutoscalingOp(id string, receipt operationReceipt) infra.Op {
	raw, _ := json.Marshal(receipt)
	return infra.Op{ID: "serving:" + string(raw), Kind: "serving-autoscaling", ServiceID: id}
}

func (b *Backend) servingAutoscaling(ctx context.Context, s *ServiceStack, config map[string]json.RawMessage) (infra.Op, error) {
	raw, err := json.Marshal(config)
	if err != nil {
		return infra.Op{}, err
	}
	receipt, err := b.receipt(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	if receipt.Action != "" {
		if receipt.Action != "autoscaling" || receipt.AutoscalingConfig != string(raw) {
			return infra.Op{}, unknownOutcome(receipt.Action, receipt.Stage)
		}
		return servingAutoscalingOp(s.ID, receipt), nil
	}
	failures, _, err := b.autoscalingFailures(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	for _, failure := range failures {
		if !failure.Observed && failure.Receipt.AutoscalingConfig == string(raw) {
			// Unobserved failures are replays, not permission to retry.
			return servingAutoscalingOp(s.ID, failure.Receipt), nil
		}
	}
	containers, err := b.client.Containers(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	ids := []string{}
	for _, c := range containers {
		if c.Status == "ACTIVE" || c.Status == "RUNNING" {
			if c.ID == "" {
				return infra.Op{}, fmt.Errorf("zerops: running container has no immutable identity")
			}
			ids = append(ids, c.ID)
		}
	}
	if len(ids) == 0 {
		return infra.Op{}, fmt.Errorf("zerops: serving autoscaling requires running incumbent containers")
	}
	slices.Sort(ids)
	baseline, _ := json.Marshal(ids)
	horizontal, err := decodeObject(config["horizontalAutoscaling"])
	if err != nil {
		return infra.Op{}, err
	}
	processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(s.ID)+"/process")
	if err != nil {
		return infra.Op{}, err
	}
	processIDs := []string{}
	for _, p := range processes {
		processIDs = append(processIDs, p.ID)
	}
	processBaseline, _ := json.Marshal(processIDs)
	receipt = operationReceipt{Action: "autoscaling", Stage: "submission", AutoscalingConfig: string(raw), Incumbents: string(baseline), MinContainers: max(1, int(number(horizontal, "minContainerCount"))), MaxContainers: max(1, int(number(horizontal, "maxContainerCount"))), AllocationBaseline: string(processBaseline)}
	if err = b.saveReceipt(ctx, s.ID, receipt); err != nil {
		return infra.Op{}, err
	}
	p, err := b.client.Autoscaling(ctx, s.ID, config)
	if err != nil {
		if ambiguousMutation(err) {
			return servingAutoscalingOp(s.ID, receipt), nil
		}
		if e := b.replaceReceipt(ctx, s.ID, operationReceipt{}, &receipt); e != nil {
			return infra.Op{}, e
		}
		return infra.Op{}, rejectedUpdate(err, "resource update", s.Name, attemptedResources(config))
	}
	if p != nil {
		receipt.Process = p.ID
	}
	// The pre-request receipt retains the baseline if storing the reply fails.
	if err = b.saveReceipt(ctx, s.ID, receipt); err != nil {
		return infra.Op{}, err
	}
	return servingAutoscalingOp(s.ID, receipt), nil
}

func (b *Backend) servingAutoscalingStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	lock := b.operationLock(op.ServiceID)
	if err := lock.Lock(ctx); err != nil {
		return infra.OpState{}, err
	}
	defer lock.Unlock()
	var receipt operationReceipt
	if !strings.HasPrefix(op.ID, "serving:") || json.Unmarshal([]byte(strings.TrimPrefix(op.ID, "serving:")), &receipt) != nil || receipt.Action != "autoscaling" || receipt.Incumbents == "" {
		return infra.OpState{}, &infra.RejectedError{Message: "zerops: invalid serving autoscaling receipt"}
	}
	failures, _, err := b.autoscalingFailures(ctx, op.ServiceID)
	if err != nil {
		return infra.OpState{}, err
	}
	if failure, ok := failures[op.ID]; ok {
		if err := b.replaceReceipt(ctx, op.ServiceID, operationReceipt{}, &failure.Receipt); err != nil {
			return infra.OpState{}, err
		}
		return failure.State, nil
	}
	process := receipt.Process
	if process == "" {
		var baseline []string
		if err := json.Unmarshal([]byte(receipt.AllocationBaseline), &baseline); err != nil {
			return infra.OpState{}, err
		}
		processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(op.ServiceID)+"/process")
		if err != nil {
			return infra.OpState{}, err
		}
		for _, p := range processes {
			if slices.Contains(baseline, p.ID) || !strings.Contains(strings.ToLower(p.ActionName), "autoscal") {
				continue
			}
			if p.ID == "" || p.ServiceStackID != "" && p.ServiceStackID != op.ServiceID || p.ProjectID != "" && p.ProjectID != b.client.project {
				return infra.OpState{}, unknownOutcome("autoscaling", "inconsistent process identity")
			}
			if process != "" {
				return infra.OpState{}, unknownOutcome("autoscaling", "multiple new autoscaling processes")
			}
			process = p.ID
		}
		if process == "" {
			return infra.OpState{Status: "OUTCOME_UNKNOWN", Message: unknownOutcome("autoscaling", "waiting for the original autoscaling process; do not resubmit").Error()}, nil
		}
	}
	status, err := b.OpStatus(ctx, processOp(&Process{ID: process}, "autoscaling", op.ServiceID))
	if err != nil || !status.Done {
		return status, err
	}
	if status.Failed {
		return b.finishFailedAutoscaling(ctx, op, receipt, status)
	}
	containers, err := b.client.Containers(ctx, op.ServiceID)
	if err != nil {
		return infra.OpState{}, err
	}
	var incumbents []string
	if err = json.Unmarshal([]byte(receipt.Incumbents), &incumbents); err != nil {
		return infra.OpState{}, err
	}
	running := []string{}
	for _, c := range containers {
		if c.Status == "ACTIVE" || c.Status == "RUNNING" {
			running = append(running, c.ID)
		}
	}
	failed := func() (infra.OpState, error) {
		status := infra.OpState{Done: true, Failed: true, Status: "INCUMBENTS_REPLACED", Message: "zerops: autoscaling replaced incumbent Linux containers; serving preservation could not be established; inspect platform processes and proxy readiness before retrying"}
		return b.finishFailedAutoscaling(ctx, op, receipt, status)
	}
	if receipt.MinContainers >= len(incumbents) {
		for _, id := range incumbents {
			if !slices.Contains(running, id) {
				return failed()
			}
		}
	} else {
		for _, id := range running {
			if !slices.Contains(incumbents, id) {
				return failed()
			}
		}
	}
	if len(running) < receipt.MinContainers || len(running) > receipt.MaxContainers {
		return infra.OpState{Status: "WAITING_CONTAINERS", Message: "waiting for requested running container count"}, nil
	}
	if err = b.replaceReceipt(ctx, op.ServiceID, operationReceipt{}, &receipt); err != nil {
		return infra.OpState{}, err
	}
	if err = b.clearAutoscalingFailures(ctx, op.ServiceID); err != nil {
		return infra.OpState{}, err
	}
	return status, nil
}
