// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func operationWaiting(stage, detail string, at time.Time, cause error) error {
	message := fmt.Sprintf("zerops: deploy outcome unknown at %s; automatic replay refused: %s", stage, detail)
	if cause != nil {
		message += "; " + infra.SafeDetail(cause)
	}
	var result error = &allocationEvidenceError{detail: message, cause: cause}
	if !at.IsZero() {
		result = &infra.RetryAfterError{Cause: result, At: at}
	}
	return result
}

func (b *Backend) beginVersionRequest(ctx context.Context, id string, receipt *operationReceipt, stage string) (context.Context, context.CancelFunc, error) {
	receipt.Stage = stage
	receipt.RequestSettleAfter = time.Now().Add(2 * b.client.http.Timeout).UnixNano()
	receipt.RequestEmptySince = 0
	requestCtx, cancel := context.WithDeadline(ctx, time.Unix(0, receipt.RequestSettleAfter).Add(-b.client.http.Timeout))
	if err := b.saveReceipt(requestCtx, id, *receipt); err != nil {
		cancel()
		return nil, nil, err
	}
	return requestCtx, cancel, nil
}

// A request fence is durable before its request is sent. Pending version status
// alone cannot distinguish that crash boundary from a lost response. Require
// independent process evidence and repeated observations beyond the request and
// delivery/visibility bounds before retrying the same selected version.
func (b *Backend) versionRequestEvidence(ctx context.Context, id string, receipt *operationReceipt, v AppVersion) (bool, *Process, error) {
	stage := "deployment submission"
	if receipt.Stage == "uploading" {
		stage = "artifact upload"
	}
	waiting := func(detail string, cause error) (bool, *Process, error) {
		return false, nil, operationWaiting(stage, detail, time.Time{}, cause)
	}
	if v.ID != receipt.Version || v.ServiceStackID != "" && v.ServiceStackID != id || v.ProjectID != "" && v.ProjectID != b.client.project {
		return waiting("waiting for consistent exact app-version identity", nil)
	}
	processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(id)+"/process")
	if err != nil {
		return waiting("waiting for complete process evidence for the selected app version", err)
	}
	var started *Process
	baseline, err := requestBaseline(*receipt)
	if err != nil {
		return false, nil, err
	}
	nonterminal := false
	for i := range processes {
		p := &processes[i]
		if p.ID == "" || p.ServiceStackID != "" && p.ServiceStackID != id || p.ProjectID != "" && p.ProjectID != b.client.project {
			return waiting("waiting for complete service-scoped process identities", nil)
		}
		if baseline[p.ID] {
			if p.Status != "FINISHED" && p.Status != "FAILED" && p.Status != "CANCELED" {
				return waiting("a previously terminal deployment process is no longer terminal", nil)
			}
			continue
		}
		deploy := strings.Contains(strings.ToLower(p.ActionName), "deploy")
		if deploy && p.AppVersion == nil {
			return waiting("deployment process has no app-version identity; inspect platform process metadata", nil)
		}
		if p.AppVersion != nil && p.AppVersion.ID == receipt.Version {
			if !deploy {
				return waiting("selected version has an unattributed platform process; inspect its action before retrying", nil)
			}
			if started != nil {
				return waiting("selected version has multiple deployment processes; observe their original outcomes", nil)
			}
			started = p
		}
		switch p.Status {
		case "FINISHED", "FAILED", "CANCELED":
		default:
			nonterminal = true
		}
	}
	if started != nil {
		return false, started, nil
	}
	if nonterminal {
		return waiting("waiting for non-terminal service processes to finish before retrying the selected version", nil)
	}
	now := time.Now()
	if receipt.RequestSettleAfter <= 0 {
		receipt.RequestSettleAfter = now.Add(2 * b.client.http.Timeout).UnixNano()
	}
	if receipt.RequestEmptySince == 0 {
		receipt.RequestEmptySince = now.UnixNano()
		if err = b.saveReceipt(ctx, id, *receipt); err != nil {
			return false, nil, err
		}
	}
	until := time.Unix(0, receipt.RequestSettleAfter)
	if poll := time.Unix(0, receipt.RequestEmptySince).Add(b.pollInterval); poll.After(until) {
		until = poll
	}
	if now.Before(until) {
		return false, nil, operationWaiting(stage, "waiting for request delivery/visibility settle window and a second unchanged-version/terminal-process observation until "+until.UTC().Format(time.RFC3339Nano), until, nil)
	}
	return true, nil, nil
}

// OpStatus has no artifact/runtime payload. Retire only the infrastructure
// attempt, not its selected-version receipt: normal provider retry re-enters
// Deploy with the original intent and this same uploaded version.
func unsubmittedDeployment() infra.OpState {
	return infra.OpState{Done: true, Failed: true, Status: "RETRY_REQUIRED", Message: "deployment submission was not observed after the settle window; retry the same deployment to submit its retained uploaded app version"}
}
