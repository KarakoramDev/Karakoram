// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package zerops

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func sameReceiptVersion(v AppVersion, id, project string, receipt operationReceipt) bool {
	return v.ID == receipt.Version && (v.ServiceStackID == "" || v.ServiceStackID == id) && (v.ProjectID == "" || v.ProjectID == project)
}

const terminalDeploymentRetries = 3
const terminalDeploymentClockMargin = 5 * time.Second

func (b *Backend) deploymentRetryNow() time.Time {
	if b.retryNow != nil {
		return b.retryNow()
	}
	return time.Now()
}

func retryableDeploymentStatus(status string) bool {
	return status == "DEPLOY_FAILED" || status == "PREPARING_RUNTIME_FAILED"
}

// A failed version alone is insufficient: its deployment process may still be
// running or a second deployment may have been submitted. Capture a complete,
// terminal process baseline before authorizing another exact-version attempt.
func (b *Backend) terminalDeploymentBaseline(ctx context.Context, id string, receipt operationReceipt) (string, string, error) {
	processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(id)+"/process")
	if err != nil {
		return "", "", operationWaiting("terminal deployment recovery", "waiting for complete service process evidence", time.Time{}, err)
	}
	ids := []string{}
	failed := false
	prior, err := requestBaseline(receipt)
	if err != nil {
		return "", "", err
	}
	var failures []string
	seen := make(map[string]bool, len(processes))
	for _, p := range processes {
		if p.ID == "" || seen[p.ID] || p.ServiceStackID != "" && p.ServiceStackID != id || p.ProjectID != "" && p.ProjectID != b.client.project {
			return "", "", unknownOutcome("deploy", "terminal process identity")
		}
		seen[p.ID] = true
		switch p.Status {
		case "FINISHED", "FAILED", "CANCELED":
		default:
			return "", "", operationWaiting("terminal deployment recovery", "waiting for all service processes to become terminal", time.Time{}, nil)
		}
		deploy := strings.Contains(strings.ToLower(p.ActionName), "deploy")
		if deploy && p.AppVersion == nil {
			return "", "", unknownOutcome("deploy", "unattributed terminal process")
		}
		if p.AppVersion != nil && p.AppVersion.ID == receipt.Version {
			if !deploy {
				return "", "", unknownOutcome("deploy", "unattributed version process")
			}
			ids = append(ids, p.ID)
			failed = failed || p.Status == "FAILED"
			if p.Status == "FAILED" && !prior[p.ID] {
				b.client.tokenMu.RLock()
				token := b.client.token
				b.client.tokenMu.RUnlock()
				failures = append(failures, processErrorSummary(p.Error, token))
			}
		}
	}
	if !failed {
		return "", "", unknownOutcome("deploy", "missing failed exact-version process")
	}
	sort.Strings(failures)
	sort.Strings(ids)
	encoded, _ := json.Marshal(ids)
	return string(encoded), boundedDiagnostic(strings.Join(failures, "; ")), nil
}

func (b *Backend) scheduleTerminalDeploymentRetry(ctx context.Context, id string, receipt operationReceipt) error {
	baseline, failure, err := b.terminalDeploymentBaseline(ctx, id, receipt)
	if err != nil {
		return err
	}
	if failure != "" {
		receipt.DeployFailure = failure
	}
	receipt.RequestBaseline = baseline
	if deploymentRetries(receipt)-receipt.DeployBudgetStart >= terminalDeploymentRetries {
		receipt.Stage = "exhausted"
		if err := b.saveReceipt(ctx, id, receipt); err != nil {
			return err
		}
		return exhaustedDeployment(id, receipt)
	}
	receipt.DeployRetries++
	receipt.Stage = "deploy-ready"
	delay := (15 * time.Second) << (deploymentRetries(receipt) - receipt.DeployBudgetStart - 1)
	receipt.DeployRetryAfter = b.deploymentRetryNow().Add(delay).UnixNano()
	if err := b.saveReceipt(ctx, id, receipt); err != nil {
		return err
	}
	return terminalDeploymentWaiting(receipt)
}

func exhaustedDeployment(service string, receipt operationReceipt) error {
	return &infra.DeploymentExhaustedError{Service: service, Version: receipt.Version, Attempts: deploymentRetries(receipt) + 1, LastError: receipt.DeployFailure}
}

func terminalDeploymentWaiting(receipt operationReceipt) error {
	message := fmt.Sprintf("zerops: confirmed terminal deployment failure; deployment retry %d scheduled after durable backoff", deploymentRetries(receipt))
	if receipt.DeployFailure != "" {
		message += "; last platform error: " + receipt.DeployFailure
	}
	return &infra.RetryAfterError{Cause: &allocationEvidenceError{detail: message}, At: time.Unix(0, receipt.DeployRetryAfter)}
}

func requestBaseline(receipt operationReceipt) (map[string]bool, error) {
	if receipt.RequestBaseline == "" {
		return nil, nil
	}
	var ids []string
	if json.Unmarshal([]byte(receipt.RequestBaseline), &ids) != nil || len(ids) == 0 {
		return nil, unknownOutcome("deploy", "invalid terminal process baseline")
	}
	baseline := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || baseline[id] {
			return nil, unknownOutcome("deploy", "invalid terminal process baseline")
		}
		baseline[id] = true
	}
	return baseline, nil
}
