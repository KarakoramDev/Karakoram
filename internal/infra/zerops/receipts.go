// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// A readable, non-secret service variable survives client/control-plane restarts.
// Write the stage BEFORE sending a mutation; an unacknowledged allocation or
// restart must not be submitted again merely because its response was lost.
const operationReceiptKey = "KARAKORAM_PLATFORM_OPERATION"

type operationReceipt struct {
	Action      string `json:"action"`
	Stage       string `json:"stage"`
	Version     string `json:"version,omitempty"`
	Process     string `json:"process,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	// JSON-encoded sorted immutable IDs; a string keeps receipts comparable
	// during the final consumption check. Empty means a legacy receipt.
	AllocationBaseline string `json:"allocationBaseline,omitempty"`
	SettleAfter        int64  `json:"settleAfter,omitempty"`
	EmptySince         int64  `json:"emptySince,omitempty"`
	RequestSettleAfter int64  `json:"requestSettleAfter,omitempty"`
	RequestEmptySince  int64  `json:"requestEmptySince,omitempty"`
	// Terminal retries retain uploaded bytes and fence each request against
	// previously completed deployment processes for that same app version.
	DeployRetries     int    `json:"deployRetries,omitempty"`
	DeployRetryAfter  int64  `json:"deployRetryAfter,omitempty"`
	RequestBaseline   string `json:"requestBaseline,omitempty"`
	DeployFailure     string `json:"deployFailure,omitempty"`
	RetryEpoch        int64  `json:"retryEpoch,omitempty"`
	DeployBudgetStart int    `json:"deployBudgetStart,omitempty"`
	// PriorRetries carries consumed retries across replacement allocations;
	// DeployRetries counts exact-version retries for the currently selected ID.
	PriorRetries int `json:"priorRetries,omitempty"`
}

func (b *Backend) receipt(ctx context.Context, id string) (operationReceipt, error) {
	entries, err := b.client.Env(ctx, id)
	if err != nil {
		return operationReceipt{}, err
	}
	var receipt operationReceipt
	found := false
	for _, entry := range entries {
		if entry.Key != operationReceiptKey || entry.Content == "" {
			continue
		}
		if found || entry.Sensitive || entry.Type != "USER" || json.Unmarshal([]byte(entry.Content), &receipt) != nil || !validReceipt(receipt) {
			return operationReceipt{}, &infra.RejectedError{Message: "zerops: invalid platform operation receipt; inspect the service environment before recovery"}
		}
		found = true
	}
	return receipt, nil
}
func (b *Backend) saveReceipt(ctx context.Context, id string, receipt operationReceipt) error {
	return b.replaceReceipt(ctx, id, receipt, nil)
}

// expected is checked against the final environment read before consuming a
// receipt. The shared submission lock encloses this transition; Zerops has no CAS.
func (b *Backend) replaceReceipt(ctx context.Context, id string, receipt operationReceipt, expected *operationReceipt) error {
	ctx, cancel := context.WithTimeout(ctx, b.waitTimeout)
	defer cancel()
	value := ""
	if receipt.Action != "" {
		data, _ := json.Marshal(receipt)
		value = string(data)
	}
	entries, err := b.client.Env(ctx, id)
	if err != nil {
		return err
	}
	existing := ""
	for _, entry := range entries {
		if expected != nil && entry.Key == operationReceiptKey {
			var current operationReceipt
			if entry.Sensitive || entry.Type != "USER" || json.Unmarshal([]byte(entry.Content), &current) != nil || current != *expected {
				return nil
			}
		}
		if entry.Key == operationReceiptKey {
			existing = entry.ID
			break
		}
	}
	if expected != nil && existing == "" {
		return nil
	}
	p, err := b.client.SetUserData(ctx, id, existing, operationReceiptKey, value, false)
	if err != nil {
		// The receipt write itself can lose its response. A readable exact value
		// proves the safety fence was stored; never send a second process PUT.
		observed, e := b.client.Env(ctx, id)
		if e == nil {
			for _, entry := range observed {
				if entry.Key == operationReceiptKey && !entry.Sensitive && entry.Type == "USER" && entry.Content == value {
					return nil
				}
			}
		}
		return err
	}
	return b.waitOp(ctx, processOp(&p, "receipt", id))
}
func unknownOutcome(action, stage string) error {
	return &classifiedError{cause: fmt.Errorf("zerops: %s outcome unknown at %s; automatic replay refused; inspect service processes and app versions, wait for pending requests to settle, then clear %s only after confirming the outcome", action, stage, operationReceiptKey), class: infra.ErrConflict}
}
func receiptAction(kind string) bool {
	return kind == "deploy" || kind == "start" || kind == "stop" || kind == "restart"
}
func (b *Backend) clearReceipt(ctx context.Context, op infra.Op) error {
	if op.ServiceID == "" || !receiptAction(op.Kind) {
		return nil
	}
	receipt, err := b.receipt(ctx, op.ServiceID)
	if notFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if receipt.Process == op.ID || (receipt.Version != "" && op.ID == "version:"+receipt.Version) {
		return b.replaceReceipt(ctx, op.ServiceID, operationReceipt{}, &receipt)
	}
	return nil
}
func (b *Backend) version(ctx context.Context, id string) (AppVersion, error) {
	var v AppVersion
	err := b.client.json(ctx, "GET", "/app-version/"+escape(id), nil, &v, true)
	return v, err
}

func deploymentRetries(receipt operationReceipt) int {
	return receipt.PriorRetries + receipt.DeployRetries
}

func (b *Backend) missingVersionOutcome(ctx context.Context, s *ServiceStack, receipt operationReceipt) error {
	if err := b.waitPreviousDeployment(ctx, s, ""); err != nil {
		return err
	}
	receipt.DeployFailure = "selected app version is absent; complete terminal process evidence confirms that replacement requires a bounded deployment retry"
	if deploymentRetries(receipt)-receipt.DeployBudgetStart >= terminalDeploymentRetries {
		if receipt.Stage != "exhausted" {
			receipt.Stage = "exhausted"
			if err := b.saveReceipt(ctx, s.ID, receipt); err != nil {
				return err
			}
		}
		return exhaustedDeployment(s.ID, receipt)
	}
	return nil
}
func (b *Backend) versionStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	v, err := b.version(ctx, strings.TrimPrefix(op.ID, "version:"))
	if notFound(err) {
		var service ServiceStack
		if op.ServiceID != "" {
			var serviceErr error
			service, serviceErr = b.client.GetService(ctx, op.ServiceID)
			if notFound(serviceErr) {
				// The owning service is also definitively absent. Retire the
				// pending action as failed, so cleanup can discard its retry
				// without inventing successful deployment evidence.
				return infra.OpState{Done: true, Failed: true, Status: "ServiceAbsent", Message: "selected app version and its owning service are absent"}, nil
			}
			if serviceErr != nil {
				return infra.OpState{}, serviceErr
			}
		}
		if op.ServiceID == "" {
			return infra.OpState{}, unknownOutcome("deploy", "missing app version without owning service identity")
		}
		receipt, receiptErr := b.receipt(ctx, op.ServiceID)
		if receiptErr != nil {
			return infra.OpState{}, receiptErr
		}
		if receipt.Version != strings.TrimPrefix(op.ID, "version:") {
			return infra.OpState{}, unknownOutcome("deploy", "missing app version without matching receipt")
		}
		if err = b.missingVersionOutcome(ctx, &service, receipt); err != nil {
			if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				return infra.OpState{Done: true, Failed: true, Status: "DeploymentRetriesExhausted", Message: err.Error()}, err
			}
			return infra.OpState{Status: "PreviousDeployment", Message: err.Error()}, err
		}
		return infra.OpState{Done: true, Failed: true, Status: "MissingAppVersion", Message: "selected app version is absent and all prior platform deployments are terminal; replacement requires a bounded deployment retry"}, nil
	}
	if err != nil {
		return infra.OpState{}, err
	}
	state := infra.OpState{Status: v.Status}
	switch v.Status {
	case "ACTIVE", "BACKUP":
		state.Done = true
	case "DEPLOY_FAILED", "BUILD_FAILED", "PREPARING_RUNTIME_FAILED", "BUILD_VALIDATION_FAILED", "CANCELLED":
		state.Done = true
		state.Failed = true
		state.Message = "deployment failed; inspect the app-version diagnostics"
	case "UPLOADING", "WAITING_TO_DEPLOY":
		state.Status = "OUTCOME_UNKNOWN"
		state.Message = unknownOutcome("deploy", "submission").Error()
	}
	if state.Done && (v.ID != strings.TrimPrefix(op.ID, "version:") || op.ServiceID != "" && v.ServiceStackID != "" && v.ServiceStackID != op.ServiceID || v.ProjectID != "" && v.ProjectID != b.client.project) {
		return infra.OpState{Status: "OUTCOME_UNKNOWN"}, unknownOutcome("deploy", "terminal version identity")
	}
	if op.ServiceID != "" {
		receipt, e := b.receipt(ctx, op.ServiceID)
		if e != nil {
			return infra.OpState{}, e
		}
		if receipt.Version == v.ID {
			if receipt.Stage == "exhausted" {
				state.Done, state.Failed = true, true
				state.Status = "DeploymentRetriesExhausted"
				state.Message = exhaustedDeployment(op.ServiceID, receipt).Error()
				return state, exhaustedDeployment(op.ServiceID, receipt)
			}
			if receipt.Stage == "deploy-ready" {
				return unsubmittedDeployment(), nil
			}
			if receipt.Stage == "deploying" && (v.Status == "WAITING_TO_DEPLOY" || receipt.RequestBaseline != "") {
				ready, started, e := b.versionRequestEvidence(ctx, op.ServiceID, &receipt, v)
				if e != nil {
					return state, e
				}
				if ready {
					receipt.Stage = "deploy-ready"
					if e = b.saveReceipt(ctx, op.ServiceID, receipt); e != nil {
						return state, e
					}
					return unsubmittedDeployment(), nil
				}
				if started != nil {
					state.Status = started.Status
					state.Done = started.Status == "FINISHED" || started.Status == "FAILED" || started.Status == "CANCELED"
					state.Failed = started.Status == "FAILED" || started.Status == "CANCELED"
					state.Message = "observing the original deployment process for the selected app version"
				}
			}
			if receipt.AllocationBaseline != "" {
				versions, e := b.allocationVersions(ctx, op.ServiceID)
				if e != nil {
					// Even optional diagnostics must not discard a platform
					// cooldown and then immediately issue receipt mutations.
					return infra.OpState{}, e
				}
				if baseline, e := allocationBaseline(receipt); e == nil {
					for _, other := range versions {
						if other.ID != v.ID && !baseline[other.ID] {
							if state.Message != "" {
								state.Message += "; "
							}
							state.Message += "additional unselected app version observed; it is never uploaded or deployed by this operation; inspect the service app-version inventory before manual cleanup"
							break
						}
					}
				}
			}
		}
		if receipt.Version == v.ID && state.Failed && retryableDeploymentStatus(v.Status) {
			// Keep the exact uploaded version for durable recovery. A later
			// Deploy call still needs the original runtime payload and must
			// prove terminal process evidence before any resubmission.
			state.Message = fmt.Sprintf("deployment failed after %d deployment retries; at most %d retries are permitted; inspect deployment and runtime preparation diagnostics", deploymentRetries(receipt), terminalDeploymentRetries)
			if receipt.DeployFailure != "" {
				state.Message += "; last platform error: " + receipt.DeployFailure
			}
			return state, nil
		}
	}
	if state.Done {
		err = b.clearReceipt(ctx, op)
	}
	return state, err
}
func (b *Backend) deployWithReceipt(ctx context.Context, s *ServiceStack, name, artifactName, runtimeYAML string, reader io.Reader, size int64, retryEpoch int64) (infra.Op, error) {
	seeker := reader.(io.ReadSeeker) // Deploy spools non-seekable artifacts first.
	position, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return infra.Op{}, err
	}
	digest := sha256.New()
	_, _ = io.WriteString(digest, runtimeYAML)
	if _, err = io.Copy(digest, contextReader{ctx, reader}); err != nil {
		return infra.Op{}, err
	}
	if _, err = seeker.Seek(position, io.SeekStart); err != nil {
		return infra.Op{}, err
	}
	fingerprint := hex.EncodeToString(digest.Sum(nil))
	receipt, err := b.receipt(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	if receipt.Action != "" && receipt.Action != "deploy" {
		return infra.Op{}, unknownOutcome(receipt.Action, receipt.Stage)
	}
	var selected AppVersion
	freshAllocation := false
	if receipt.Version != "" {
		selected, err = b.version(ctx, receipt.Version)
		if notFound(err) {
			if retryEpoch > receipt.RetryEpoch {
				receipt.RetryEpoch = retryEpoch
				receipt.DeployBudgetStart = deploymentRetries(receipt)
			}
			if err = b.missingVersionOutcome(ctx, s, receipt); err != nil {
				return infra.Op{}, err
			}
			previous := receipt
			receipt, err = b.newAllocationReceipt(ctx, s.ID, fingerprint)
			if err != nil {
				return infra.Op{}, err
			}
			receipt.PriorRetries = deploymentRetries(previous) + 1
			receipt.DeployBudgetStart = previous.DeployBudgetStart
			receipt.RetryEpoch = previous.RetryEpoch
			freshAllocation = true
		}
		if err != nil {
			return infra.Op{}, err
		}
	}
	if receipt.Action == "deploy" && receipt.Fingerprint != fingerprint {
		if receipt.Version == "" || receipt.Stage == "creating" || receipt.Stage == "uploading" {
			return infra.Op{}, unknownOutcome("deploy", "changed intent with an unresolved original request")
		}
		v := selected
		var e error
		if !sameReceiptVersion(v, s.ID, b.client.project, receipt) {
			return infra.Op{}, unknownOutcome("deploy", "terminal version identity")
		}
		if !retryableDeploymentStatus(v.Status) {
			return infra.Op{}, &classifiedError{cause: errors.New("zerops: another deployment has an unconsumed operation receipt; observe its outcome before changing the artifact or runtime configuration"), class: infra.ErrConflict}
		}
		if receipt.Stage == "deploying" && receipt.RequestBaseline != "" {
			ready, started, e := b.versionRequestEvidence(ctx, s.ID, &receipt, v)
			if e != nil {
				return infra.Op{}, e
			}
			if !ready && (started == nil || started.Status != "FAILED" && started.Status != "CANCELED") {
				return infra.Op{}, unknownOutcome("deploy", "changed intent with an unresolved retry")
			}
		}
		if _, _, e = b.terminalDeploymentBaseline(ctx, s.ID, receipt); e != nil {
			return infra.Op{}, e
		}
		if e = b.replaceReceipt(ctx, s.ID, operationReceipt{}, &receipt); e != nil {
			return infra.Op{}, e
		}
		receipt, e = b.receipt(ctx, s.ID)
		if e != nil {
			return infra.Op{}, e
		}
		if receipt.Action != "" {
			return infra.Op{}, unknownOutcome("deploy", "changed terminal receipt")
		}
	}
	versionOp := func() infra.Op { return infra.Op{ID: "version:" + receipt.Version, Kind: "deploy", ServiceID: s.ID} }
	if receipt.Action == "deploy" && retryEpoch > receipt.RetryEpoch {
		receipt.RetryEpoch = retryEpoch
		receipt.DeployBudgetStart = deploymentRetries(receipt)
		if receipt.Stage == "exhausted" {
			receipt.DeployRetries++
			receipt.Stage = "deploy-ready"
			receipt.DeployRetryAfter = b.deploymentRetryNow().Add(15 * time.Second).UnixNano()
		} else if receipt.Stage == "deploy-ready" && receipt.DeployRetries > 0 {
			// The scheduled attempt consumes the first retry of the new budget.
			receipt.DeployBudgetStart--
		}
		if err := b.saveReceipt(ctx, s.ID, receipt); err != nil {
			return infra.Op{}, err
		}
	}
	// Receipts from older releases may contain an unsubmitted retry beyond
	// the hard cap. Cut them over without sending that scheduled attempt.
	if receipt.Stage == "deploy-ready" && deploymentRetries(receipt)-receipt.DeployBudgetStart > terminalDeploymentRetries {
		// Scheduled but unsent work is not an attempted deployment.
		receipt.DeployRetries--
		return infra.Op{}, b.scheduleTerminalDeploymentRetry(ctx, s.ID, receipt)
	}
	upload := false
	if receipt.Action != "" && !freshAllocation {
		switch receipt.Stage {
		case "exhausted":
			return infra.Op{}, exhaustedDeployment(name, receipt)
		case "submitted":
			return processOp(&Process{ID: receipt.Process}, "deploy", s.ID), nil
		case "deploying":
			v := selected
			if retryableDeploymentStatus(v.Status) {
				if !sameReceiptVersion(v, s.ID, b.client.project, receipt) {
					return infra.Op{}, unknownOutcome("deploy", "terminal version identity")
				}
				if receipt.RequestBaseline != "" {
					ready, started, e := b.versionRequestEvidence(ctx, s.ID, &receipt, v)
					if e != nil {
						return infra.Op{}, e
					}
					if ready {
						// The fenced retry was never sent. Reuse this attempt,
						// not another retry count or a new uploaded version.
						break
					}
					if started == nil || started.Status != "FAILED" && started.Status != "CANCELED" {
						return versionOp(), nil
					}
				}
				// Always returns the durable RetryAfter (or a persistence error).
				return infra.Op{}, b.scheduleTerminalDeploymentRetry(ctx, s.ID, receipt)
			}
			if v.Status != "WAITING_TO_DEPLOY" {
				return versionOp(), nil
			}
			ready, started, e := b.versionRequestEvidence(ctx, s.ID, &receipt, v)
			if e != nil {
				return infra.Op{}, e
			}
			if !ready || started != nil {
				return versionOp(), nil
			}
		case "deploy-ready":
			if receipt.DeployRetryAfter > 0 {
				// Retry cadence is wall-clock based, unlike terminal operation
				// budgets. A backward step must not postpone healing for hours:
				// an impossible future deadline is due, but evidence gates below
				// still apply before any deployment mutation.
				remaining := time.Unix(0, receipt.DeployRetryAfter).Sub(b.deploymentRetryNow())
				if remaining > 0 && remaining <= 60*time.Second+terminalDeploymentClockMargin {
					return infra.Op{}, terminalDeploymentWaiting(receipt)
				}
				v := selected
				if !sameReceiptVersion(v, s.ID, b.client.project, receipt) {
					return infra.Op{}, unknownOutcome("deploy", "terminal version identity")
				}
				if !retryableDeploymentStatus(v.Status) {
					return versionOp(), nil
				}
				baseline, _, e := b.terminalDeploymentBaseline(ctx, s.ID, receipt)
				if e != nil {
					return infra.Op{}, e
				}
				if baseline != receipt.RequestBaseline {
					return infra.Op{}, unknownOutcome("deploy", "changed process inventory during terminal retry backoff")
				}
			}
		case "upload-ready":
			upload = true
		case "uploading":
			v := selected
			if v.Status == "UPLOADING" {
				ready, started, e := b.versionRequestEvidence(ctx, s.ID, &receipt, v)
				if e != nil {
					return infra.Op{}, e
				}
				if started != nil {
					return versionOp(), nil
				}
				upload = ready
			} else if v.Status != "WAITING_TO_DEPLOY" {
				return versionOp(), nil
			}
		case "creating":
			receipt, err = b.resolveAllocation(ctx, s.ID, receipt)
			if err != nil {
				return infra.Op{}, err
			}
			upload = receipt.Version != ""
		default:
			return infra.Op{}, unknownOutcome("deploy", "app-version allocation")
		}
	} else if !freshAllocation {
		receipt, err = b.newAllocationReceipt(ctx, s.ID, fingerprint)
		if err != nil {
			return infra.Op{}, err
		}
		receipt.RetryEpoch = retryEpoch
	}
	if receipt.Version == "" || upload {
		if err := b.waitPreviousDeployment(ctx, s, receipt.Version); err != nil {
			return infra.Op{}, err
		}
	}
	release, err := b.admitDeployment(ctx, s)
	if err != nil {
		return infra.Op{}, err
	}
	submitted := false
	defer func() {
		if !submitted {
			release()
		} else {
			b.finishDeploymentSubmission(s.ID)
		}
	}()
	if receipt.Version == "" {
		// Receipt persistence and the one allocation share a bounded request
		// budget. The second budget is a delivery/visibility settle margin.
		receipt.SettleAfter = time.Now().Add(2 * b.client.http.Timeout).UnixNano()
		receipt.EmptySince = 0
		requestCtx, cancel := context.WithDeadline(ctx, time.Unix(0, receipt.SettleAfter).Add(-b.client.http.Timeout))
		defer cancel()
		if err = b.saveReceipt(requestCtx, s.ID, receipt); err != nil {
			return infra.Op{}, err
		}
		submitted = true
		v, e := b.client.CreateAppVersion(requestCtx, s.ID, artifactName)
		if e != nil {
			if !ambiguousMutation(e) {
				submitted = false
				if receipt.PriorRetries > 0 {
					// Keep consumed retries even when this replacement allocation
					// was rejected. The allocation fence safely resolves absence
					// before another attempt; clearing it would reset the budget.
					return infra.Op{}, e
				}
				if clearErr := b.saveReceipt(ctx, s.ID, operationReceipt{}); clearErr != nil {
					return infra.Op{}, errors.Join(e, clearErr)
				}
				return infra.Op{}, e
			}
			until := time.Unix(0, receipt.SettleAfter)
			return infra.Op{}, allocationWaiting("waiting for app-version/process evidence and allocation delivery/visibility settle window until "+until.UTC().Format(time.RFC3339Nano), until, nil)
		}
		if v.ID == "" {
			until := time.Unix(0, receipt.SettleAfter)
			return infra.Op{}, allocationWaiting("waiting for app-version/process evidence and allocation delivery/visibility settle window until "+until.UTC().Format(time.RFC3339Nano), until, nil)
		}
		receipt.Version = v.ID
		receipt.Stage = "upload-ready"
		if err = b.saveReceipt(ctx, s.ID, receipt); err != nil {
			return infra.Op{}, err
		}
		upload = true
	}
	if upload {
		if err := b.waitPreviousDeployment(ctx, s, receipt.Version); err != nil {
			return infra.Op{}, err
		}
		requestCtx, cancel, e := b.beginVersionRequest(ctx, s.ID, &receipt, "uploading")
		if e != nil {
			return infra.Op{}, e
		}
		submitted = true
		err = b.client.Upload(requestCtx, receipt.Version, reader, size)
		cancel()
		if err != nil {
			if !ambiguousMutation(err) {
				submitted = false
				receipt.Stage = "upload-ready"
				if e := b.saveReceipt(ctx, s.ID, receipt); e != nil {
					return infra.Op{}, errors.Join(err, e)
				}
				return infra.Op{}, err
			}
			// The upload response may be lost after accepting all bytes. Only an
			// authoritative app-version transition permits continuing without replay.
			observed, e := b.version(ctx, receipt.Version)
			if e != nil {
				return infra.Op{}, errors.Join(err, e)
			}
			if observed.Status != "WAITING_TO_DEPLOY" {
				until := time.Unix(0, receipt.RequestSettleAfter)
				return infra.Op{}, operationWaiting("artifact upload", "waiting for exact-version/process evidence and request settle window until "+until.UTC().Format(time.RFC3339Nano), until, err)
			}
		}
	}
	requestCtx, cancel, err := b.beginVersionRequest(ctx, s.ID, &receipt, "deploying")
	if err != nil {
		return infra.Op{}, err
	}
	submitted = true
	_, err = b.client.Deploy(requestCtx, receipt.Version, runtimeYAML, name)
	cancel()
	if err != nil {
		if ambiguousMutation(err) {
			return versionOp(), nil
		}
		submitted = false
		// An explicit rejection is not an accepted deployment. Preserve the
		// uploaded version so the next call can submit it after the refusal clears.
		receipt.Stage = "deploy-ready"
		if e := b.saveReceipt(ctx, s.ID, receipt); e != nil {
			return infra.Op{}, errors.Join(err, e)
		}
		return infra.Op{}, err
	}
	// The pre-submission version receipt already fences this deployment. A
	// user-data update here is queued behind the deployment on Zerops; waiting
	// for it can prevent quorum peers from ever being submitted. Observe the
	// durable app version instead, just as after an ambiguous deploy reply.
	return versionOp(), nil
}
func (b *Backend) lifecycleWithReceipt(ctx context.Context, s *ServiceStack, action string) (infra.Op, error) {
	receipt, err := b.receipt(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	if receipt.Action != "" {
		if receipt.Action == action && receipt.Process != "" {
			return processOp(&Process{ID: receipt.Process}, action, s.ID), nil
		}
		if receipt.Action == action && receipt.Process == "" && (action == "start" || action == "stop") {
			return b.observeLifecycleOutcome(ctx, s, action)
		}
		return infra.Op{}, unknownOutcome(receipt.Action, receipt.Stage)
	}
	receipt = operationReceipt{Action: action, Stage: "submission"}
	if err = b.saveReceipt(ctx, s.ID, receipt); err != nil {
		return infra.Op{}, err
	}
	p, err := b.client.Lifecycle(ctx, s.ID, action)
	if err != nil {
		if ambiguousMutation(err) {
			if action == "start" || action == "stop" {
				return b.observeLifecycleOutcome(ctx, s, action)
			}
			return infra.Op{}, unknownOutcome(action, "submission")
		}
		if e := b.saveReceipt(ctx, s.ID, operationReceipt{}); e != nil {
			return infra.Op{}, errors.Join(err, e)
		}
		return infra.Op{}, err
	}
	if p.ID == "" {
		return infra.Op{}, unknownOutcome(action, "submission")
	}
	receipt.Process = p.ID
	if err = b.saveReceipt(ctx, s.ID, receipt); err != nil {
		return infra.Op{}, err
	}
	return processOp(&p, action, s.ID), nil
}

func ambiguousMutation(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var api *APIError
	if errors.As(err, &api) {
		return api.StatusCode >= 500
	}
	return errors.Is(err, infra.ErrUnavailable)
}

func (b *Backend) observeLifecycleOutcome(ctx context.Context, s *ServiceStack, action string) (infra.Op, error) {
	observed, err := b.client.GetService(ctx, s.ID)
	if err != nil {
		return infra.Op{}, err
	}
	desired := infra.StatusActive
	if action == "stop" {
		desired = infra.StatusStopped
	}
	if observed.ID == s.ID && observed.Name == s.Name && !observed.protected() && (observed.ProjectID == "" || observed.ProjectID == b.client.project) && observed.Status == desired {
		if err = b.saveReceipt(ctx, s.ID, operationReceipt{}); err != nil {
			return infra.Op{}, err
		}
		return infra.Op{Kind: action, ServiceID: s.ID}, nil
	}
	return infra.Op{}, unknownOutcome(action, "submission")
}

func validReceipt(receipt operationReceipt) bool {
	if receipt.PriorRetries < 0 || receipt.DeployRetries < 0 || receipt.DeployRetryAfter < 0 || receipt.RetryEpoch < 0 || receipt.DeployBudgetStart < 0 || receipt.DeployBudgetStart > deploymentRetries(receipt) {
		return false
	}
	// Replacement allocations can consume the whole shared budget before the
	// selected version has any exact-version retry. Keep its terminal baseline
	// for re-arm, without inventing an attempted retry or a scheduled deadline.
	terminalReplacement := receipt.Stage == "exhausted" && receipt.PriorRetries > 0 && receipt.DeployRetries == 0 &&
		receipt.DeployRetryAfter == 0 && receipt.RequestBaseline != "" &&
		deploymentRetries(receipt)-receipt.DeployBudgetStart >= terminalDeploymentRetries
	if receipt.DeployRetries > 0 && (receipt.RequestBaseline == "" || receipt.DeployRetryAfter <= 0) || receipt.DeployRetries == 0 && (receipt.RequestBaseline != "" || receipt.DeployRetryAfter != 0) && !terminalReplacement {
		return false
	}
	if _, err := requestBaseline(receipt); err != nil {
		return false
	}
	switch receipt.Action {
	case "deploy", "start", "stop", "restart":
	default:
		return false
	}
	switch receipt.Stage {
	case "creating", "submission":
	case "upload-ready", "uploading", "deploy-ready", "deploying", "exhausted":
		if receipt.Version == "" {
			return false
		}
	case "submitted":
		if receipt.Process == "" {
			return false
		}
	default:
		return false
	}
	return true
}

// A process-retention expiry is not permission to replay a receipted restart.
// Deployments retain the independently observable app-version identity.
func (b *Backend) expiredReceiptStatus(ctx context.Context, op infra.Op) (infra.OpState, bool, error) {
	if op.ServiceID == "" || !receiptAction(op.Kind) {
		return infra.OpState{}, false, nil
	}
	receipt, err := b.receipt(ctx, op.ServiceID)
	if err != nil {
		return infra.OpState{}, true, err
	}
	if receipt.Process != op.ID {
		return infra.OpState{}, false, nil
	}
	if receipt.Action == "deploy" && receipt.Version != "" {
		state, err := b.versionStatus(ctx, infra.Op{ID: "version:" + receipt.Version, Kind: "deploy", ServiceID: op.ServiceID})
		return state, true, err
	}
	if receipt.Action == "start" || receipt.Action == "stop" {
		s, err := b.client.GetService(ctx, op.ServiceID)
		if err != nil {
			return infra.OpState{}, true, err
		}
		_, err = b.observeLifecycleOutcome(ctx, &s, receipt.Action)
		if err == nil {
			return infra.OpState{Done: true, Status: "FINISHED"}, true, nil
		}
		if !errors.Is(err, infra.ErrConflict) {
			return infra.OpState{}, true, err
		}
	}
	return infra.OpState{Status: "OUTCOME_UNKNOWN", Message: unknownOutcome(receipt.Action, "expired process receipt").Error()}, true, nil
}
