// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Allocation does not start a process or deploy code. Its list response has
// immutable IDs but no request token/name. Under exclusive service mutation
// ownership, an exact pre-request inventory identifies a sole new allocation.
func (b *Backend) allocationVersions(ctx context.Context, id string) ([]AppVersion, error) {
	versions, err := getPages[AppVersion](ctx, b.client, "/service-stack/"+escape(id)+"/app-version")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(versions))
	for _, v := range versions {
		if v.ID == "" || seen[v.ID] || v.ServiceStackID != "" && v.ServiceStackID != id || v.ProjectID != "" && v.ProjectID != b.client.project {
			return nil, errors.New("zerops: incomplete or conflicting app-version inventory")
		}
		seen[v.ID] = true
	}
	return versions, nil
}

func allocationBaseline(receipt operationReceipt) (map[string]bool, error) {
	var ids []string
	if err := json.Unmarshal([]byte(receipt.AllocationBaseline), &ids); err != nil || ids == nil {
		return nil, errors.New("zerops: invalid allocation inventory receipt; inspect the service operation marker")
	}
	baseline := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || baseline[id] {
			return nil, errors.New("zerops: invalid allocation inventory receipt; inspect the service operation marker")
		}
		baseline[id] = true
	}
	return baseline, nil
}

func (b *Backend) newAllocationReceipt(ctx context.Context, id, fingerprint string) (operationReceipt, error) {
	versions, err := b.allocationVersions(ctx, id)
	if err != nil {
		return operationReceipt{}, err
	}
	ids := make([]string, 0, len(versions))
	for _, v := range versions {
		ids = append(ids, v.ID)
	}
	sort.Strings(ids)
	encoded, _ := json.Marshal(ids)
	return operationReceipt{Action: "deploy", Stage: "creating", Fingerprint: fingerprint, AllocationBaseline: string(encoded)}, nil
}

// Only locally constructed, credential-free detail is exposed. The cause still
// carries the evidence-read classification and any Retry-After timestamp.
type allocationEvidenceError struct {
	detail string
	cause  error
}

func (e *allocationEvidenceError) Error() string           { return e.detail }
func (e *allocationEvidenceError) SafeDetail() string      { return e.detail }
func (e *allocationEvidenceError) Unwrap() error           { return e.cause }
func (e *allocationEvidenceError) Is(target error) bool    { return target == infra.ErrConflict }
func (e *allocationEvidenceError) DeploymentWaiting() bool { return true }

func allocationWaiting(detail string, at time.Time, cause error) error {
	return operationWaiting("app-version allocation", detail, at, cause)
}

func (b *Backend) resolveAllocation(ctx context.Context, id string, receipt operationReceipt) (operationReceipt, error) {
	versions, err := b.allocationVersions(ctx, id)
	if err != nil {
		return receipt, allocationWaiting("waiting for complete app-version evidence; restore successful service-scoped listings", time.Time{}, err)
	}
	processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(id)+"/process")
	if err != nil {
		return receipt, allocationWaiting("waiting for complete process evidence; restore successful service-scoped listings", time.Time{}, err)
	}
	for _, p := range processes {
		if p.ID == "" || p.ServiceStackID != "" && p.ServiceStackID != id || p.ProjectID != "" && p.ProjectID != b.client.project {
			return receipt, allocationWaiting("waiting for complete process evidence; service identity is missing or conflicting", time.Time{}, nil)
		}
		switch p.Status {
		case "FINISHED", "FAILED", "CANCELED":
		default:
			return receipt, allocationWaiting("waiting for non-terminal process to finish; the next reconciliation will re-read service processes and app versions", time.Time{}, nil)
		}
	}
	now := time.Now()
	if receipt.AllocationBaseline == "" {
		if len(versions) != 0 {
			return receipt, allocationWaiting("legacy allocation receipt has no immutable-ID baseline; inspect existing app versions and confirm the original outcome before clearing the operation marker", time.Time{}, nil)
		}
		// An old empty receipt has no trustworthy request time. Start a full
		// delivery/visibility margin at the first successful observation instead.
		receipt.AllocationBaseline = "[]"
		receipt.SettleAfter = now.Add(2 * b.client.http.Timeout).UnixNano()
	}
	baseline, err := allocationBaseline(receipt)
	if err != nil {
		return receipt, err
	}
	candidates := make([]AppVersion, 0, 1)
	observed := make(map[string]bool, len(versions))
	for _, v := range versions {
		observed[v.ID] = true
		if !baseline[v.ID] {
			candidates = append(candidates, v)
		}
	}
	for old := range baseline {
		if !observed[old] {
			return receipt, allocationWaiting("waiting for complete app-version evidence; a baseline version is missing from the listing", time.Time{}, nil)
		}
	}
	if len(candidates) > 1 {
		return receipt, allocationWaiting("multiple unselected app versions have no acknowledged allocation identity; inspect the inventory before resolving the operation marker", time.Time{}, nil)
	}
	if len(candidates) == 1 {
		candidate := candidates[0]
		if candidate.Status != "UPLOADING" {
			return receipt, allocationWaiting("new app version is not an unuploaded allocation; inspect the original version and any external deployment before resolving the operation marker", time.Time{}, nil)
		}
		// Corroborate list identity/status with the independent exact-ID read.
		exact, e := b.version(ctx, candidate.ID)
		if e != nil {
			return receipt, allocationWaiting("waiting for exact app-version evidence", time.Time{}, e)
		}
		if exact.ID != candidate.ID || exact.Status != "UPLOADING" || exact.ServiceStackID != "" && exact.ServiceStackID != id || exact.ProjectID != "" && exact.ProjectID != b.client.project {
			return receipt, allocationWaiting("waiting for consistent exact app-version evidence", time.Time{}, nil)
		}
		receipt.Version = candidate.ID
		receipt.Stage = "upload-ready"
		if err = b.saveReceipt(ctx, id, receipt); err != nil {
			return receipt, err
		}
		return receipt, nil
	}
	if receipt.SettleAfter <= 0 {
		receipt.SettleAfter = now.Add(2 * b.client.http.Timeout).UnixNano()
	}
	if receipt.EmptySince == 0 {
		receipt.EmptySince = now.UnixNano()
		if err = b.saveReceipt(ctx, id, receipt); err != nil {
			return receipt, err
		}
		until := time.Unix(0, receipt.SettleAfter)
		if poll := now.Add(b.pollInterval); poll.After(until) {
			until = poll
		}
		return receipt, allocationWaiting("waiting for allocation delivery/visibility settle window and a second empty-inventory/terminal-process observation until "+until.UTC().Format(time.RFC3339Nano), until, nil)
	}
	until := time.Unix(0, receipt.SettleAfter)
	if poll := time.Unix(0, receipt.EmptySince).Add(b.pollInterval); poll.After(until) {
		until = poll
	}
	if now.Before(until) {
		return receipt, allocationWaiting("waiting for allocation delivery/visibility settle window until "+until.UTC().Format(time.RFC3339Nano), until, nil)
	}
	// Both complete observations agree, all service processes are terminal, and
	// the request bound plus visibility margin elapsed. The caller persists a
	// fresh bound before retrying allocation. A later original allocation can
	// only be inert: upload/deploy always use the one durable selected ID.
	return receipt, nil
}
