// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const deploymentReservationTTL = 5 * time.Minute

func deploymentLimit(configured int, raw string) (int, error) {
	if raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("zerops: KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS must be an integer >= 1")
		}
		return n, nil
	}
	if configured < 0 {
		return 0, fmt.Errorf("zerops: MaxInflightDeploys must be >= 1")
	}
	if configured == 0 {
		return 2, nil
	}
	return configured, nil
}

func admissionRetryAt(cause error) time.Time {
	at := time.Now().Add(15 * time.Second)
	if retry := infra.RetryAt(cause); retry.After(at) {
		at = retry
	}
	return at
}

type deploymentReservation struct {
	service, name string
	at            time.Time
	finishedAt    time.Time
	live          bool
	baseline      map[string]struct{}
}
type deploymentAdmission struct {
	lock         contextMutex
	reservations map[*deploymentReservation]struct{}
	observedAt   time.Time
	processes    []Process
	names        map[string]string
}

var projectDeploymentAdmissions sync.Map

func (b *Backend) deploymentAdmission() *deploymentAdmission {
	key := b.client.base + "\x00" + b.client.project
	state, _ := projectDeploymentAdmissions.LoadOrStore(key, &deploymentAdmission{reservations: make(map[*deploymentReservation]struct{})})
	return state.(*deploymentAdmission)
}

func deploymentProcess(p Process) bool {
	action := strings.ToLower(p.ActionName)
	return strings.Contains(action, "deploy") || strings.Contains(action, "build")
}
func activeDeploymentProcess(p Process) bool {
	switch p.Status {
	case "FINISHED", "FAILED", "CANCELED", "CANCELLED":
		return false
	}
	// Missing actions and unrecognized statuses are incomplete evidence.
	// Only positively unrelated actions or terminal work may be excluded.
	switch p.ActionName {
	case "start", "stop", "restart", "stack.start", "stack.stop", "stack.restart",
		"stack.create", "stack.delete", "stack.updateUserData", "stack.updateAutoscaling", "stack.enableSubdomainAccess":
		return false
	default:
		return true
	}
}

// deploymentObservation reads only explicitly owned services. A shared ten-second
// cache bounds read amplification across namespaces and concurrent submissions.
// Full service histories are bounded; age never makes a running process safe.
func (b *Backend) deploymentObservation(ctx context.Context, state *deploymentAdmission) ([]Process, map[string]string, error) {
	if !state.observedAt.IsZero() && time.Since(state.observedAt) < 10*time.Second {
		return state.processes, state.names, nil
	}
	services, err := b.client.ListServices(ctx)
	if err != nil {
		return nil, nil, err
	}
	names := make(map[string]string)
	var all []Process
	for _, service := range services {
		if service.protected() || service.ProjectID != "" && service.ProjectID != b.client.project {
			continue
		}
		items, err := b.client.Env(ctx, service.ID)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range items {
			if item.Key != "KARAKORAM_OWNER" {
				continue
			}
			if item.Sensitive || item.Content == maskedValue {
				return nil, nil, fmt.Errorf("zerops: service ownership marker unreadable")
			}
			if item.Type == "USER" && item.Content != "" {
				names[service.ID] = service.Name
			}
		}
		if _, owned := names[service.ID]; !owned {
			continue
		}
		processes, err := b.boundedServiceProcesses(ctx, service.ID)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, processes...)
	}
	state.processes, state.names, state.observedAt = all, names, time.Now()
	return all, names, nil
}

func (b *Backend) boundedServiceProcesses(ctx context.Context, service string) ([]Process, error) {
	var all []Process
	for offset, pages := 0, 0; pages < 5; pages++ {
		var p page[Process]
		path := fmt.Sprintf("/service-stack/%s/process?limit=100&offset=%d&sort=created&order=desc", escape(service), offset)
		if err := b.client.json(ctx, "GET", path, nil, &p, false); err != nil {
			return nil, err
		}
		for _, process := range p.List {
			if process.ServiceStackID != "" && process.ServiceStackID != service || process.ProjectID != "" && process.ProjectID != b.client.project {
				return nil, fmt.Errorf("zerops: inconsistent service process identity")
			}
			process.ServiceStackID = service
			all = append(all, process)
		}
		offset += len(p.List)
		if offset >= p.Total {
			return all, nil
		}
		if len(p.List) == 0 {
			return nil, fmt.Errorf("zerops: incomplete service process inventory")
		}
	}
	return nil, fmt.Errorf("zerops: service process inventory exceeds admission observation bound")
}

func deploymentWaiting(reason string, keys []string, detail string, cause error) error {
	sort.Strings(keys)
	return &infra.DeploymentWaitError{Reason: reason, Key: strings.Join(keys, ","), Detail: detail, RetryAt: admissionRetryAt(cause)}
}

// finishDeploymentSubmission starts the visibility window only after the caller
// returns. A live allocation/upload/deploy must retain its slot without a TTL.
func (b *Backend) finishDeploymentSubmission(service string) {
	state := b.deploymentAdmission()
	_ = state.lock.Lock(context.Background())
	defer state.lock.Unlock()
	for pending := range state.reservations {
		if pending.service == service {
			pending.live = false
			pending.finishedAt = time.Now()
		}
	}
}

// admitDeployment reserves capacity while the caller is live, then through
// submission visibility. Definitive failures release immediately.
func (b *Backend) admitDeployment(ctx context.Context, s *ServiceStack) (func(), error) {
	state := b.deploymentAdmission()
	if err := state.lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer state.lock.Unlock()
	var keys []string
	keys = []string{b.client.project + ":EVIDENCE_UNAVAILABLE"}
	waiting := func(detail string, cause error) (func(), error) {
		return nil, deploymentWaiting("DeploymentSlot", keys, detail, cause)
	}
	processes, names, err := b.deploymentObservation(ctx, state)
	if err != nil {
		return waiting("waiting for a platform deployment slot: owned service/process evidence unavailable", err)
	}
	keys = nil
	diagnostics := []string{}
	seen := make(map[string]bool)
	for _, p := range processes {
		name, owned := names[p.ServiceStackID]
		if !owned || p.ProjectID != "" && p.ProjectID != b.client.project || !activeDeploymentProcess(p) || seen[p.ID] {
			continue
		}
		if p.ID == "" {
			keys = append(keys, p.ServiceStackID+":IDENTITY_UNAVAILABLE:"+p.Status)
			return waiting("waiting for a platform deployment slot: process identity unavailable", nil)
		}
		seen[p.ID] = true
		keys = append(keys, p.ServiceStackID+"/"+p.ID+":"+p.Status)
		diagnostics = append(diagnostics, fmt.Sprintf("%s:%s%s", name, p.Status, processAge(p)))
	}
	now := time.Now()
	var reservation *deploymentReservation
	for pending := range state.reservations {
		observed := false
		observedActive := false
		for _, p := range processes {
			at := p.Created
			if at.IsZero() {
				at = p.Started
			}
			_, existed := pending.baseline[p.ID]
			if p.ID != "" && p.ServiceStackID == pending.service && deploymentProcess(p) && (p.ProjectID == "" || p.ProjectID == b.client.project) && (!at.IsZero() && !at.Before(pending.at) || at.IsZero() && !existed) {
				observed = true
				observedActive = observedActive || activeDeploymentProcess(p)
			}
		}
		if !pending.live && (observed || !pending.finishedAt.IsZero() && now.Sub(pending.finishedAt) >= deploymentReservationTTL) {
			delete(state.reservations, pending)
			continue
		}
		if observedActive {
			continue
		}
		if pending.service == s.ID {
			// The service operation lock fences recovery of the same submission.
			// Reuse its reservation instead of consuming another slot.
			reservation = pending
			continue
		}
		keys = append(keys, pending.service+":SUBMITTING")
		diagnostics = append(diagnostics, fmt.Sprintf("%s:SUBMITTING (age %s)", pending.name, now.Sub(pending.at).Round(time.Second)))
	}
	limit := b.maxInflightDeploys
	if limit < 1 {
		limit = 2
	}
	if len(diagnostics) >= limit {
		sort.Strings(diagnostics)
		return waiting(fmt.Sprintf("waiting for a platform deployment slot: %d/%d in flight (%s)", len(diagnostics), limit, strings.Join(diagnostics, ", ")), nil)
	}
	if reservation == nil {
		reservation = &deploymentReservation{service: s.ID, name: s.Name, at: now, live: true, baseline: make(map[string]struct{})}
		for _, p := range processes {
			if p.ServiceStackID == s.ID {
				reservation.baseline[p.ID] = struct{}{}
			}
		}
		state.reservations[reservation] = struct{}{}
	}
	// A refused recovery must leave the returned holder's visibility window
	// intact: no release/completion callback is handed to a waiting caller.
	reservation.live = true
	reservation.finishedAt = time.Time{}
	return func() {
		// Releasing must work even when the submission context was canceled.
		_ = state.lock.Lock(context.Background())
		delete(state.reservations, reservation)
		state.lock.Unlock()
	}, nil
}

// waitPreviousDeployment refuses allocation/upload on missing or unknown
// evidence. selectedVersion permits resuming the current upload, not bypassing
// an active build/deployment of that same version.
func (b *Backend) waitPreviousDeployment(ctx context.Context, s *ServiceStack, selectedVersion string) error {
	var keys []string
	waiting := func(detail string, cause error) error {
		if len(keys) == 0 {
			keys = []string{s.ID + ":EVIDENCE_UNAVAILABLE"}
		}
		return deploymentWaiting("PreviousDeployment", keys, "waiting for previous platform deployment to finish: "+detail, cause)
	}
	state := b.deploymentAdmission()
	if err := state.lock.Lock(ctx); err != nil {
		return err
	}
	var processes []Process
	var err error
	if _, owned := state.names[s.ID]; owned && !state.observedAt.IsZero() && time.Since(state.observedAt) < 10*time.Second {
		for _, p := range state.processes {
			if p.ServiceStackID == s.ID {
				processes = append(processes, p)
			}
		}
	} else {
		processes, err = b.boundedServiceProcesses(ctx, s.ID)
	}
	state.lock.Unlock()
	if err != nil {
		return waiting("process evidence unavailable", err)
	}
	var diagnostics []string
	for _, p := range processes {
		if p.ServiceStackID != "" && p.ServiceStackID != s.ID || p.ProjectID != "" && p.ProjectID != b.client.project {
			return waiting("inconsistent process identity", nil)
		}
		if !activeDeploymentProcess(p) {
			continue
		}
		keys = append(keys, s.ID+"/"+p.ID+":"+p.Status)
		diagnostics = append(diagnostics, fmt.Sprintf("process %s %s%s", p.ID, p.Status, processAge(p)))
	}
	if len(diagnostics) != 0 {
		sort.Strings(diagnostics)
		return waiting(strings.Join(diagnostics, ", "), nil)
	}
	versions, err := b.allocationVersions(ctx, s.ID)
	if err != nil {
		return waiting("app-version evidence unavailable", err)
	}
	for _, version := range versions {
		switch version.Status {
		case "ACTIVE", "BACKUP", "DEPLOYED", "FAILED", "BUILD_FAILED", "DEPLOY_FAILED", "PREPARING_RUNTIME_FAILED", "BUILD_VALIDATION_FAILED", "CANCELLED", "CANCELED", "DELETED", "STOPPED":
		case "UPLOADING", "WAITING_TO_DEPLOY":
			if version.ID != selectedVersion {
				keys = append(keys, s.ID+"/"+version.ID+":"+version.Status)
				diagnostics = append(diagnostics, fmt.Sprintf("app version %s %s", version.ID, version.Status))
			}
		default:
			keys = append(keys, s.ID+"/"+version.ID+":"+version.Status)
			diagnostics = append(diagnostics, fmt.Sprintf("app version %s %s", version.ID, version.Status))
		}
	}
	if len(diagnostics) != 0 {
		sort.Strings(diagnostics)
		return waiting(strings.Join(diagnostics, ", "), nil)
	}
	return nil
}
