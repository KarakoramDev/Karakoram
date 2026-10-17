// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const opPrefix = "nomad:v1:"
const startupWindow = 15 * time.Minute
const operationLeaseDuration = 120 * time.Second
const operationRequestTimeout = 10 * time.Second

type operationLease struct {
	Owner string    `json:"owner"`
	Until time.Time `json:"until"`
}

type operationAttempt struct{ uncertainMutation bool }
type operationAttemptKey struct{}

func operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	work, cancel := context.WithTimeout(ctx, operationRequestTimeout)
	return context.WithValue(work, operationAttemptKey{}, &operationAttempt{}), cancel
}

// Only an unacknowledged write needs delivery quarantine. In particular, a
// failed observation after acknowledged writes cannot still change Nomad.
func operationMutation(ctx context.Context, request func() error) error {
	attempt, _ := ctx.Value(operationAttemptKey{}).(*operationAttempt)
	if attempt == nil {
		return request()
	}
	previous := attempt.uncertainMutation
	attempt.uncertainMutation = true
	err := request()
	var apiErr *APIError
	acknowledged := err == nil || errors.Is(err, infra.ErrConflict) ||
		errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500
	attempt.uncertainMutation = previous || !acknowledged
	return err
}

type operation struct {
	Action         string            `json:"action"`
	Hostname       string            `json:"hostname,omitempty"`
	RecordID       string            `json:"recordId,omitempty"`
	JobID          string            `json:"jobId,omitempty"`
	EvalID         string            `json:"evalId,omitempty"`
	Phase          string            `json:"phase,omitempty"`
	Issued         time.Time         `json:"issued"`
	Started        time.Time         `json:"started"`
	Services       map[string]string `json:"services,omitempty"`
	JobVersion     uint64            `json:"jobVersion,omitempty"`
	JobModifyIndex uint64            `json:"jobModifyIndex,omitempty"`
	Message        string            `json:"message,omitempty"`
	Lease          *operationLease   `json:"lease,omitempty"`
}

func encodeOp(o operation) infra.Op {
	if o.Issued.IsZero() {
		o.Issued = time.Now().UTC()
	}
	// Handles identify the operation, not a particular poll's lease.
	o.Lease = nil
	data, _ := json.Marshal(o)
	return infra.Op{ID: opPrefix + base64.RawURLEncoding.EncodeToString(data), Kind: o.Action, Hostname: o.Hostname, ServiceID: o.RecordID, Services: o.Services}
}
func decodeOp(op infra.Op) (operation, error) {
	var o operation
	if !strings.HasPrefix(op.ID, opPrefix) || len(op.ID) > 65536 {
		return o, reject("invalid operation handle")
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(op.ID, opPrefix))
	if err != nil {
		return o, reject("invalid operation handle")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&o) != nil {
		return o, reject("invalid operation handle")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return o, reject("invalid operation handle")
	}
	switch o.Action {
	case "import", "deploy", "start", "restart", "stop", "delete", "resources", "scale", "ports":
	default:
		return o, reject("invalid operation action")
	}
	if o.Issued.IsZero() {
		return o, reject("operation issue time is required")
	}
	if o.Action == "import" {
		for hostname, id := range o.Services {
			if !safeHostname(hostname) || id == "" {
				return o, reject("invalid import operation")
			}
		}
	} else if !safeHostname(o.Hostname) || o.RecordID == "" || o.JobID != o.Hostname {
		return o, reject("invalid operation target")
	}
	switch o.Phase {
	case "", "stopping", "registering", "starting", "done", "failed":
	default:
		return o, reject("invalid operation phase")
	}
	return o, nil
}
func (b *Backend) begin(ctx context.Context, r *record, action string) (infra.Op, error) {
	work, cancel := operationContext(ctx)
	defer cancel()
	if b.operationLeaseBlocked(r) {
		return infra.Op{}, infra.ErrConflict
	}
	job, observed, err := b.operationJob(work, r)
	if err != nil && !errors.Is(err, infra.ErrNotFound) {
		return infra.Op{}, err
	}
	lease, err := b.newOperationLease()
	if err != nil {
		return infra.Op{}, err
	}
	o := operation{Action: action, Hostname: r.Hostname, RecordID: r.ID, JobID: r.Hostname, Issued: time.Now().UTC(), Phase: "stopping", Lease: lease}
	// Claim exclusion and persist the pending specification before stopping.
	r.Pending = &o
	if err = b.save(work, r); err != nil {
		return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
	}
	if job != nil {
		if err = b.stopOperationJob(work, r, job, observed); err != nil {
			return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
		}
	}
	if err = b.releaseOperationLease(work, r); err != nil {
		return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
	}
	return encodeOp(o), nil
}

// A failed/abandoned poll may still have a request in delivery. Even the same
// instance waits until Until plus the delivery margin before retrying. A fully
// acknowledged poll releases its lease immediately instead.
func (b *Backend) operationLeaseBlocked(r *record) bool {
	return r.RestartLease != nil && b.now().Before(r.RestartLease.Until.Add(restoreFence)) ||
		r.Pending != nil && r.Pending.Lease != nil && b.now().Before(r.Pending.Lease.Until.Add(restoreFence))
}

func (b *Backend) newOperationLease() (*operationLease, error) {
	if b.instance == "" {
		id, err := newID()
		if err != nil {
			return nil, err
		}
		b.instance = id
	}
	return &operationLease{Owner: b.instance, Until: b.now().UTC().Add(operationLeaseDuration)}, nil
}

func (b *Backend) operationLeaseUsable(r *record) bool {
	return b.leaseUsable(r.Pending.Lease)
}

func (b *Backend) leaseUsable(lease *operationLease) bool {
	return lease != nil && lease.Owner == b.instance && !b.now().Add(operationRequestTimeout).After(lease.Until)
}

func (b *Backend) releaseOperationLease(ctx context.Context, r *record) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.Pending.Lease = nil
	return b.save(ctx, r)
}

// Read-only failures and definitive mutation rejections have no delivery debt.
func (b *Backend) releaseFailedOperation(ctx context.Context, attempted *record, ours *operationLease, failure error) error {
	if attempt, _ := ctx.Value(operationAttemptKey{}).(*operationAttempt); attempt != nil && attempt.uncertainMutation {
		return failure
	}
	return errors.Join(failure, b.releaseExactOperationLease(ctx, attempted, ours))
}

// Release only the exact acquired claim from fresh durable state, never the
// failed attempt's unsaved phase/spec. Direct restarts keep Pending untouched.
func (b *Backend) releaseExactOperationLease(ctx context.Context, attempted *record, ours *operationLease) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationRequestTimeout)
	defer cancel()
	current, err := b.load(cleanup, attempted.Hostname)
	if err != nil {
		return err
	}
	if current == nil || current.ID != attempted.ID {
		return nil
	}
	restart := attempted.RestartLease != nil && attempted.RestartLease.Owner == ours.Owner && attempted.RestartLease.Until.Equal(ours.Until)
	var lease *operationLease
	if restart {
		lease = current.RestartLease
	} else {
		if current.Pending == nil || attempted.Pending == nil || !current.Pending.Issued.Equal(attempted.Pending.Issued) {
			return nil
		}
		lease = current.Pending.Lease
	}
	if lease == nil || lease.Owner != b.instance || lease.Owner != ours.Owner || !lease.Until.Equal(ours.Until) {
		return nil
	}
	if restart {
		current.RestartLease = nil
	} else {
		current.Pending.Lease = nil
	}
	return b.save(cleanup, current)
}

// Preserve the exact server jobspec (including integer indices) for an atomic
// stop update; ownership is checked before either issuance or polling can stop.
func (b *Backend) operationJob(ctx context.Context, r *record) (map[string]json.RawMessage, observedJob, error) {
	var raw json.RawMessage
	var observed observedJob
	if _, err := b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &raw); err != nil {
		return nil, observed, err
	}
	if err := json.Unmarshal(raw, &observed); err != nil {
		return nil, observed, silentFailure("cannot decode job ownership")
	}
	if !b.ownsJob(r, observed) {
		return nil, observed, jobOwnershipConflict()
	}
	var job map[string]json.RawMessage
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, observed, silentFailure("cannot decode job specification")
	}
	return job, observed, nil
}

func (b *Backend) stopOperationJob(ctx context.Context, r *record, job map[string]json.RawMessage, observed observedJob) error {
	if !b.operationLeaseUsable(r) {
		return infra.ErrConflict
	}
	job["Stop"] = json.RawMessage("true")
	err := operationMutation(ctx, func() error {
		return b.c.post(ctx, "/v1/jobs", nil, map[string]any{
			"Job": job, "EnforceIndex": true, "JobModifyIndex": observed.JobModifyIndex,
		}, nil)
	})
	if errors.Is(err, infra.ErrConflict) {
		return nil // The exact observed job changed; re-observe on the next poll.
	}
	return err
}
func expired(o operation) infra.OpState {
	return infra.OpState{Done: true, Failed: o.Action != "import", Expired: true, Status: "expired", Message: "operation outcome is no longer known"}
}
func operationIDs(o operation) []string {
	ids := make([]string, 0, len(o.Services)+1)
	if o.RecordID != "" {
		ids = append(ids, o.RecordID)
	}
	for _, id := range o.Services {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func (b *Backend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opStatus(ctx, op)
}
func (b *Backend) opStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "" {
		return infra.OpState{Done: true, Status: "done"}, nil
	}
	o, err := decodeOp(op)
	if err != nil {
		return infra.OpState{}, err
	}
	state := infra.OpState{Status: o.Phase, ServiceIDs: operationIDs(o)}
	if o.Action == "import" {
		for hostname, id := range o.Services {
			r, e := b.load(ctx, hostname)
			if e != nil {
				return state, e
			}
			if r == nil || r.ID != id {
				return expired(o), nil
			}
			if r.Pending != nil && r.Pending.Action == "ports" {
				if _, e = b.opStatus(ctx, encodeOp(*r.Pending)); e != nil {
					return state, e
				}
			}
			if r.Kind == infra.KindVolume {
				var volume struct{ State string }
				if _, e = b.c.get(ctx, "/v1/volume/host/"+r.VolumeID, nil, &volume); e != nil {
					if errors.Is(e, infra.ErrNotFound) {
						return expired(o), nil
					}
					return state, e
				}
				if volume.State != "ready" {
					if time.Since(o.Issued) > startupWindow {
						state.Failed = true
						state.Status = "failed"
						state.Message = "volume creation exceeded the 15 minute startup window"
					}
					return state, nil
				}
			}
		}
		state.Done = true
		state.Status = "done"
		return state, nil
	}
	r, err := b.load(ctx, o.Hostname)
	if err != nil {
		return state, err
	}
	if r == nil {
		if o.Action == "delete" {
			state.Done = true
			state.Status = "done"
			return state, nil
		}
		return expired(o), nil
	}
	if r.ID != o.RecordID {
		return expired(o), nil
	}
	if r.Pending == nil || !r.Pending.Issued.Equal(o.Issued) {
		if r.Pending != nil && r.Pending.Action == "ports" {
			// Port changes have no caller-visible handle. Polling an older
			// operation for this service must still advance their restart.
			if _, err = b.opStatus(ctx, encodeOp(*r.Pending)); err != nil {
				return state, err
			}
		}
		return expired(o), nil
	}
	current := r.Pending
	state.Status = current.Phase
	if current.Phase == "done" {
		state.Done = true
		return state, nil
	}
	if current.Phase == "failed" {
		state.Failed = true
		state.Message = current.Message
		return state, nil
	}
	if b.operationLeaseBlocked(r) {
		state.Message = "waiting for lifecycle operation lease"
		return state, nil
	}
	work, cancel := operationContext(ctx)
	defer cancel()
	lease, err := b.newOperationLease()
	if err != nil {
		return state, err
	}
	current.Lease = lease
	if err = b.save(work, r); err != nil {
		if errors.Is(err, infra.ErrConflict) {
			return state, nil // Another process acquired or advanced the operation.
		}
		return state, b.releaseFailedOperation(work, r, lease, err)
	}
	state, err = b.advanceOperation(work, r, o, state)
	if err == nil && !(o.Action == "delete" && state.Done) {
		err = b.releaseOperationLease(work, r)
	}
	if err != nil {
		err = b.releaseFailedOperation(work, r, lease, err)
	}
	return state, err
}

func (b *Backend) advanceOperation(ctx context.Context, r *record, o operation, state infra.OpState) (infra.OpState, error) {
	current := r.Pending
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return state, err
	}
	if current.Phase == "stopping" {
		// The CAS-saved restoration claim survives process reconstruction and
		// every superseding save. Absence is not final while its POST can land.
		if b.restorationFenced(r) {
			state.Message = "waiting for restoration registration fence"
			return state, nil
		}
		job, observed, jobErr := b.operationJob(ctx, r)
		if jobErr != nil && !errors.Is(jobErr, infra.ErrNotFound) {
			return state, jobErr
		}
		if jobErr == nil {
			if observed.Meta["karakoram.operation"] == current.Issued.Format(time.RFC3339Nano) {
				// A stale stopping poll must never revoke its own successor,
				// even when a previous poll's phase-save response was lost.
				latest, err := b.load(ctx, r.Hostname)
				if latest != nil && latest.Pending != nil {
					state.Status = latest.Pending.Phase
				}
				return state, err
			}
			if !observed.Stop {
				state.Message = "waiting for previous job to stop"
				return state, b.stopOperationJob(ctx, r, job, observed)
			}
		}
		for _, a := range as {
			if !allocationTerminal(a) {
				state.Message = "waiting for previous allocations to stop"
				return state, nil
			}
		}
		if !b.operationLeaseUsable(r) {
			return state, nil
		}
		if job != nil {
			// Purge is safe only while registration is excluded by our lease,
			// after an observed stop and terminal allocations.
			if err = operationMutation(ctx, func() error {
				return ignoreNotFound(b.c.delete(ctx, "/v1/job/"+r.Hostname, query("purge", "true"), nil))
			}); err != nil {
				return state, err
			}
		}
		if o.Action == "delete" {
			deleted, err := b.finishDelete(ctx, r)
			if err != nil || !deleted {
				return state, err
			}
			state.Done = true
			state.Status = "done"
			return state, nil
		}
		if o.Action == "stop" {
			current.Phase = "done"
			state.Done = true
			state.Status = "done"
			return state, b.save(ctx, r)
		}
		current.Phase = "registering"
		state.Status = current.Phase
		return state, b.save(ctx, r)
	}
	if current.Phase == "registering" {
		var registered struct {
			Version, JobModifyIndex uint64
			Meta                    map[string]string
		}
		_, err = b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &registered)
		if err != nil && !errors.Is(err, infra.ErrNotFound) {
			return state, err
		}
		if err == nil {
			if registered.Meta["karakoram.operation"] != current.Issued.Format(time.RFC3339Nano) {
				return state, infra.ErrConflict
			}
			var evaluations []struct {
				ID             string
				JobModifyIndex uint64
			}
			if _, err = b.c.get(ctx, "/v1/job/"+r.Hostname+"/evaluations", nil, &evaluations); err != nil {
				return state, err
			}
			for _, evaluation := range evaluations {
				if evaluation.JobModifyIndex == registered.JobModifyIndex {
					current.EvalID = evaluation.ID
					break
				}
			}
			if current.EvalID == "" {
				return expired(o), nil
			}
		} else {
			job, e := b.renderJob(ctx, r)
			if e != nil {
				return state, e
			}
			// A second poller cannot replace the job registered by the first.
			job["EnforceIndex"] = true
			job["JobModifyIndex"] = uint64(0)
			var response struct {
				EvalID         string
				JobModifyIndex uint64
			}
			if !b.operationLeaseUsable(r) {
				return state, nil
			}
			if err = operationMutation(ctx, func() error {
				return b.c.post(ctx, "/v1/jobs", nil, job, &response)
			}); err != nil {
				return state, err
			}
			if response.EvalID == "" {
				return state, silentFailure("job registration omitted evaluation identity")
			}
			if _, err = b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &registered); err != nil {
				return state, err
			}
			current.EvalID = response.EvalID
		}
		current.Phase = "starting"
		current.Started = time.Now().UTC()
		current.JobVersion = registered.Version
		current.JobModifyIndex = registered.JobModifyIndex
		state.Status = current.Phase
		return state, b.save(ctx, r)
	}
	// Shutdown can legitimately outlast this window on a disconnected node.
	// Only a registered replacement's startup is bounded.
	if !current.Started.IsZero() && time.Since(current.Started) > startupWindow {
		return b.failOperation(ctx, r, state, "runtime did not start within the 15 minute startup window")
	}
	var eval struct {
		Status, StatusDescription, BlockedEval string
		FailedTGAllocs                         map[string]struct {
			NodesEvaluated, NodesFiltered, NodesExhausted         int
			ConstraintFiltered, ClassFiltered, DimensionExhausted map[string]int
		}
	}
	_, err = b.c.get(ctx, "/v1/evaluation/"+current.EvalID, nil, &eval)
	if err != nil && !errors.Is(err, infra.ErrNotFound) {
		return state, err
	}
	running := 0
	failed := ""
	for _, a := range as {
		if a.JobVersion != current.JobVersion || a.CreateIndex <= current.JobModifyIndex {
			continue
		}
		if a.ClientStatus == "running" {
			running++
		} else if a.ClientStatus == "failed" || a.ClientStatus == "lost" {
			failed = a.ClientStatus
		}
	}
	// The original evaluation can retain historical placement failures even
	// after its blocked evaluation has successfully placed the current job.
	if running >= r.Max {
		current.Phase = "done"
		state.Done = true
		state.Status = "done"
		return state, b.save(ctx, r)
	}
	missing := errors.Is(err, infra.ErrNotFound)
	if !missing && (eval.Status == "failed" || eval.Status == "canceled" || eval.Status == "cancelled" || eval.Status == "blocked" || len(eval.FailedTGAllocs) > 0 || eval.BlockedEval != "") {
		groups := make([]string, 0, len(eval.FailedTGAllocs))
		for group, f := range eval.FailedTGAllocs {
			groups = append(groups, fmt.Sprintf("%s: evaluated=%d filtered=%d exhausted=%d", group, f.NodesEvaluated, f.NodesFiltered, f.NodesExhausted))
		}
		sort.Strings(groups)
		message := "Nomad evaluation " + eval.Status
		if len(groups) > 0 {
			message += "; FailedTGAllocs: " + strings.Join(groups, "; ")
		}
		placementBlocked := eval.Status == "blocked" || len(eval.FailedTGAllocs) > 0 || eval.BlockedEval != ""
		if placementBlocked {
			message += "; placement blocked"
			state.Message = message
			// Let Nomad's blocked evaluation wait for node/capacity recovery.
			// The normal startup deadline still bounds this operation.
			if current.Message != message {
				current.Message = message
				return state, b.save(ctx, r)
			}
			return state, nil
		}
		return b.failOperation(ctx, r, state, message)
	}
	if failed != "" {
		return b.failOperation(ctx, r, state, "allocation "+failed)
	}
	if missing {
		return expired(o), nil
	}
	return state, nil
}
func (b *Backend) failOperation(ctx context.Context, r *record, state infra.OpState, message string) (infra.OpState, error) {
	// Operation failure is not desired-state cancellation. Keep the job and
	// its blocked evaluations/rescheduling active for eventual recovery.
	r.Pending.Phase = "failed"
	r.Pending.Message = message
	state.Failed = true
	state.Status = "failed"
	state.Message = message
	return state, b.save(ctx, r)
}
