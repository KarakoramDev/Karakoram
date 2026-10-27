// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"reflect"
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
	Action           string            `json:"action"`
	Hostname         string            `json:"hostname,omitempty"`
	RecordID         string            `json:"recordId,omitempty"`
	JobID            string            `json:"jobId,omitempty"`
	EvalID           string            `json:"evalId,omitempty"`
	Phase            string            `json:"phase,omitempty"`
	Issued           time.Time         `json:"issued"`
	Started          time.Time         `json:"started"`
	Services         map[string]string `json:"services,omitempty"`
	JobVersion       uint64            `json:"jobVersion,omitempty"`
	JobModifyIndex   uint64            `json:"jobModifyIndex,omitempty"`
	Message          string            `json:"message,omitempty"`
	RetainOnly       bool              `json:"retainOnly,omitempty"`
	UnplacedPrevious *infra.Resources  `json:"unplacedPrevious,omitempty"`
	RetainedHash     string            `json:"retainedHash,omitempty"`
	Lease            *operationLease   `json:"lease,omitempty"`

	ResourceCorrection *resourceCorrection `json:"resourceCorrection,omitempty"`
	ResourceRejected   bool                `json:"resourceRejected,omitempty"`
}

// Conditional resource corrections retain their exact predecessor and accepted
// target so a lost provider checkpoint recovers the receipt, not another rollout.
type resourceCorrection struct {
	Predecessor time.Time       `json:"predecessor"`
	Resources   infra.Resources `json:"resources"`
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
	return b.beginOperation(ctx, r, action, false, nil)
}

func (b *Backend) beginOperation(ctx context.Context, r *record, action string, retainOnly bool, unplacedPrevious *infra.Resources) (infra.Op, error) {
	if r.Importing {
		return infra.Op{}, &APIError{Status: 409, Op: "lifecycle", Message: "import is incomplete; retry the original import before changing this runtime"}
	}
	if r.Volume != "" && action != "stop" && action != "delete" {
		ok, err := b.restorableVolume(ctx, r)
		if err != nil {
			return infra.Op{}, err
		}
		if !ok {
			return infra.Op{}, &APIError{Status: 409, Op: "lifecycle", Message: "original host volume is missing, changed or not ready; restore original storage or explicitly replace the member; refusing runtime mutation"}
		}
	}
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
	o := operation{Action: action, Hostname: r.Hostname, RecordID: r.ID, JobID: r.Hostname, Issued: time.Now().UTC(), Phase: "stopping", Lease: lease, RetainOnly: retainOnly, UnplacedPrevious: unplacedPrevious}
	if unplacedPrevious != nil && r.Pending != nil {
		o.ResourceCorrection = &resourceCorrection{Predecessor: r.Pending.Issued, Resources: r.Resources}
	}
	// Stateless changes need no single-writer shutdown fence. Register the
	// new version over the owned job and let its serving-gated canary update
	// retain the old allocations. Explicit lifecycle actions, singleton relays
	// and volume members still fence; only application specification updates roll.
	relay := r.Deployment != nil && len(r.Deployment.Runtime.Args) != 0 && r.Deployment.Runtime.Args[0] == "relay"
	if r.Volume == "" && !relay && (action == "deploy" || action == "resources" || action == "scale" || action == "ports") {
		o.Phase = "registering"
		o.UnplacedPrevious = nil
	}
	// Plan before saving the new resources or issuing the single-writer stop.
	// An infeasible replacement must leave both the serving job and its durable
	// specification untouched, including when convergence retries this request.
	// Conditional initial-deployment corrections have no serving allocation
	// to protect; their separate admission/history fence must remain usable
	// while placement is blocked (including on an ineligible node).
	if action == "resources" && o.Phase == "stopping" && unplacedPrevious == nil {
		proposed := *r
		proposed.Pending = &o
		if err := b.planResourceUpdate(work, &proposed); err != nil {
			return infra.Op{}, err
		}
	}
	// Claim exclusion and persist the pending specification before mutation.
	r.Pending = &o
	if err = b.save(work, r); err != nil {
		return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
	}
	if job != nil && o.Phase == "stopping" {
		if err = b.stopOperationJob(work, r, job, observed); err != nil {
			return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
		}
		if unplacedPrevious != nil && r.Volume != "" {
			if err = b.checkUnplacedResize(work, r); err != nil {
				return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
			}
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
	ctx = infra.WithoutObservation(ctx)
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
			if r.Importing {
				state.Message = "import is incomplete; retry the original import with the same specification to resume its reserved identities"
				return state, nil
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
	if r.Pending != nil && r.Pending.Phase == "failed" && r.Pending.UnplacedPrevious != nil {
		state.Failed, state.Status, state.Message = true, "DeploymentRetriesExhausted", r.Pending.Message
		return state, unplacedResizeFailure(r)
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
		if current.ResourceRejected {
			return state, &infra.ResourceUpdateRejectedError{Message: current.Message}
		}
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
		if current.UnplacedPrevious != nil && r.Volume != "" {
			if err := b.checkUnplacedResize(ctx, r); err != nil {
				return state, err
			}
		}
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
				// A lost/failed initial stop may be retried after capacity
				// changed. Replan before that retry, not after stopping.
				if current.Action == "resources" && current.UnplacedPrevious == nil {
					if err := b.planResourceUpdate(ctx, r); err != nil {
						if errors.Is(err, infra.ErrRejected) {
							var rejected *infra.ResourceUpdateRejectedError
							current.ResourceRejected = errors.As(err, &rejected)
							failed, saveErr := b.failOperation(ctx, r, state, err.Error())
							return failed, errors.Join(err, saveErr)
						}
						return state, err
					}
				}
				state.Message = "waiting for previous job to stop"
				return state, b.stopOperationJob(ctx, r, job, observed)
			}
		}
		for _, a := range as {
			if !allocationTerminal(a) {
				state.Message = "waiting for previous allocations to stop"
				failures, err := b.stalledStops(ctx, r, as)
				if err != nil {
					return state, err
				}
				if len(failures) > 0 {
					state.Status = "BackendStopStalled"
					messages := make([]string, len(failures))
					for i, failure := range failures {
						messages[i] = failure.Message
					}
					state.Message = strings.Join(messages, "; ")
				}
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
		current.UnplacedPrevious = nil
		state.Status = current.Phase
		return state, b.save(ctx, r)
	}
	if current.Phase == "registering" {
		existing, observed, jobErr := b.operationJob(ctx, r)
		if jobErr != nil && !errors.Is(jobErr, infra.ErrNotFound) {
			if (current.RetainOnly || current.RetainedHash != "") && errors.Is(jobErr, infra.ErrConflict) {
				return b.failOperation(ctx, r, state, "count-only recovery lost job ownership before registration; refusing to mutate the foreign runtime")
			}
			return state, jobErr
		}
		registered := observed
		existingHash := retainedJobHash(existing)
		matches := jobErr == nil && observed.Meta["karakoram.operation"] == current.Issued.Format(time.RFC3339Nano) &&
			(current.JobModifyIndex == 0 || observed.JobModifyIndex != current.JobModifyIndex)
		if current.RetainedHash != "" {
			matches = jobErr == nil && !observed.Stop && observed.Version == current.JobVersion &&
				observed.JobModifyIndex >= current.JobModifyIndex && retainedJobHash(existing) == current.RetainedHash
			if !matches && (jobErr != nil || observed.JobModifyIndex != current.JobModifyIndex) {
				return b.failOperation(ctx, r, state, "count-only registration was superseded or automatically reverted")
			}
		}
		if jobErr == nil && !matches && r.Volume != "" {
			return state, infra.ErrConflict
		}
		if matches {
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
			retaining := false
			if current.Action == "scale" && r.Volume == "" && jobErr == nil && !observed.Stop {
				// A pure count change must not replace the serving tasks or
				// require start-first canary capacity.
				if retained, e := b.retainedScaleJob(ctx, r, existing, false, current.RetainOnly); e != nil {
					return state, e
				} else if retained != nil {
					job["Job"] = retained
					retaining = true
				}
			}
			if retaining {
				retained := job["Job"].(map[string]json.RawMessage)
				hash := retainedJobHash(retained)
				if current.RetainedHash != "" && hash != current.RetainedHash {
					return b.failOperation(ctx, r, state, "count-only registration content changed before acknowledgement")
				}
				// Job/group/task Meta is part of Nomad's task specification.
				// Keep the serving Meta unchanged; a fresh operation marker
				// would restart every retained allocation. Before POST, persist
				// the exact normalized content digest, expected next version and
				// CAS base index. A lost response may adopt only that next version
				// with identical content, never a later revert/superseding write.
				// Once acknowledged, version AND JobModifyIndex are the receipt.
				current.RetainedHash = hash
				current.JobVersion = observed.Version + 1
				current.JobModifyIndex = observed.JobModifyIndex
				if hash == existingHash {
					// Nomad does not increment the version for an identical
					// registration. Adopt the exact existing version/index
					// instead of issuing an unnecessary, unidentifiable POST.
					current.JobVersion = observed.Version
					return state, b.save(ctx, r)
				}
				if err = b.save(ctx, r); err != nil {
					return state, err
				}
			}
			if (current.RetainOnly || current.RetainedHash != "") && !retaining {
				return b.failOperation(ctx, r, state, "count-only recovery no longer has enough healthy allocations of the exact desired specification; inspect placement and retry after restoring capacity or health")
			}
			// Replace only the exact owned job observed above. A stale poller
			// cannot overwrite a successor or a foreign registration.
			job["EnforceIndex"] = true
			job["JobModifyIndex"] = observed.JobModifyIndex
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
			acknowledged, checked, checkErr := b.operationJob(ctx, r)
			if checkErr != nil {
				if retaining && errors.Is(checkErr, infra.ErrConflict) {
					return b.failOperation(ctx, r, state, "count-only recovery lost job ownership before acknowledgement")
				}
				return state, checkErr
			}
			registered = checked
			if retaining {
				if registered.Stop || registered.Version != current.JobVersion ||
					registered.JobModifyIndex != response.JobModifyIndex || retainedJobHash(acknowledged) != current.RetainedHash {
					return b.failOperation(ctx, r, state, "count-only registration was superseded or automatically reverted before acknowledgement")
				}
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
	// A scheduler rejection is not a failed deployment attempt. Keep the same
	// registration/evaluation alive and give the runtime a fresh startup window
	// once placement becomes possible, rather than spending deploy retries.
	eval, err := b.currentEvaluation(ctx, r, current.JobModifyIndex)
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
	if r.Volume != "" && running >= r.Max {
		current.Phase = "done"
		state.Done = true
		state.Status = "done"
		return state, b.save(ctx, r)
	}
	// Placement rejection cannot revive a rollout that Nomad abandoned.
	// Check exact job identity and native terminal deployment outcomes before
	// suspending only the startup deadline for genuinely pending scheduling.
	statelessState := state
	if r.Volume == "" {
		observed, observeErr := b.statelessOperationStatus(ctx, r, state, as, eval.Status, err)
		if observeErr != nil || observed.Done || observed.Failed {
			return observed, observeErr
		}
		statelessState = observed
	}
	if err == nil && (eval.Status == "failed" || eval.Status == "canceled" || eval.Status == "cancelled") {
		return b.failOperation(ctx, r, state, "Nomad evaluation "+eval.Status)
	}
	if message := b.placementMessage(r, eval); err == nil && message != "" && !currentAllocationsPlaced(as, current.JobVersion, current.JobModifyIndex, r.Max) {
		state.Status = "BackendPlacementFailed"
		state.Message = message
		if r.Volume == "" {
			state.Message += "; provide spare CPU, memory and a free published host-port slot for the canary"
		}
		current.Message = state.Message
		current.Started = b.now().UTC()
		return state, nil
	}
	if current.Message != "" {
		current.Message = ""
		current.Started = b.now().UTC()
	}
	// Shutdown can legitimately outlast this window on a disconnected node.
	// Only a registered replacement's unblocked startup is bounded.
	if !current.Started.IsZero() && b.now().Sub(current.Started) > startupWindow {
		if r.Volume == "" {
			return b.failOperation(ctx, r, state, "stateless runtime did not start within the 15 minute startup window; inspect Nomad deployment health and placement, provide spare CPU, memory and published host-port capacity, then retry")
		}
		return b.failOperation(ctx, r, state, "runtime did not start within the 15 minute startup window")
	}
	if r.Volume == "" {
		return statelessState, nil
	}
	missing := errors.Is(err, infra.ErrNotFound)
	if failed != "" {
		return b.failOperation(ctx, r, state, "allocation "+failed)
	}
	if missing {
		return expired(o), nil
	}
	return state, nil
}

// Stateless count-only updates can retain allocations from older job versions.
// A deployment, when present for this version, is the health authority; a
// running task alone must not prematurely complete a canary rollout.
func (b *Backend) statelessOperationStatus(ctx context.Context, r *record, state infra.OpState, as []allocation, evalStatus string, evalErr error) (infra.OpState, error) {
	current := r.Pending
	raw, job, err := b.operationJob(ctx, r)
	if err != nil {
		if errors.Is(err, infra.ErrNotFound) {
			// The durable service still exists, but this rollout no longer has
			// a job to observe. Match stateful polling's expired receipt so
			// callers can reconcile or delete the owned logical service.
			return expired(*current), nil
		}
		if (current.RetainOnly || current.RetainedHash != "") && errors.Is(err, infra.ErrConflict) {
			return b.failOperation(ctx, r, state, "count-only recovery lost job ownership; refusing to adopt the foreign runtime")
		}
		return state, err
	}
	if !operationMatchesJob(current, job) {
		return b.failOperation(ctx, r, state, "stateless job was superseded or automatically reverted; inspect the failed Nomad deployment and correct resources, host-port capacity or application readiness before retrying")
	}
	var deployment *struct {
		JobVersion uint64
		Status     string
	}
	_, err = b.c.get(ctx, "/v1/job/"+r.Hostname+"/deployment", nil, &deployment)
	if err != nil && !errors.Is(err, infra.ErrNotFound) {
		return state, err
	}
	if deployment != nil && deployment.JobVersion == current.JobVersion {
		switch deployment.Status {
		case "failed", "cancelled", "canceled":
			return b.failOperation(ctx, r, state, "stateless allocation deployment "+deployment.Status+"; previous stable job may have been restored; inspect Nomad deployment and correct resources, host-port capacity or application readiness before retrying")
		case "successful":
			// Also wait for scale-down's surplus allocations to terminate.
		default:
			if current.Action == "scale" {
				retained, err := b.retainedScaleJob(ctx, r, raw, true, current.RetainOnly)
				if err != nil {
					return state, err
				}
				if retained != nil {
					// Persist the retry boundary before preparing the exact
					// count-only registration receipt.
					current.Phase = "registering"
					return state, b.save(ctx, r)
				}
			}
			if state.Message == "" {
				state.Message = "waiting for stateless deployment health; ensure spare CPU, memory and a free published host-port slot for the canary"
			}
			return state, nil
		}
	}
	running, active, healthy := 0, 0, 0
	for _, a := range as {
		if allocationTerminal(a) {
			continue
		}
		active++
		if a.ClientStatus == "running" {
			running++
		}
		if a.ClientStatus == "running" && a.DesiredStatus == "run" && a.DeploymentStatus != nil &&
			a.DeploymentStatus.Healthy != nil && *a.DeploymentStatus.Healthy {
			healthy++
		}
	}
	if evalErr == nil && evalStatus == "complete" && running == r.Max && active == r.Max &&
		(current.Action != "scale" || healthy == r.Max) {
		current.Phase = "done"
		state.Done = true
		state.Status = "done"
		return state, b.save(ctx, r)
	}
	if evalErr != nil && !errors.Is(evalErr, infra.ErrNotFound) {
		return state, evalErr
	}
	if evalStatus == "failed" || evalStatus == "canceled" || evalStatus == "cancelled" {
		return b.failOperation(ctx, r, state, "stateless evaluation "+evalStatus)
	}
	if state.Message == "" {
		state.Message = "waiting for stateless allocations; check Nomad placement, spare resources and published host-port capacity"
	}
	return state, nil
}

// Scale only the serving specification, never an unready image/configuration.
// Compare server-normalized jobs from allocations and the current owned job,
// rather than comparing a renderer's sparse fields with Nomad's defaults.
func (b *Backend) retainedScaleJob(ctx context.Context, r *record, job map[string]json.RawMessage, requireCanary, requireDesiredHealthy bool) (map[string]json.RawMessage, error) {
	var groups []map[string]any
	if err := json.Unmarshal(job["TaskGroups"], &groups); err != nil {
		return nil, err
	}
	if len(groups) != 1 {
		return nil, nil
	}
	count, _ := groups[0]["Count"].(float64)
	// Increases need the existing count healthy, not the not-yet-created
	// desired count. Reductions still need the entire retained count healthy.
	requiredHealthy := min(int(count), r.Max)
	// Automatic failed-scale recovery is placement-free: admission and every
	// subsequent registration must already have the whole desired count healthy.
	if requireDesiredHealthy {
		requiredHealthy = r.Max
	}
	update, ok := groups[0]["Update"].(map[string]any)
	if !ok || requireCanary && update["Canary"] == float64(0) {
		return nil, nil
	}
	rendered, err := b.renderJob(ctx, r)
	if err != nil {
		return nil, err
	}
	desiredJSON, err := json.Marshal(rendered["Job"])
	if err != nil {
		return nil, err
	}
	currentJSON, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	var desired, existing any
	if err := json.Unmarshal(desiredJSON, &desired); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(currentJSON, &existing); err != nil {
		return nil, err
	}
	if !matchesRenderedScale(desired, existing) {
		// A superseded registering operation may have saved a new spec
		// (for example, revoked ports) without registering it yet.
		return nil, nil
	}
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return nil, err
	}
	healthy := 0
	var servingMeta json.RawMessage
	for _, a := range as {
		if a.ClientStatus != "running" || a.DesiredStatus != "run" {
			continue
		}
		var detail struct {
			Job              map[string]json.RawMessage
			DeploymentStatus *struct {
				Healthy *bool
			}
		}
		if _, err := b.c.get(ctx, "/v1/allocation/"+a.ID, nil, &detail); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if detail.DeploymentStatus == nil || detail.DeploymentStatus.Healthy == nil || !*detail.DeploymentStatus.Healthy {
			continue
		}
		if sameScaleSpecification(job, detail.Job) {
			if servingMeta == nil {
				servingMeta = detail.Job["Meta"]
			} else if !equalJobMeta(servingMeta, detail.Job["Meta"]) {
				continue
			}
			healthy++
		}
	}
	if healthy < requiredHealthy {
		return nil, nil
	}
	// A blocked rollout may carry a newer operation marker than its serving
	// predecessors. Preserve the predecessors' Meta, not that unplaced job's.
	job["Meta"] = servingMeta
	groups[0]["Count"] = r.Max
	// Keep every other health/deadline setting unchanged. Nomad rejects
	// AutoPromote without canaries, so promotion is disabled with them.
	update["Canary"] = 0
	update["AutoPromote"] = false
	job["TaskGroups"], err = json.Marshal(groups)
	return job, err
}

// Nomad adds defaults to sparse rendered jobs. Compare every desired field and
// also the optional fields whose removal changes the application specification.
func matchesRenderedScale(desired, existing any) bool {
	switch desired := desired.(type) {
	case map[string]any:
		existing, ok := existing.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range desired {
			if key == "Count" || key == "Update" || key == "karakoram.operation" {
				continue
			}
			if !matchesRenderedScale(value, existing[key]) {
				return false
			}
		}
		// Only Nomad's exact implicit signal/discovery constraints may be
		// absent from a sparse render. User placement constraints are not defaults.
		for _, key := range []string{"ReservedPorts", "Checks", "Cores", "Constraints", "Spreads", "Volumes", "VolumeMounts", "EphemeralDisk"} {
			if _, present := desired[key]; present {
				continue
			}
			switch value := existing[key].(type) {
			case nil:
			case float64:
				if value != 0 {
					return false
				}
			case []any:
				if key == "Constraints" && implicitNomadConstraints(value) {
					continue
				}
				if len(value) != 0 {
					return false
				}
			case map[string]any:
				if key == "EphemeralDisk" && value["SizeMB"] == float64(300) && value["Sticky"] == false && value["Migrate"] == false {
					continue // Nomad's default for an omitted ephemeral disk.
				}
				if len(value) != 0 {
					return false
				}
			default:
				return false
			}
		}
		return true
	case []any:
		existing, ok := existing.([]any)
		if !ok || len(desired) != len(existing) {
			return false
		}
		for i := range desired {
			if !matchesRenderedScale(desired[i], existing[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(desired, existing)
	}
}

func implicitNomadConstraints(constraints []any) bool {
	for _, value := range constraints {
		constraint, ok := value.(map[string]any)
		if !ok || len(constraint) != 3 {
			return false
		}
		switch {
		case constraint["LTarget"] == "${attr.os.signals}" && constraint["Operand"] == "set_contains" && constraint["RTarget"] == "SIGTERM":
		case constraint["LTarget"] == "${attr.nomad.service_discovery}" && constraint["Operand"] == "=" && constraint["RTarget"] == "true":
		case constraint["LTarget"] == "${attr.nomad.version}" && constraint["Operand"] == "semver" && constraint["RTarget"] == ">= 1.4.0":
		default:
			return false
		}
	}
	return true
}

func sameScaleSpecification(a, b map[string]json.RawMessage) bool {
	var leftMeta, rightMeta map[string]string
	if json.Unmarshal(a["Meta"], &leftMeta) != nil || json.Unmarshal(b["Meta"], &rightMeta) != nil {
		return false
	}
	delete(leftMeta, "karakoram.operation")
	delete(rightMeta, "karakoram.operation")
	if !reflect.DeepEqual(leftMeta, rightMeta) {
		return false
	}
	for _, key := range []string{"ID", "Namespace", "Type", "Datacenters", "NodePool", "Constraints", "Affinities", "Spreads", "TaskGroups"} {
		var left, right any
		if json.Unmarshal(a[key], &left) != nil && len(a[key]) > 0 {
			return false
		}
		if json.Unmarshal(b[key], &right) != nil && len(b[key]) > 0 {
			return false
		}
		if key == "TaskGroups" {
			for _, value := range []any{left, right} {
				groups, ok := value.([]any)
				if !ok {
					return false
				}
				for _, value := range groups {
					group, ok := value.(map[string]any)
					if !ok {
						return false
					}
					delete(group, "Count")
					delete(group, "Update")
				}
			}
		}
		if !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

func equalJobMeta(a, b json.RawMessage) bool {
	var left, right map[string]string
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && reflect.DeepEqual(left, right)
}

// Only server-maintained identity/status fields are excluded, matching Nomad's
// Job.SpecChanged normalization (NomadTokenID is the registering token's
// accessor, overwritten on every registration). All submitted specification
// fields (including every level of Meta, Count and Update) remain in the
// digest. The source is an already server-normalized retained job.
func retainedJobHash(job map[string]json.RawMessage) string {
	spec := make(map[string]json.RawMessage, len(job))
	for key, value := range job {
		switch key {
		case "Version", "CreateIndex", "ModifyIndex", "JobModifyIndex", "SubmitTime", "Status", "StatusDescription", "Stable", "NomadTokenID":
			continue
		}
		spec[key] = value
	}
	// Decode before hashing so JSON whitespace and object ordering are irrelevant.
	data, _ := json.Marshal(spec)
	var normalized any
	_ = json.Unmarshal(data, &normalized)
	data, _ = json.Marshal(normalized)
	sum := sha256.Sum256(data)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func operationMatchesJob(current *operation, job observedJob) bool {
	if job.Stop || job.Version != current.JobVersion {
		return false
	}
	if current.RetainedHash != "" {
		return job.JobModifyIndex == current.JobModifyIndex
	}
	return job.Meta["karakoram.operation"] == current.Issued.Format(time.RFC3339Nano)
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
