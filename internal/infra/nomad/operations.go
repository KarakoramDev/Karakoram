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
}

func encodeOp(o operation) infra.Op {
	if o.Issued.IsZero() {
		o.Issued = time.Now().UTC()
	}
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
	o := operation{Action: action, Hostname: r.Hostname, RecordID: r.ID, JobID: r.Hostname, Issued: time.Now().UTC(), Phase: "stopping"}
	// Persist the pending specification before stopping; polling can resume after a restart.
	r.Pending = &o
	if err := b.save(ctx, r); err != nil {
		return infra.Op{}, err
	}
	if err := ignoreNotFound(b.c.delete(ctx, "/v1/job/"+r.Hostname, nil, nil)); err != nil {
		return infra.Op{}, err
	}
	return encodeOp(o), nil
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
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return state, err
	}
	if current.Phase == "stopping" {
		for _, a := range as {
			if !allocationTerminal(a) {
				state.Message = "waiting for previous allocations to stop"
				// A persisted stop may precede its HTTP request after a process restart.
				err = ignoreNotFound(b.c.delete(ctx, "/v1/job/"+r.Hostname, nil, nil))
				return state, err
			}
		}
		if err = ignoreNotFound(b.c.delete(ctx, "/v1/job/"+r.Hostname, query("purge", "true"), nil)); err != nil {
			return state, err
		}
		if o.Action == "delete" {
			if err = b.finishDelete(ctx, r); err != nil {
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
			if err = b.c.post(ctx, "/v1/jobs", nil, job, &response); err != nil {
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
