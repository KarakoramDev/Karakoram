// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var layout = infra.Layout{AppDir: "/opt/karakoram", VolumeRoot: "/var/lib/karakoram/volumes", User: "karakoram", PrivateAddressFamily: infra.AddressFamilyIPv4}

type Backend struct {
	c           *client
	target      infra.Target
	mu          sync.Mutex
	artifacts   *artifactResolver
	artifactURL string
	clock       func() time.Time
	instance    string
}

var _ infra.Backend = (*Backend)(nil)
var _ infra.CredentialRotator = (*Backend)(nil)

func (b *Backend) Name() string                     { return "nomad" }
func (b *Backend) Layout() infra.Layout             { return layout }
func (b *Backend) Capabilities() infra.Capabilities { return nomadCapabilities }
func (b *Backend) SetToken(token string)            { b.c.setToken(token) }
func reject(message string) error                   { return rejectedConfig("nomad", message) }
func (b *Backend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	var ns struct{ Name string }
	_, err := b.c.get(ctx, "/v1/namespace/"+b.target.ID, nil, &ns)
	return infra.Project{ID: ns.Name, Name: ns.Name, PublicIPv4: b.target.Config["publicIPv4"], PublicIPv6: b.target.Config["publicIPv6"]}, err
}
func safeHostname(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}
func resourceValidation(r infra.Resources) error {
	for _, v := range []float64{r.MinCPU, r.MaxCPU, r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk} {
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return reject("resource bounds must be finite and non-negative")
		}
	}
	if r.StartCPU != nil && (*r.StartCPU < 0 || math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0)) {
		return reject("start CPU must be finite and non-negative")
	}
	if r.CPUMode == infra.CPUDedicated {
		cores := r.MaxCPU
		if cores == 0 {
			cores = r.MinCPU
		}
		if cores < 1 || math.Trunc(cores) != cores {
			return reject("dedicated CPU requires a whole number of at least one core")
		}
	} else if r.CPUMode != "" && r.CPUMode != infra.CPUShared {
		return reject("unknown CPU mode")
	}
	if r.MaxCPU > 0 && r.MinCPU > r.MaxCPU || r.MaxRAM > 0 && r.MinRAM > r.MaxRAM || r.MaxDisk > 0 && r.MinDisk > r.MaxDisk {
		return reject("minimum resource bounds exceed maximum")
	}
	return nil
}
func (b *Backend) observed(ctx context.Context, r *record) (infra.Service, error) {
	if err := b.resumePending(ctx, r); err != nil {
		return infra.Service{}, err
	}
	s := infra.Service{ID: r.ID, Hostname: r.Hostname, Kind: r.Kind, Status: infra.StatusReadyToDeploy, Resources: r.Resources, MinContainers: r.Min, MaxContainers: r.Max, Ownership: map[string]string{}}
	for k, v := range r.Vars {
		if identityKey(k) {
			s.Ownership[k] = v
		}
	}
	for _, k := range r.SecretKeys {
		if identityKey(k) {
			s.Masked = true
		}
	}
	if r.Importing {
		s.Importing = true
		s.Status = infra.StatusCreating
		return s, nil
	}
	if r.Kind == infra.KindVolume {
		status, err := b.volumeStatus(ctx, r)
		s.Status = status
		return s, err
	}
	if r.Desired == "deleting" {
		s.Status = infra.StatusDeleting
		return s, nil
	}
	if r.Deployment == nil {
		return s, nil
	}
	if r.Desired == "stopped" {
		if b.restorationFenced(r) || r.Pending != nil && r.Pending.Phase == "stopping" {
			s.Status = infra.StatusCreating
			if r.Pending != nil && r.Pending.Action == "stop" {
				state, err := b.OpStatus(ctx, encodeOp(*r.Pending))
				if state.Done {
					s.Status = infra.StatusStopped
				}
				return s, err
			}
			return s, nil
		}
		s.Status = infra.StatusStopped
		return s, nil
	}
	s.Status = infra.StatusCreating
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return s, err
	}
	var job observedJob
	_, err = b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &job)
	if err != nil && !errors.Is(err, infra.ErrNotFound) {
		return s, err
	}
	if r.Pending != nil && r.Pending.Phase == "failed" {
		s.Status = infra.StatusFailed
	}
	if errors.Is(err, infra.ErrNotFound) {
		s.Status, err = b.restoreMissingJob(ctx, r)
		return s, err
	}
	if !b.ownsJob(r, job) {
		s.Status = infra.StatusFailed
		return s, jobOwnershipConflict()
	}
	if job.Stop {
		// A confirmed external scheduler stop is recoverable through Start.
		// Never confuse our own in-flight shutdown/replacement with that
		// signal, or let observation bypass a held lifecycle/restoration claim.
		if r.Desired == "running" && (r.Pending == nil || r.Pending.Phase == "done") &&
			!b.operationLeaseBlocked(r) && !b.restorationFenced(r) {
			s.Status = infra.StatusStopped
		}
		return s, nil
	}
	if r.Volume == "" && r.Pending != nil && r.Pending.Phase != "done" {
		// A running canary (or serving predecessor) is not completion of
		// this configuration. Keep service readiness behind the same native
		// deployment/evaluation barrier as the caller-visible operation.
		if r.Pending.Phase != "failed" || !operationMatchesJob(r.Pending, job) {
			return s, nil
		}
		// A historical timeout must not hide eventual native recovery, but
		// neither running tasks nor an automatically reverted job prove it.
		var deployment *struct {
			JobVersion uint64
			Status     string
		}
		_, err := b.c.get(ctx, "/v1/job/"+r.Hostname+"/deployment", nil, &deployment)
		if err != nil && !errors.Is(err, infra.ErrNotFound) {
			return s, err
		}
		if deployment != nil && deployment.JobVersion == job.Version && deployment.Status != "successful" {
			return s, nil
		}
		// A matching successful deployment is durable completion proof even
		// after Nomad garbage-collects the original evaluation. Count-only
		// updates without that proof still require a completed evaluation.
		complete := deployment != nil && deployment.JobVersion == job.Version && deployment.Status == "successful"
		if !complete {
			var evaluation struct{ Status string }
			if _, err := b.c.get(ctx, "/v1/evaluation/"+r.Pending.EvalID, nil, &evaluation); err != nil {
				if errors.Is(err, infra.ErrNotFound) {
					return s, nil
				}
				return s, err
			}
			complete = evaluation.Status == "complete"
		}
		active, running := 0, 0
		for _, a := range as {
			if !allocationTerminal(a) {
				active++
				if a.ClientStatus == "running" {
					running++
				}
			}
		}
		if complete && active == r.Max && running == r.Max {
			s.Status = infra.StatusRunning
		}
		return s, nil
	}
	if r.Volume == "" && r.Pending != nil && r.Pending.Phase == "done" &&
		operationMatchesJob(r.Pending, job) {
		// Count-only and in-place updates legitimately retain older allocation
		// versions. Accept them only after this exact operation's deployment/
		// evaluation gate completed, never to hide a pending or failed rollout.
		running := 0
		for _, a := range as {
			if a.ClientStatus == "running" {
				running++
			}
		}
		if !job.Stop && running >= r.Max {
			s.Status = infra.StatusRunning
			return s, nil
		}
	}
	for _, a := range as {
		if a.JobVersion != job.Version || a.CreateIndex <= job.JobModifyIndex {
			continue
		}
		if a.ClientStatus == "running" {
			s.Status = infra.StatusRunning
			return s, nil
		}
		if a.ClientStatus == "failed" || a.ClientStatus == "lost" {
			s.Status = infra.StatusFailed
		}
	}
	return s, nil
}
func (b *Backend) GetService(ctx context.Context, hostname string) (*infra.Service, error) {
	r, err := b.load(ctx, hostname)
	if err != nil || r == nil {
		return nil, err
	}
	s, err := b.observed(ctx, r)
	return &s, err
}
func (b *Backend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	rs, _, err := b.records(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]infra.Service, len(rs))
	if err := infra.ObserveParallel(ctx, len(rs), func(i int) error {
		// Pending completion and missing-job restoration retain their own
		// existing mutex/reload fences inside observed's mutation helpers.
		service, err := b.observed(ctx, rs[i])
		out[i] = service
		return err
	}); err != nil {
		return nil, err
	}
	return out, nil
}
func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", silentFailure("cannot create service identity")
	}
	return hex.EncodeToString(value[:]), nil
}
func (b *Backend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (op infra.Op, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rs, _, err := b.records(ctx, "")
	if err != nil {
		return op, err
	}
	existing := map[string]*record{}
	for _, r := range rs {
		existing[r.Hostname] = r
	}
	seen := map[string]bool{}
	batchHash := importHash(specs)
	for _, s := range specs {
		if !safeHostname(s.Hostname) {
			return op, reject("service hostname must be a DNS label")
		}
		if seen[s.Hostname] || existing[s.Hostname] != nil && !resumableImport(existing[s.Hostname], batchHash) {
			return op, infra.ErrConflict
		}
		seen[s.Hostname] = true
		if s.Kind != infra.KindVolume && s.Kind != infra.KindRuntime {
			return op, reject("unknown service kind")
		}
		if s.Kind == infra.KindRuntime && s.Base != infra.BaseUbuntu2404 {
			return op, reject("unsupported runtime base")
		}
		if err = resourceValidation(s.Resources); err != nil {
			return op, err
		}
		if err = validateEnvironment(s.Env); err != nil {
			return op, err
		}
		if err = validateEnvironment(s.Vars); err != nil {
			return op, err
		}
		if _, reserved := s.Vars["record"]; reserved {
			return op, reject("readable variable key record is reserved for service metadata")
		}
		for k := range s.Env {
			if _, ok := s.Vars[k]; ok {
				return op, reject("variable cannot be both secret and readable")
			}
		}
		if s.MinContainers < 0 || s.MaxContainers < 0 || s.MaxContainers > 0 && s.MinContainers > s.MaxContainers {
			return op, reject("invalid container bounds")
		}
		if s.Volume != "" && s.MaxContainers > 1 {
			return op, reject("a volume runtime requires one replica")
		}
		if s.Kind == infra.KindRuntime {
			_, jobErr := b.c.get(ctx, "/v1/job/"+s.Hostname, nil, nil)
			if jobErr == nil {
				return op, &APIError{Status: 409, Op: "import", Message: "job already exists without a Karakoram service record"}
			}
			if !errors.Is(jobErr, infra.ErrNotFound) {
				return op, jobErr
			}
		}
		if existing[s.Hostname] == nil {
			env, envErr := b.readVariable(ctx, "nomad/jobs/"+s.Hostname)
			if envErr != nil {
				return op, envErr
			}
			if env.ModifyIndex != 0 {
				return op, &APIError{Status: 409, Op: "import", Message: "environment already exists without a Karakoram service record; refusing adoption"}
			}
		}
	}
	ordered := append([]infra.ServiceSpec(nil), specs...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Kind == infra.KindVolume && ordered[j].Kind != infra.KindVolume })
	ids := map[string]string{}
	// Reservations survive errors. Never compensate storage after publishing
	// an identity: another observer may already be using that committed record.
	defer func() {
		if err != nil && len(ids) != 0 {
			op = encodeOp(operation{Action: "import", Services: ids, Issued: time.Now().UTC()})
		}
	}()
	reserved := make([]*record, 0, len(ordered))
	for _, s := range ordered {
		r := existing[s.Hostname]
		if r == nil {
			r, err = b.reserveImport(ctx, s, batchHash, rs, existing)
			if r != nil {
				ids[r.Hostname] = r.ID
			}
			if err != nil {
				return op, err
			}
			existing[r.Hostname] = r
			rs = append(rs, r)
		}
		ids[r.Hostname] = r.ID
		reserved = append(reserved, r)
	}
	// Only after every identity is durable may resource creation begin.
	for i, r := range reserved {
		if !r.Importing {
			continue
		}
		if err = b.finishImport(ctx, r, ordered[i]); err != nil {
			return op, err
		}
	}
	// Imported identities and resources remain visible to resume/cleanup on
	// every failure path, including lost successful replies.
	return encodeOp(operation{Action: "import", Services: ids, Issued: time.Now().UTC()}), nil
}
func ignoreNotFound(err error) error {
	if errors.Is(err, infra.ErrNotFound) {
		return nil
	}
	return err
}
func datacenters(config map[string]string) []string {
	var out []string
	for _, dc := range strings.Split(config["datacenters"], ",") {
		out = append(out, strings.TrimSpace(dc))
	}
	return out
}
