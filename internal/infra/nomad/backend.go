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
	if err := b.resumePorts(ctx, r); err != nil {
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
	if r.Kind == infra.KindVolume {
		s.Status = infra.StatusActive
		return s, nil
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
	out := make([]infra.Service, 0, len(rs))
	for _, r := range rs {
		s, e := b.observed(ctx, r)
		if e != nil {
			return nil, e
		}
		out = append(out, s)
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
	for _, s := range specs {
		if !safeHostname(s.Hostname) {
			return op, reject("service hostname must be a DNS label")
		}
		if seen[s.Hostname] || existing[s.Hostname] != nil {
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
	}
	ordered := append([]infra.ServiceSpec(nil), specs...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Kind == infra.KindVolume && ordered[j].Kind != infra.KindVolume })
	var created []*record
	createdEnv := map[string]uint64{}
	defer func() {
		if err == nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for i := len(created) - 1; i >= 0; i-- {
			r := created[i]
			if r.Index != 0 {
				_ = b.removeVariable(cleanup, recordPrefix+r.Hostname, r.Index)
			}
			if index := createdEnv[r.Hostname]; index != 0 {
				_ = b.removeVariable(cleanup, "nomad/jobs/"+r.Hostname, index)
			}
			if r.VolumeID != "" {
				_ = b.c.delete(cleanup, "/v1/volume/host/"+r.VolumeID, nil, nil)
			}
		}
	}()
	ids := map[string]string{}
	for _, s := range ordered {
		id, e := newID()
		if e != nil {
			return op, e
		}
		r := &record{ID: id, Hostname: s.Hostname, Kind: s.Kind, Volume: s.Volume, SpreadGroup: s.SpreadGroup, Resources: s.Resources, Min: s.MinContainers, Max: s.MaxContainers, Vars: map[string]string{}, Desired: "stopped"}
		if r.Min == 0 {
			r.Min = 1
		}
		if r.Max == 0 {
			r.Max = 1
		}
		if r.Max < r.Min {
			return op, reject("invalid container bounds")
		}
		for k, v := range s.Vars {
			r.Vars[k] = v
		}
		for k := range s.Env {
			r.SecretKeys = append(r.SecretKeys, k)
		}
		sort.Strings(r.SecretKeys)
		if r.Kind == infra.KindVolume {
			ns, e := b.nodes(ctx)
			if e != nil {
				return op, e
			}
			r.Node, e = chooseNode(ns, rs, r.SpreadGroup, s.SpreadReplaces)
			if e != nil {
				return op, e
			}
			if err = b.createVolume(ctx, r); err != nil {
				return op, err
			}
			r.ID = r.VolumeID
		} else if r.Volume != "" {
			v := existing[r.Volume]
			if v == nil || v.Kind != infra.KindVolume {
				return op, reject("runtime references an absent volume")
			}
			r.Node = v.Node
		}
		created = append(created, r)
		env := map[string]string{}
		for k, v := range s.Env {
			env[k] = v
		}
		for k, v := range s.Vars {
			env[k] = v
		}
		if len(env) > 0 {
			v := variable{Path: "nomad/jobs/" + r.Hostname, Items: env}
			if err = b.writeVariable(ctx, &v); err != nil {
				return op, err
			}
			createdEnv[r.Hostname] = v.ModifyIndex
		}
		existing[r.Hostname] = r
		rs = append(rs, r)
		ids[r.Hostname] = r.ID
	}
	// Publish durable identities only after all resources and environment writes succeed.
	for _, r := range created {
		if err = b.save(ctx, r); err != nil {
			return op, err
		}
	}
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
