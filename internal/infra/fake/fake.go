// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package fake implements a deterministic, thread-safe in-memory infrastructure
// backend for engine tests and backend contract tests.
package fake

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Options configures an isolated backend. Zero capabilities enable all core
// optional features; set any flag explicitly to test a restricted backend.
type Options struct {
	Name         string
	Project      infra.Project
	Capabilities infra.Capabilities
	Layout       infra.Layout
	// Polls is the number of incomplete observations before an operation
	// completes. Service status transitions are applied at completion.
	Polls       int
	MaskSecrets bool
}

func defaults(o Options) Options {
	if o.Name == "" {
		o.Name = "fake"
	}
	if o.Project.ID == "" {
		o.Project.ID = "fake-project"
	}
	if o.Capabilities == (infra.Capabilities{}) {
		o.Capabilities = infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Logs: true, HorizontalScaling: true, DedicatedCPU: true}
	}
	if o.Layout == (infra.Layout{}) {
		o.Layout = infra.Layout{AppDir: "/srv/karakoram", VolumeRoot: "/srv/volumes", User: "karakoram"}
	}
	return o
}

type record struct {
	service        infra.Service
	secrets, vars  map[string]string
	volume         string
	runtime        infra.RuntimeSpec
	deployed       bool
	public         []infra.PublicPort
	subdomain      bool
	logs           []infra.LogLine
	placementGroup string
}

type statusChange struct {
	record *record
	status string
}

type operation struct {
	remaining int
	ids       []string
	statuses  []statusChange
}
type state struct {
	mu       sync.Mutex
	next     int
	services map[string]*record
	ops      map[string]*operation
	failures map[string][]error
	calls    []string
}

// Backend implements the infrastructure contract without external effects.
type Backend struct {
	options Options
	state   *state
	token   string
}

var _ infra.Backend = (*Backend)(nil)
var _ infra.SubdomainAccess = (*Backend)(nil)
var _ infra.CredentialRotator = (*Backend)(nil)

// New creates a backend with isolated service and operation state.
func New(o Options) *Backend {
	return &Backend{options: defaults(o), state: &state{services: map[string]*record{}, ops: map[string]*operation{}, failures: map[string][]error{}}}
}

func (b *Backend) Name() string                     { return b.options.Name }
func (b *Backend) Capabilities() infra.Capabilities { return b.options.Capabilities }
func (b *Backend) Layout() infra.Layout             { return b.options.Layout }
func (b *Backend) SetToken(token string) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	b.token = token
}

// FailNext queues a failure for the named Backend method, before any mutation.
func (b *Backend) FailNext(method string, err error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	b.state.failures[method] = append(b.state.failures[method], err)
}

// Calls returns an independent snapshot of calls, including injected failures.
func (b *Backend) Calls() []string {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	return slices.Clone(b.state.calls)
}

// enter requires the state lock to be held.
func (b *Backend) enter(ctx context.Context, method string) error {
	b.state.calls = append(b.state.calls, method)
	if err := ctx.Err(); err != nil {
		return err
	}
	if q := b.state.failures[method]; len(q) > 0 {
		b.state.failures[method] = q[1:]
		return q[0]
	}
	return nil
}
func (b *Backend) id(prefix string) string {
	b.state.next++
	return fmt.Sprintf("%s-%d", prefix, b.state.next)
}
func (b *Backend) op(kind, hostname string, ids ...string) infra.Op {
	id := b.id("op")
	b.state.ops[id] = &operation{remaining: b.options.Polls, ids: slices.Clone(ids)}
	op := infra.Op{ID: id, Kind: kind, Hostname: hostname}
	if len(ids) == 1 {
		op.ServiceID = ids[0]
	}
	return op
}
func (b *Backend) setStatus(op infra.Op, r *record, status string) {
	if b.options.Polls <= 0 {
		r.service.Status = status
		return
	}
	state := b.state.ops[op.ID]
	state.statuses = append(state.statuses, statusChange{record: r, status: status})
}
func (b *Backend) get(hostname string) (*record, error) {
	r := b.state.services[hostname]
	if r == nil {
		return nil, fmt.Errorf("%w: service %s", infra.ErrNotFound, hostname)
	}
	return r, nil
}
func reject(message string) error {
	return &infra.RejectedError{Code: "invalidRequest", Message: message}
}
func identity(key string) bool {
	switch key {
	case "KARAKORAM_OWNER", "KARAKORAM_CLUSTER", "KARAKORAM_MEMBER", "KARAKORAM_AGENT_TOKEN", "KARAKORAM_DATA_DIR", "KARAKORAM_RELAY_OWNER":
		return true
	}
	return false
}
func cloneResources(r infra.Resources) infra.Resources {
	if r.StartCPU != nil {
		v := *r.StartCPU
		r.StartCPU = &v
	}
	return r
}
func (b *Backend) observed(r *record) infra.Service {
	s := r.service
	s.Resources = cloneResources(s.Resources)
	s.Ownership = map[string]string{}
	for k, v := range r.secrets {
		if !identity(k) {
			continue
		}
		if b.options.MaskSecrets {
			s.Masked = true
		} else {
			s.Ownership[k] = v
		}
	}
	for k, v := range r.vars {
		if identity(k) {
			s.Ownership[k] = v
		}
	}
	return s
}

func (b *Backend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "ProjectInfo"); err != nil {
		return infra.Project{}, err
	}
	return b.options.Project, nil
}
func (b *Backend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "ImportServices"); err != nil {
		return infra.Op{}, err
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if b.state.services[s.Hostname] != nil || seen[s.Hostname] {
			return infra.Op{}, fmt.Errorf("%w: service %s exists", infra.ErrConflict, s.Hostname)
		}
		if s.Hostname == "" || (s.Kind != infra.KindRuntime && s.Kind != infra.KindVolume) {
			return infra.Op{}, reject("service requires a hostname and supported kind")
		}
		if s.Kind == infra.KindRuntime && s.Base != infra.BaseUbuntu2404 {
			return infra.Op{}, reject("unsupported runtime base")
		}
		if invalidResources(s.Resources) {
			return infra.Op{}, reject("negative resources")
		}
		if unknownCPUMode(s.Resources) {
			return infra.Op{}, reject("unknown CPU mode")
		}
		seen[s.Hostname] = true
	}
	services := map[string]string{}
	ids := make([]string, 0, len(specs))
	for _, s := range specs {
		id := b.id("service")
		min, max := s.MinContainers, s.MaxContainers
		if s.Kind == infra.KindRuntime {
			if min == 0 {
				min = 1
			}
			if max == 0 {
				max = min
			}
		}
		b.state.services[s.Hostname] = &record{service: infra.Service{ID: id, Hostname: s.Hostname, Kind: s.Kind, Status: infra.StatusCreating, Resources: cloneResources(s.Resources), MinContainers: min, MaxContainers: max}, secrets: maps.Clone(s.Env), vars: maps.Clone(s.Vars), volume: s.Volume}
		services[s.Hostname] = id
		ids = append(ids, id)
	}
	op := b.op("import", "", ids...)
	op.Services = services
	for _, s := range specs {
		status := infra.StatusReadyToDeploy
		if s.Kind == infra.KindVolume {
			status = infra.StatusActive
		}
		b.setStatus(op, b.state.services[s.Hostname], status)
	}
	return op, nil
}
func (b *Backend) GetService(ctx context.Context, hostname string) (*infra.Service, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "GetService"); err != nil {
		return nil, err
	}
	r := b.state.services[hostname]
	if r == nil {
		return nil, nil
	}
	s := b.observed(r)
	return &s, nil
}
func (b *Backend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "ListServices"); err != nil {
		return nil, err
	}
	out := []infra.Service{}
	for hostname, r := range b.state.services {
		if strings.HasPrefix(hostname, prefix) {
			out = append(out, b.observed(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
}
func (b *Backend) DeleteService(ctx context.Context, hostname string) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "DeleteService"); err != nil {
		return infra.Op{}, err
	}
	return b.delete(hostname, "", false)
}
func (b *Backend) DeleteServiceByID(ctx context.Context, hostname, expectedID string) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "DeleteServiceByID"); err != nil {
		return infra.Op{}, err
	}
	return b.delete(hostname, expectedID, true)
}
func (b *Backend) delete(hostname, expectedID string, fenced bool) (infra.Op, error) {
	r := b.state.services[hostname]
	if r == nil {
		return infra.Op{}, nil
	}
	if fenced && r.service.ID != expectedID {
		return infra.Op{}, infra.ErrConflict
	}
	if r.service.Kind == infra.KindVolume {
		for _, other := range b.state.services {
			if other.service.Kind == infra.KindRuntime && other.volume == hostname {
				return infra.Op{}, fmt.Errorf("%w: volume is mounted", infra.ErrConflict)
			}
		}
	}
	delete(b.state.services, hostname)
	return b.op("delete", hostname, r.service.ID), nil
}
func cloneRuntime(s infra.RuntimeSpec) infra.RuntimeSpec {
	s.Prepare = slices.Clone(s.Prepare)
	s.Args = slices.Clone(s.Args)
	s.Ports = slices.Clone(s.Ports)
	s.Env = maps.Clone(s.Env)
	if s.Readiness != nil {
		r := *s.Readiness
		r.Args = slices.Clone(r.Args)
		s.Readiness = &r
	}
	return s
}
func (b *Backend) Deploy(ctx context.Context, hostname string, deployment infra.Deployment) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "Deploy"); err != nil {
		return infra.Op{}, err
	}
	if err := deployment.Validate(); err != nil {
		return infra.Op{}, &infra.RejectedError{Code: "invalidDeployment", Message: err.Error()}
	}
	if deployment.Runtime.Base != infra.BaseUbuntu2404 {
		return infra.Op{}, reject("unsupported runtime base")
	}
	r, err := b.get(hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.service.Kind != infra.KindRuntime {
		return infra.Op{}, reject("only runtime services can deploy")
	}
	volume := deployment.Runtime.Volume
	if volume != "" {
		v := b.state.services[volume]
		if v == nil || v.service.Kind != infra.KindVolume {
			return infra.Op{}, fmt.Errorf("%w: volume is absent", infra.ErrConflict)
		}
		for name, other := range b.state.services {
			if name != hostname && other.service.Kind == infra.KindRuntime && other.volume == volume {
				return infra.Op{}, fmt.Errorf("%w: volume is mounted", infra.ErrConflict)
			}
		}
	}
	if _, err := io.Copy(io.Discard, deployment.Artifact.Reader); err != nil {
		return infra.Op{}, err
	}
	r.runtime = cloneRuntime(deployment.Runtime)
	r.deployed = true
	r.volume = volume
	op := b.op("deploy", hostname, r.service.ID)
	b.setStatus(op, r, infra.StatusActive)
	return op, nil
}

// Deployed returns an independent snapshot of the last successful deployment.
func (b *Backend) Deployed(hostname string) (infra.RuntimeSpec, bool) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	r := b.state.services[hostname]
	if r == nil || !r.deployed {
		return infra.RuntimeSpec{}, false
	}
	return cloneRuntime(r.runtime), true
}
func (b *Backend) SetEnvSecrets(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, "SetEnvSecrets", hostname, values, true)
}
func (b *Backend) SetEnvVars(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, "SetEnvVars", hostname, values, false)
}
func (b *Backend) setEnv(ctx context.Context, method, hostname string, values map[string]string, secret bool) error {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, method); err != nil {
		return err
	}
	r, err := b.get(hostname)
	if err != nil {
		return err
	}
	if !secret {
		for key := range values {
			if _, exists := r.secrets[key]; exists && key != "KARAKORAM_OWNER" && key != "KARAKORAM_RELAY_OWNER" {
				return reject("refusing to make a sensitive variable readable")
			}
		}
	}
	if r.secrets == nil {
		r.secrets = map[string]string{}
	}
	if r.vars == nil {
		r.vars = map[string]string{}
	}
	for k, v := range values {
		if secret {
			delete(r.vars, k)
			r.secrets[k] = v
		} else {
			delete(r.secrets, k)
			r.vars[k] = v
		}
	}
	return nil
}
func (b *Backend) Start(ctx context.Context, hostname string) (infra.Op, error) {
	return b.lifecycle(ctx, "Start", hostname, infra.StatusActive)
}
func (b *Backend) Stop(ctx context.Context, hostname string) (infra.Op, error) {
	return b.lifecycle(ctx, "Stop", hostname, infra.StatusStopped)
}
func (b *Backend) Restart(ctx context.Context, hostname string) (infra.Op, error) {
	return b.lifecycle(ctx, "Restart", hostname, infra.StatusActive)
}
func (b *Backend) lifecycle(ctx context.Context, method, hostname, status string) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, method); err != nil {
		return infra.Op{}, err
	}
	r, err := b.get(hostname)
	if err != nil {
		return infra.Op{}, err
	}
	op := b.op(strings.ToLower(method), hostname, r.service.ID)
	b.setStatus(op, r, status)
	return op, nil
}
func invalidResources(r infra.Resources) bool {
	return r.MinCPU < 0 || r.MaxCPU < 0 || r.MinRAM < 0 || r.MaxRAM < 0 || r.MinDisk < 0 || r.MaxDisk < 0 || r.StartCPU != nil && *r.StartCPU < 0
}

// Real backends accept only the infra CPU mode constants; so does the fake.
func unknownCPUMode(r infra.Resources) bool {
	return r.CPUMode != "" && r.CPUMode != infra.CPUShared && r.CPUMode != infra.CPUDedicated
}
func (b *Backend) SetResources(ctx context.Context, hostname string, resources infra.Resources) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "SetResources"); err != nil {
		return infra.Op{}, err
	}
	if invalidResources(resources) {
		return infra.Op{}, reject("negative resources")
	}
	if unknownCPUMode(resources) {
		return infra.Op{}, reject("unknown CPU mode")
	}
	r, err := b.get(hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.service.Kind == infra.KindVolume && (resources.MinDisk != 0 && resources.MinDisk != r.service.Resources.MinDisk || resources.MaxDisk != 0 && resources.MaxDisk != r.service.Resources.MaxDisk) && !b.Capabilities().DiskGrowth {
		return infra.Op{}, infra.ErrUnsupported
	}
	if resources.CPUMode == infra.CPUDedicated && !b.Capabilities().DedicatedCPU {
		return infra.Op{}, infra.ErrUnsupported
	}
	dst := &r.service.Resources
	if resources.CPUMode != "" {
		dst.CPUMode = resources.CPUMode
	}
	if resources.MinCPU != 0 {
		dst.MinCPU = resources.MinCPU
	}
	if resources.MaxCPU != 0 {
		dst.MaxCPU = resources.MaxCPU
	}
	if resources.MinRAM != 0 {
		dst.MinRAM = resources.MinRAM
	}
	if resources.MaxRAM != 0 {
		dst.MaxRAM = resources.MaxRAM
	}
	if resources.MinDisk != 0 {
		dst.MinDisk = resources.MinDisk
	}
	if resources.MaxDisk != 0 {
		dst.MaxDisk = resources.MaxDisk
	}
	if resources.StartCPU != nil && *resources.StartCPU != 0 {
		v := *resources.StartCPU
		dst.StartCPU = &v
	}
	return b.op("resources", hostname, r.service.ID), nil
}
func (b *Backend) SetContainerCount(ctx context.Context, hostname string, min, max int) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "SetContainerCount"); err != nil {
		return infra.Op{}, err
	}
	if min < 1 || max < min {
		return infra.Op{}, reject("invalid container bounds")
	}
	if max > 1 && !b.Capabilities().HorizontalScaling {
		return infra.Op{}, infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return infra.Op{}, err
	}
	r.service.MinContainers = min
	r.service.MaxContainers = max
	return b.op("scale", hostname, r.service.ID), nil
}
func clonePort(p infra.PublicPort) infra.PublicPort {
	p.Allowlist = slices.Clone(p.Allowlist)
	return p
}
func (b *Backend) EnsurePublicPort(ctx context.Context, hostname string, port infra.PublicPort) error {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "EnsurePublicPort"); err != nil {
		return err
	}
	if !b.Capabilities().PublicTCP || len(port.Allowlist) > 0 && !b.Capabilities().TCPAllowlist {
		return infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return err
	}
	for name, other := range b.state.services {
		for _, p := range other.public {
			if p.Port == port.Port && name != hostname {
				return infra.ErrConflict
			}
		}
	}
	for i, p := range r.public {
		if p.Port == port.Port {
			r.public[i] = clonePort(port)
			return nil
		}
	}
	r.public = append(r.public, clonePort(port))
	return nil
}
func (b *Backend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "ListPublicPorts"); err != nil {
		return nil, err
	}
	if !b.Capabilities().PublicTCP {
		return nil, infra.ErrUnsupported
	}
	out := []infra.PublicPortRoute{}
	for name, r := range b.state.services {
		for _, p := range r.public {
			out = append(out, infra.PublicPortRoute{Hostname: name, PublicPort: clonePort(p)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname == out[j].Hostname {
			return out[i].Port < out[j].Port
		}
		return out[i].Hostname < out[j].Hostname
	})
	return out, nil
}
func (b *Backend) DeletePublicPorts(ctx context.Context, hostname string) error {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "DeletePublicPorts"); err != nil {
		return err
	}
	if !b.Capabilities().PublicTCP {
		return infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return err
	}
	r.public = nil
	return nil
}
func (b *Backend) SetSubdomainAccess(ctx context.Context, hostname string, enabled bool) (infra.Op, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "SetSubdomainAccess"); err != nil {
		return infra.Op{}, err
	}
	if !b.Capabilities().PublicHTTPS {
		return infra.Op{}, infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return infra.Op{}, err
	}
	r.subdomain = enabled
	return b.op("subdomain", hostname, r.service.ID), nil
}
func (b *Backend) SubdomainURLs(ctx context.Context, hostname string) ([]string, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "SubdomainURLs"); err != nil {
		return nil, err
	}
	if !b.Capabilities().PublicHTTPS {
		return nil, infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return nil, err
	}
	out := []string{}
	if r.subdomain {
		for _, p := range r.runtime.Ports {
			if p.HTTP {
				out = append(out, fmt.Sprintf("https://%s-%d.example.test", hostname, p.Port))
			}
		}
	}
	return out, nil
}
func (b *Backend) Containers(ctx context.Context, hostname string) ([]infra.Container, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "Containers"); err != nil {
		return nil, err
	}
	r, err := b.get(hostname)
	if err != nil {
		return nil, err
	}
	out := []infra.Container{}
	if r.service.Kind == infra.KindRuntime && r.service.Status == infra.StatusActive {
		for i := range r.service.MinContainers {
			out = append(out, infra.Container{ID: fmt.Sprintf("%s-%d", r.service.ID, i+1), Hostname: hostname, Status: infra.ContainerActive, PlacementGroup: r.placementGroup})
		}
	}
	return out, nil
}

// SetPlacementGroup sets the authoritative placement returned for a service's
// containers. An empty group models unavailable placement observations.
func (b *Backend) SetPlacementGroup(hostname, group string) error {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	r, err := b.get(hostname)
	if err != nil {
		return err
	}
	r.placementGroup = group
	return nil
}

// AddLog appends recorded log lines for a service.
func (b *Backend) AddLog(hostname string, lines ...infra.LogLine) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if r := b.state.services[hostname]; r != nil {
		r.logs = append(r.logs, lines...)
	}
}
func (b *Backend) Logs(ctx context.Context, hostname string, query infra.LogQuery) ([]infra.LogLine, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "Logs"); err != nil {
		return nil, err
	}
	if !b.Capabilities().Logs {
		return nil, infra.ErrUnsupported
	}
	r, err := b.get(hostname)
	if err != nil {
		return nil, err
	}
	out := []infra.LogLine{}
	for _, line := range r.logs {
		if !query.Since.IsZero() && line.Timestamp.Before(query.Since) || query.Container != "" && line.Container != query.Container {
			continue
		}
		out = append(out, line)
	}
	if query.Limit > 0 && len(out) > query.Limit {
		out = out[len(out)-query.Limit:]
	}
	return out, nil
}
func (b *Backend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	if err := b.enter(ctx, "OpStatus"); err != nil {
		return infra.OpState{}, err
	}
	if op.ID == "" {
		return infra.OpState{Done: true}, nil
	}
	state := b.state.ops[op.ID]
	if state == nil {
		return infra.OpState{Done: true, Expired: true, Failed: op.Kind != "import"}, nil
	}
	if state.remaining > 0 {
		state.remaining--
		return infra.OpState{Status: infra.StatusCreating, ServiceIDs: slices.Clone(state.ids)}, nil
	}
	for _, change := range state.statuses {
		change.record.service.Status = change.status
	}
	state.statuses = nil
	return infra.OpState{Done: true, Status: infra.StatusActive, ServiceIDs: slices.Clone(state.ids)}, nil
}
