// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string)    { l.mu.Lock(); defer l.mu.Unlock(); l.calls = append(l.calls, call) }
func (l *callLog) snapshot() []string { l.mu.Lock(); defer l.mu.Unlock(); return slices.Clone(l.calls) }
func (l *callLog) clear()             { l.mu.Lock(); defer l.mu.Unlock(); l.calls = nil }

type fakeInfra struct {
	services     map[string]*infra.Service
	done, failed bool
	mutations    int
	imports      [][]infra.ServiceSpec
	varWrites    map[string]map[string]string
	deployments  []infra.Deployment
	publicPorts  []infra.PublicPortRoute
	subdomains   map[string]bool
	failures     map[string]error
	states       map[string]infra.OpState
	log          *callLog
	agents       *fakeAgents
	capabilities *infra.Capabilities
}

var _ infra.Backend = (*fakeInfra)(nil)
var _ infra.SubdomainAccess = (*fakeInfra)(nil)

func (f *fakeInfra) Name() string { return "test" }
func (f *fakeInfra) Capabilities() infra.Capabilities {
	if f.capabilities != nil {
		return *f.capabilities
	}
	return infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Logs: true, ScopedTokens: true, DedicatedCPU: true, HorizontalScaling: true}
}
func (f *fakeInfra) Layout() infra.Layout {
	return infra.Layout{AppDir: filepath.Join("/", "var", "www"), VolumeRoot: filepath.Join("/", "mnt"), User: "zerops"}
}
func (f *fakeInfra) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: "project", PublicIPv6: "2001:db8::1", PublicIPv4: "192.0.2.1"}, nil
}
func (f *fakeInfra) op(action, name string) (infra.Op, error) {
	f.log.add("infra:" + name + ":" + action)
	if err := f.failures[action+":"+name]; err != nil {
		return infra.Op{}, err
	}
	if err := f.failures[action]; err != nil {
		return infra.Op{}, err
	}
	f.mutations++
	return infra.Op{ID: fmt.Sprintf("%s-%d", action, f.mutations), Kind: action, ServiceID: name}, nil
}
func (f *fakeInfra) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	op, err := f.op("import", specs[0].Hostname)
	if err != nil {
		return op, err
	}
	f.imports = append(f.imports, slices.Clone(specs))
	op.Services = make(map[string]string, len(specs))
	for _, s := range specs {
		ownership := make(map[string]string, len(s.Env)+len(s.Vars))
		maps.Copy(ownership, s.Env)
		maps.Copy(ownership, s.Vars)
		f.services[s.Hostname] = &infra.Service{ID: s.Hostname, Hostname: s.Hostname, Kind: s.Kind, Status: infra.StatusActive, Resources: s.Resources, Ownership: ownership}
		op.Services[s.Hostname] = s.Hostname
	}
	return op, nil
}
func (f *fakeInfra) GetService(_ context.Context, name string) (*infra.Service, error) {
	return f.services[name], nil
}
func (f *fakeInfra) ListServices(context.Context, string) ([]infra.Service, error) {
	out := []infra.Service{}
	for _, s := range f.services {
		out = append(out, *s)
	}
	return out, nil
}
func (f *fakeInfra) DeleteService(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("delete", name)
	if err == nil {
		delete(f.services, name)
	}
	return op, err
}
func (f *fakeInfra) DeleteServiceByID(ctx context.Context, name, expectedID string) (infra.Op, error) {
	service := f.services[name]
	if service == nil {
		return infra.Op{}, nil
	}
	if service.ID != expectedID {
		return infra.Op{}, infra.ErrConflict
	}
	return f.DeleteService(ctx, name)
}
func (f *fakeInfra) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if d.Artifact.Reader == nil {
		return infra.Op{}, errors.New("missing artifact")
	}
	if _, err := io.ReadAll(d.Artifact.Reader); err != nil {
		return infra.Op{}, err
	}
	op, err := f.op("deploy", name)
	if err != nil {
		return op, err
	}
	d.Artifact.Reader = nil
	f.deployments = append(f.deployments, d)
	if f.agents != nil {
		f.agents.redeploy(name)
	}
	return op, nil
}
func (f *fakeInfra) SetEnvSecrets(context.Context, string, map[string]string) error { return nil }
func (f *fakeInfra) SetEnvVars(_ context.Context, name string, vars map[string]string) error {
	if err := f.failures["vars:"+name]; err != nil {
		return err
	}
	service := f.services[name]
	if service == nil {
		return engine.ErrNotFound
	}
	if service.Ownership == nil {
		service.Ownership = make(map[string]string)
	}
	maps.Copy(service.Ownership, vars)
	if f.varWrites == nil {
		f.varWrites = make(map[string]map[string]string)
	}
	f.varWrites[name] = maps.Clone(vars)
	return nil
}
func (f *fakeInfra) Start(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("start", name)
	if err == nil {
		f.agents.redeploy(name)
	}
	return op, err
}
func (f *fakeInfra) Stop(_ context.Context, name string) (infra.Op, error) { return f.op("stop", name) }
func (f *fakeInfra) Restart(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("restart", name)
	if err == nil {
		f.agents.redeploy(name)
	}
	return op, err
}
func (f *fakeInfra) SetResources(_ context.Context, name string, r infra.Resources) (infra.Op, error) {
	op, err := f.op("resources", name)
	if err == nil && f.services[name] != nil {
		f.services[name].Resources = r
	}
	return op, err
}
func (f *fakeInfra) SetContainerCount(_ context.Context, name string, _, _ int) (infra.Op, error) {
	return f.op("containers", name)
}
func (f *fakeInfra) EnsurePublicPort(_ context.Context, name string, p infra.PublicPort) error {
	f.log.add(fmt.Sprintf("infra:%s:expose:%d", name, p.Port))
	for i, r := range f.publicPorts {
		if r.Hostname == name && r.IPType == p.IPType {
			f.publicPorts[i] = infra.PublicPortRoute{Hostname: name, PublicPort: p}
			return nil
		}
	}
	f.publicPorts = append(f.publicPorts, infra.PublicPortRoute{Hostname: name, PublicPort: p})
	return nil
}
func (f *fakeInfra) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return slices.Clone(f.publicPorts), nil
}
func (f *fakeInfra) DeletePublicPorts(_ context.Context, name string) error {
	f.log.add("infra:" + name + ":unexpose")
	f.publicPorts = slices.DeleteFunc(f.publicPorts, func(r infra.PublicPortRoute) bool { return r.Hostname == name })
	return nil
}
func (f *fakeInfra) Containers(context.Context, string) ([]infra.Container, error) {
	return []infra.Container{{Status: infra.ContainerActive}}, nil
}
func (f *fakeInfra) Logs(context.Context, string, infra.LogQuery) ([]infra.LogLine, error) {
	return nil, nil
}
func (f *fakeInfra) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	if s, ok := f.states[op.ID]; ok {
		return s, nil
	}
	return infra.OpState{Done: f.done, Failed: f.failed, Message: "scripted platform failure"}, nil
}
func (f *fakeInfra) SetSubdomainAccess(_ context.Context, name string, enabled bool) (infra.Op, error) {
	op, err := f.op("subdomain", name)
	if err == nil {
		f.subdomains[name] = enabled
	}
	return op, err
}
func (f *fakeInfra) SubdomainURLs(_ context.Context, name string) ([]string, error) {
	if !f.subdomains[name] {
		return nil, nil
	}
	return []string{"https://" + name + ".example.com"}, nil
}

type fakeArtifact struct{}

func (fakeArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("test-agent-artifact")), nil
}

type testClock struct{ at time.Time }

func (c *testClock) Now() time.Time { return c.at }
