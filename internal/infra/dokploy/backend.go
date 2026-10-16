// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (b *Backend) Name() string         { return "dokploy" }
func (b *Backend) Layout() infra.Layout { return layout }
func (b *Backend) Capabilities() infra.Capabilities {
	c := dokployCapabilities
	c.PublicHTTPS = b.target.Config["domainSuffix"] != ""
	return c
}
func (b *Backend) SetToken(token string) { b.c.setToken(token) }
func reject(message string) error        { return &infra.RejectedError{Code: "dokploy", Message: message} }
func (b *Backend) environment(ctx context.Context) (environment, error) {
	var e environment
	err := b.c.get(ctx, "environment.one", map[string]string{"environmentId": b.target.ID}, &e)
	return e, err
}
func (b *Backend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	e, err := b.environment(ctx)
	if err != nil {
		return infra.Project{}, err
	}
	return infra.Project{ID: e.ID, Name: e.Project.Name + " / " + e.Name, PublicIPv4: b.target.Config["publicIPv4"], PublicIPv6: b.target.Config["publicIPv6"]}, nil
}
func (b *Backend) nodes(ctx context.Context) ([]node, error) {
	var ns []node
	err := b.c.get(ctx, "cluster.getNodes", nil, &ns)
	return ns, err
}
func eligibleNodes(ns []node) []node {
	labelled := false
	for _, n := range ns {
		if n.Spec.Labels["karakoram.data"] == "true" {
			labelled = true
			break
		}
	}
	var out []node
	for _, n := range ns {
		if n.Status.State == "ready" && n.Spec.Availability == "active" && (!labelled || n.Spec.Labels["karakoram.data"] == "true") {
			out = append(out, n)
		}
	}
	return out
}
func chooseNode(ns []node, ms []metadata, group, replaces string) (string, error) {
	ns = eligibleNodes(ns)
	if len(ns) == 0 {
		return "", fmt.Errorf("%w: no ready active data nodes", infra.ErrUnavailable)
	}
	counts, total := map[string]int{}, map[string]int{}
	for _, m := range ms {
		if m.Kind == infra.KindVolume {
			total[m.Node]++
			if m.SpreadGroup == group && m.Hostname != replaces {
				counts[m.Node]++
			}
		}
	}
	sort.Slice(ns, func(i, j int) bool {
		a, c := ns[i].Description.Hostname, ns[j].Description.Hostname
		if counts[a] != counts[c] {
			return counts[a] < counts[c]
		}
		if total[a] != total[c] {
			return total[a] < total[c]
		}
		return a < c
	})
	return ns[0].Description.Hostname, nil
}
func (b *Backend) validate(ctx context.Context) (infra.Project, error) {
	p, err := b.ProjectInfo(ctx)
	if err != nil {
		return p, err
	}
	ns, err := b.nodes(ctx)
	if err != nil {
		return p, err
	}
	if len(eligibleNodes(ns)) == 0 {
		return p, reject("Dokploy needs at least one ready active node")
	}
	if len(ns) > 1 && b.target.Config["registryId"] == "" {
		return p, reject("multi-node Dokploy requires registryId so workers can pull deployment images")
	}
	if id := b.target.Config["registryId"]; id != "" {
		if err = b.c.get(ctx, "registry.one", map[string]string{"registryId": id}, nil); err != nil {
			return p, err
		}
	}
	return p, b.ensureNetwork(ctx)
}
func (b *Backend) ensureNetwork(ctx context.Context) error {
	network := b.network()
	var networks []struct {
		Name string `json:"name"`
	}
	if err := b.c.get(ctx, "network.all", nil, &networks); err != nil {
		return err
	}
	// Dokploy's built-in overlay is not represented in network.all.
	if network == "dokploy-network" {
		return nil
	}
	for _, n := range networks {
		if n.Name == network {
			return nil
		}
	}
	err := b.c.post(ctx, "network.create", map[string]any{"name": network, "driver": "overlay", "attachable": true, "enableIPv4": true}, nil)
	if errors.Is(err, infra.ErrRejected) {
		var api *APIError
		if errors.As(err, &api) && strings.Contains(strings.ToLower(api.Message), "already exists") {
			err = nil
		}
	}
	return err
}
func (b *Backend) network() string {
	if n := b.target.Config["network"]; n != "" {
		return n
	}
	return defaultNetworkName(b.target.ID)
}
func (b *Backend) application(ctx context.Context, hostname string) (*application, error) {
	e, err := b.environment(ctx)
	if err != nil {
		return nil, err
	}
	for _, a := range e.Applications {
		m, ok := parseMetadata(a.Description)
		if ok && m.Hostname == hostname {
			var full application
			err = b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full)
			if errors.Is(err, infra.ErrNotFound) {
				return nil, nil
			}
			return &full, err
		}
	}
	return nil, nil
}
func (b *Backend) require(ctx context.Context, hostname string) (*application, metadata, error) {
	a, err := b.application(ctx, hostname)
	if err != nil {
		return nil, metadata{}, err
	}
	if a == nil {
		return nil, metadata{}, infra.ErrNotFound
	}
	m, _ := parseMetadata(a.Description)
	return a, m, nil
}
func observe(a application, m metadata) infra.Service {
	status := infra.StatusReadyToDeploy
	if m.Kind == infra.KindVolume {
		status = infra.StatusActive
	} else if m.Deployed {
		status = infra.StatusCreating
		if a.Status == "error" {
			status = infra.StatusFailed
		}
	}
	s := infra.Service{ID: a.ID, Hostname: m.Hostname, Kind: m.Kind, Status: status, Resources: m.Resources, MinContainers: m.Min, MaxContainers: m.Max, Ownership: map[string]string{}}
	for k, v := range m.Vars {
		if identityKey(k) {
			s.Ownership[k] = v
		}
	}
	for _, k := range m.SecretKeys {
		if identityKey(k) {
			s.Masked = true
		}
	}
	return s
}
func identityKey(k string) bool {
	switch k {
	case "KARAKORAM_OWNER", "KARAKORAM_RELAY_OWNER", "KARAKORAM_CLUSTER", "KARAKORAM_MEMBER", "KARAKORAM_AGENT_TOKEN", "KARAKORAM_DATA_DIR":
		return true
	}
	return false
}
func (b *Backend) observedService(ctx context.Context, a application, m metadata) (infra.Service, error) {
	s := observe(a, m)
	if m.Kind == infra.KindRuntime && m.Deployed && a.Status != "error" {
		ts, err := b.tasks(ctx, a.AppName)
		if err != nil {
			return s, err
		}
		s.Status = infra.StatusStopped
		for _, t := range ts {
			if strings.EqualFold(t.State, "running") {
				s.Status = infra.StatusCreating
				if strings.HasPrefix(strings.ToLower(t.CurrentState), "running") {
					s.Status = infra.StatusRunning
				}
				break
			}
		}
		if s.Status == infra.StatusStopped && a.Status == "running" {
			s.Status = infra.StatusCreating
		}
	}
	return s, nil
}
func (b *Backend) GetService(ctx context.Context, hostname string) (*infra.Service, error) {
	a, err := b.application(ctx, hostname)
	if err != nil || a == nil {
		return nil, err
	}
	m, _ := parseMetadata(a.Description)
	s, err := b.observedService(ctx, *a, m)
	return &s, err
}
func (b *Backend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	e, err := b.environment(ctx)
	if err != nil {
		return nil, err
	}
	out := []infra.Service{}
	for _, a := range e.Applications {
		m, ok := parseMetadata(a.Description)
		if ok && strings.HasPrefix(m.Hostname, prefix) {
			if m.Kind == infra.KindRuntime && m.Deployed {
				var full application
				if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full); err != nil {
					if errors.Is(err, infra.ErrNotFound) {
						continue
					}
					return nil, err
				}
				a = full
			}
			s, err := b.observedService(ctx, a, m)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
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
		return infra.ErrUnsupported
	}
	if r.CPUMode != "" && r.CPUMode != infra.CPUShared {
		return reject("unknown CPU mode")
	}
	if (r.MaxCPU > 0 && r.MinCPU > r.MaxCPU) || (r.MaxRAM > 0 && r.MinRAM > r.MaxRAM) || (r.MaxDisk > 0 && r.MinDisk > r.MaxDisk) {
		return reject("minimum resource bounds exceed maximum")
	}
	return nil
}
func resourceSettings(r infra.Resources) map[string]any {
	m := map[string]any{}
	for k, v := range map[string]float64{"cpuReservation": r.MinCPU * 1e9, "cpuLimit": r.MaxCPU * 1e9, "memoryReservation": r.MinRAM * (1 << 30), "memoryLimit": r.MaxRAM * (1 << 30)} {
		if v > 0 {
			m[k] = strconv.FormatInt(int64(v), 10)
		}
	}
	return m
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
func (b *Backend) update(ctx context.Context, a *application, m metadata, settings map[string]any) error {
	if settings == nil {
		settings = map[string]any{}
	}
	settings["applicationId"] = a.ID
	settings["description"] = m.description()
	return b.c.post(ctx, "application.update", settings, nil)
}
func (b *Backend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (op infra.Op, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var created []string
	defer func() {
		if err == nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for i := len(created) - 1; i >= 0; i-- {
			_ = b.c.post(cleanup, "application.delete", map[string]any{"applicationId": created[i]}, nil)
		}
	}()
	e, err := b.environment(ctx)
	if err != nil {
		return infra.Op{}, err
	}
	if err = b.ensureNetwork(ctx); err != nil {
		return infra.Op{}, err
	}
	existing := map[string]application{}
	ms := []metadata{}
	for _, a := range e.Applications {
		if m, ok := parseMetadata(a.Description); ok {
			existing[m.Hostname] = a
			ms = append(ms, m)
		}
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if !safeHostname(s.Hostname) {
			return infra.Op{}, reject("service hostname must be a DNS label")
		}
		if seen[s.Hostname] || existing[s.Hostname].ID != "" {
			return infra.Op{}, infra.ErrConflict
		}
		seen[s.Hostname] = true
		if s.Kind != infra.KindVolume && s.Kind != infra.KindRuntime {
			return infra.Op{}, reject("unknown service kind")
		}
		if s.Kind == infra.KindRuntime && s.Base != infra.BaseUbuntu2404 {
			return infra.Op{}, reject("unsupported runtime base")
		}
		if err = resourceValidation(s.Resources); err != nil {
			return infra.Op{}, err
		}
		if _, err = encodeEnvironment(s.Env); err != nil {
			return infra.Op{}, err
		}
		if _, err = encodeEnvironment(s.Vars); err != nil {
			return infra.Op{}, err
		}
	}
	// Holders must exist before the runtimes which reference them.
	ordered := append([]infra.ServiceSpec(nil), specs...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Kind == infra.KindVolume && ordered[j].Kind != infra.KindVolume })
	ids := map[string]string{}
	var pending []struct {
		app  application
		meta metadata
	}
	for _, s := range ordered {
		m := metadata{Kind: s.Kind, Hostname: s.Hostname, Volume: s.Volume, SpreadGroup: s.SpreadGroup, Resources: s.Resources, Min: s.MinContainers, Max: s.MaxContainers, Vars: map[string]string{}}
		if m.Min == 0 {
			m.Min = 1
		}
		if m.Max == 0 {
			m.Max = 1
		}
		if m.Min < 1 || m.Max < m.Min {
			return infra.Op{}, reject("invalid container bounds")
		}
		if m.Volume != "" && m.Max != 1 {
			return infra.Op{}, reject("a volume runtime requires one replica")
		}
		for k, v := range s.Vars {
			m.Vars[k] = v
			m.VarKeys = append(m.VarKeys, k)
		}
		for k := range s.Env {
			if _, ok := s.Vars[k]; ok {
				return infra.Op{}, reject("variable cannot be both secret and readable")
			}
			m.SecretKeys = append(m.SecretKeys, k)
		}
		sort.Strings(m.VarKeys)
		sort.Strings(m.SecretKeys)
		if m.Kind == infra.KindVolume {
			if m.SpreadGroup == "" {
				sum := sha256.Sum256([]byte(s.Env["KARAKORAM_CLUSTER"]))
				m.SpreadGroup = hex.EncodeToString(sum[:])
			}
			ns, err := b.nodes(ctx)
			if err != nil {
				return infra.Op{}, err
			}
			m.Node, err = chooseNode(ns, ms, m.SpreadGroup, s.SpreadReplaces)
			if err != nil {
				return infra.Op{}, err
			}
		} else if m.Volume != "" {
			v, ok := existing[m.Volume]
			if !ok {
				return infra.Op{}, reject("runtime references an absent volume")
			}
			if v.AppName == "" {
				var full application
				if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": v.ID}, &full); err != nil {
					return infra.Op{}, err
				}
				v = full
				existing[m.Volume] = full
			}
			vm, ok := parseMetadata(v.Description)
			if !ok || vm.Kind != infra.KindVolume {
				return infra.Op{}, reject("runtime volume is not a volume service")
			}
			m.Node = vm.Node
		}
		var a application
		if err = b.c.post(ctx, "application.create", map[string]any{"name": s.Hostname, "appName": s.Hostname, "description": "Infrastructure import in progress", "environmentId": b.target.ID, "sourceType": "docker"}, &a); err != nil {
			return infra.Op{}, err
		}
		created = append(created, a.ID)
		ids[s.Hostname] = a.ID
		a.Description = m.description()
		existing[s.Hostname] = a
		ms = append(ms, m)
		settings := resourceSettings(s.Resources)
		settings["dockerImage"] = baseImage
		settings["replicas"] = m.Max
		if m.Node != "" {
			settings["placementSwarm"] = map[string]any{"Constraints": []string{"node.hostname==" + m.Node}}
		}
		settings["applicationId"] = a.ID
		if err = b.c.post(ctx, "application.update", settings, nil); err != nil {
			return infra.Op{Services: ids}, err
		}
		all := map[string]string{}
		for k, v := range s.Env {
			all[k] = v
		}
		for k, v := range s.Vars {
			all[k] = v
		}
		if err = b.saveEnv(ctx, a.ID, all); err != nil {
			return infra.Op{Services: ids}, err
		}
		if m.Kind == infra.KindVolume {
			err = b.c.post(ctx, "mounts.create", map[string]any{"type": "volume", "volumeName": a.AppName, "mountPath": "/data", "serviceType": "application", "serviceId": a.ID}, nil)
		} else if m.Volume != "" {
			v := existing[m.Volume]
			err = b.c.post(ctx, "mounts.create", map[string]any{"type": "volume", "volumeName": v.AppName, "mountPath": b.Layout().VolumePath(m.Volume), "serviceType": "application", "serviceId": a.ID}, nil)
		}
		if err != nil {
			return infra.Op{Services: ids}, err
		}
		pending = append(pending, struct {
			app  application
			meta metadata
		}{a, m})
	}
	// Publish ownership metadata only after every mount and environment is ready.
	for _, p := range pending {
		if err = b.update(ctx, &p.app, p.meta, nil); err != nil {
			return infra.Op{Services: ids}, err
		}
	}
	if len(ids) == 0 {
		return infra.Op{}, nil
	}
	return encodeOp(operation{Action: "import", Services: ids}), nil
}
