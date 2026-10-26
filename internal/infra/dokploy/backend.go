// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

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
	id := ""
	for _, a := range e.Applications {
		if m, ok := parseMetadata(a.Description); ok && m.Hostname == hostname {
			if id != "" && id != a.ID {
				return nil, restoreConflict("multiple applications claim the requested hostname; refusing ambiguous lookup")
			}
			id = a.ID
		}
	}
	if remembered := b.remembered(hostname); id == "" {
		id = remembered
	}
	if id == "" {
		matches, err := b.searchApplications(ctx, hostname)
		if err != nil {
			return nil, err
		}
		for _, a := range matches {
			if m, ok := parseMetadata(a.Description); ok && m.Hostname == hostname {
				if id != "" && id != a.ID {
					return nil, restoreConflict("multiple applications claim the requested hostname; refusing ambiguous lookup")
				}
				id = a.ID
			}
		}
	}
	if id == "" {
		return nil, nil
	}
	var full application
	if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": id}, &full); err != nil {
		if errors.Is(err, infra.ErrNotFound) {
			b.remember(hostname, "")
			return nil, nil
		}
		return nil, err
	}
	m, ok := parseMetadata(full.Description)
	if !ok || m.Hostname != hostname || full.Name != hostname || full.Environment != b.target.ID || m.Environment != "" && m.Environment != b.target.ID {
		return nil, restoreConflict("application identity or ownership changed; refusing lookup/adoption")
	}
	b.remember(hostname, full.ID)
	return &full, nil
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
	if m.Importing {
		status = infra.StatusCreating
	} else if m.Kind == infra.KindVolume {
		status = infra.StatusActive
	} else if m.Deployed {
		status = infra.StatusCreating
		if a.Status == "error" {
			status = infra.StatusFailed
		}
	}
	s := infra.Service{ID: a.ID, Hostname: m.Hostname, Kind: m.Kind, Status: status, Resources: m.Resources, MinContainers: m.Min, MaxContainers: m.Max, Ownership: map[string]string{}}
	s.Importing = m.Importing
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
	if m.Importing {
		return s, nil
	}
	if m.Kind == infra.KindVolume {
		if err := b.checkPinnedNode(ctx, m.Node); err != nil {
			s.Status = infra.StatusFailed
			return s, err
		}
	}
	if m.Kind != infra.KindRuntime || !m.Deployed && m.Pending == nil {
		return s, nil
	}
	return b.observeRuntime(ctx, a, m)
}
func (b *Backend) GetService(ctx context.Context, hostname string) (*infra.Service, error) {
	if err := b.mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()
	a, err := b.application(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return b.orphanService(ctx, hostname)
	}
	m, _ := parseMetadata(a.Description)
	s, err := b.observedService(ctx, *a, m)
	return &s, err
}
func (b *Backend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	if err := b.mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer b.mu.Unlock()
	e, err := b.environment(ctx)
	if err != nil {
		return nil, err
	}
	candidates := make([]application, 0, len(e.Applications))
	for _, a := range e.Applications {
		if m, ok := parseMetadata(a.Description); ok && strings.HasPrefix(m.Hostname, prefix) {
			candidates = append(candidates, a)
		}
	}
	type result struct {
		application    application
		service        infra.Service
		skip, mutation bool
	}
	results := make([]result, len(candidates))
	if err := infra.ObserveParallel(ctx, len(candidates), func(i int) error {
		a := candidates[i]
		m, _ := parseMetadata(a.Description)
		if m.Kind == infra.KindRuntime && (m.Deployed || m.Pending != nil) {
			var full application
			if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full); err != nil {
				if errors.Is(err, infra.ErrNotFound) {
					results[i].skip = true
					return nil
				}
				return err
			}
			a = full
		}
		m, ok := parseMetadata(a.Description)
		if !ok {
			return restoreConflict("application ownership changed during observation; refusing adoption")
		}
		results[i].application = a
		var err error
		if m.Kind == infra.KindRuntime && (m.Deployed || m.Pending != nil) {
			results[i].service, err = b.observeRuntimeMode(ctx, a, m, true)
		} else {
			results[i].service, err = b.observedService(ctx, a, m)
		}
		if errors.Is(err, errObservationMutation) {
			results[i].mutation = true
			return nil
		}
		return err
	}); err != nil {
		// No mutation is authorized after failed prerequisites; retain every
		// observed error, retry deadline and cancellation from the read batch.
		return nil, err
	}
	out := make([]infra.Service, 0, len(results))
	for _, result := range results {
		if result.skip {
			continue
		}
		if result.mutation {
			mutationCtx := infra.WithoutObservation(ctx)
			// Reload under the existing target lock: an earlier serialized
			// decision may already have advanced this application's journal.
			var current application
			if err := b.c.get(mutationCtx, "application.one", map[string]string{"applicationId": result.application.ID}, &current); err != nil {
				return nil, errors.Join(err, ctx.Err())
			}
			m, ok := parseMetadata(current.Description)
			if !ok {
				return nil, restoreConflict("application ownership changed during observation; refusing adoption")
			}
			service, err := b.observedService(mutationCtx, current, m)
			if err != nil {
				return nil, errors.Join(err, ctx.Err())
			}
			result.service = service
		}
		out = append(out, result.service)
	}
	if err = b.checkOrphans(ctx, prefix, e.Applications); err != nil {
		return out, err
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
	if m.Kind == infra.KindRuntime {
		// Private peer DNS must resolve the task address, not a Swarm VIP:
		// mutually authenticated peers can verify source addresses against DNS.
		// A custom EndpointSpec also overrides Dokploy's port list. Public
		// ingress must use its default VIP endpoint and separate port settings.
		var endpoint any
		if len(m.PublicPorts) == 0 {
			endpoint = map[string]any{"Mode": "dnsrr"}
		}
		settings["endpointSpecSwarm"] = endpoint
	}
	settings["applicationId"] = a.ID
	settings["description"] = m.description()
	return b.c.post(ctx, "application.update", settings, nil)
}
