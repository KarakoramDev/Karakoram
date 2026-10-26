// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func importDigest(s infra.ServiceSpec) string {
	data, _ := json.Marshal(s)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (b *Backend) remember(hostname, id string) {
	b.state.knownMu.Lock()
	defer b.state.knownMu.Unlock()
	if id == "" {
		delete(b.state.known, hostname)
	} else {
		b.state.known[hostname] = id
	}
}
func (b *Backend) remembered(hostname string) string {
	b.state.knownMu.Lock()
	defer b.state.knownMu.Unlock()
	return b.state.known[hostname]
}

// Called under target mutation exclusion after exact-ID deletion completes.
func (b *Backend) forgetDeleted(hostname, id string) {
	b.state.knownMu.Lock()
	defer b.state.knownMu.Unlock()
	if current := b.state.known[hostname]; current == "" || current == id {
		delete(b.state.known, hostname)
		delete(b.state.imports, hostname)
	}
}

// Search is an independent database lookup, not a negative environment-list
// cache. Exact name, environment and ownership are still checked by the caller.
func (b *Backend) searchApplications(ctx context.Context, hostname string) ([]application, error) {
	var matches []application
	for offset := 0; ; {
		var page struct {
			Items []application `json:"items"`
			Total *int          `json:"total"`
		}
		err := b.c.get(ctx, "application.search", map[string]string{"name": hostname, "environmentId": b.target.ID, "limit": "100", "offset": strconv.Itoa(offset)}, &page)
		if err != nil {
			return nil, err
		}
		if page.Total == nil || *page.Total < 0 || len(page.Items) == 0 && offset < *page.Total {
			return nil, silentFailure("application search is incomplete; refusing to infer absence")
		}
		for _, a := range page.Items {
			if (hostname == "" || a.Name == hostname) && a.Environment == b.target.ID {
				matches = append(matches, a)
			}
		}
		offset += len(page.Items)
		if offset >= *page.Total {
			return matches, nil
		}
	}
}

func (b *Backend) importCandidate(ctx context.Context, s infra.ServiceSpec) (*application, error) {
	if id := b.remembered(s.Hostname); id != "" {
		var a application
		err := b.c.get(ctx, "application.one", map[string]string{"applicationId": id}, &a)
		if err == nil {
			if a.Name != s.Hostname || a.Environment != b.target.ID {
				return nil, restoreConflict("remembered application identity changed; refusing import")
			}
			return &a, nil
		}
		if !errors.Is(err, infra.ErrNotFound) {
			return nil, err
		}
		b.remember(s.Hostname, "")
	}
	matches, err := b.searchApplications(ctx, s.Hostname)
	if err != nil {
		return nil, err
	}
	if len(matches) > 1 {
		return nil, restoreConflict("multiple applications claim the requested hostname; inspect and resolve duplicate ownership before importing")
	}
	if len(matches) == 0 {
		return nil, nil
	}
	var a application
	if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": matches[0].ID}, &a); err != nil {
		return nil, err
	}
	b.remember(s.Hostname, a.ID)
	return &a, nil
}

func (b *Backend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (op infra.Op, err error) {
	if err = b.mu.Lock(ctx); err != nil {
		return op, err
	}
	defer b.mu.Unlock()
	ids := map[string]string{}
	defer func() {
		if len(ids) > 0 && op.ID == "" {
			op = encodeOp(operation{Action: "import", Services: ids})
		}
	}()
	seen := map[string]bool{}
	for _, s := range specs {
		if !safeHostname(s.Hostname) {
			return op, reject("service hostname must be a DNS label")
		}
		if seen[s.Hostname] {
			return op, infra.ErrConflict
		}
		seen[s.Hostname] = true
		if s.Kind != infra.KindRuntime && s.Kind != infra.KindVolume {
			return op, reject("unknown service kind")
		}
		if s.Kind == infra.KindRuntime && s.Base != infra.BaseUbuntu2404 {
			return op, reject("unsupported runtime base")
		}
		if err = resourceValidation(s.Resources); err != nil {
			return op, err
		}
		if _, err = encodeEnvironment(s.Env); err != nil {
			return op, err
		}
		if _, err = encodeEnvironment(s.Vars); err != nil {
			return op, err
		}
		for k := range s.Env {
			if _, ok := s.Vars[k]; ok {
				return op, reject("variable cannot be both secret and readable")
			}
		}
	}
	e, err := b.environment(ctx)
	if err != nil {
		return op, err
	}
	if err = b.ensureNetwork(ctx); err != nil {
		return op, err
	}
	existing := map[string]application{}
	ms := []metadata{}
	for _, a := range e.Applications {
		if m, ok := parseMetadata(a.Description); ok {
			if old, exists := existing[m.Hostname]; exists && old.ID != a.ID {
				return op, restoreConflict("multiple applications claim a hostname; refusing import")
			}
			existing[m.Hostname] = a
			ms = append(ms, m)
		}
	}
	// Check every requested identity before creating anything. Completed entries
	// may be reused only to finish a partially published, identical import batch.
	candidates := map[string]*application{}
	resume := false
	for _, s := range specs {
		a, err := b.importCandidate(ctx, s)
		if err != nil {
			return op, err
		}
		candidates[s.Hostname] = a
		if a == nil {
			resume = true
			continue
		}
		m, ok := parseMetadata(a.Description)
		if !ok || m.Environment != b.target.ID || m.Hostname != s.Hostname || m.Kind != s.Kind || m.ImportDigest != importDigest(s) {
			return op, restoreConflict("application already exists without this import's ownership receipt; refusing adoption")
		}
		ids[s.Hostname] = a.ID
		existing[s.Hostname] = *a
		if m.Importing {
			resume = true
		}
	}
	if !resume && len(specs) > 0 {
		return op, restoreConflict("matching import already completed; observe its returned service identities instead of recreating resources")
	}
	ordered := append([]infra.ServiceSpec(nil), specs...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Kind == infra.KindVolume && ordered[j].Kind != infra.KindVolume })
	var pending []struct {
		app  application
		meta metadata
	}
	for _, s := range ordered {
		var a application
		var m metadata
		if candidate := candidates[s.Hostname]; candidate != nil {
			a = *candidate
			m, _ = parseMetadata(a.Description)
			if !m.Importing {
				continue
			}
		} else {
			if claim := b.state.imports[s.Hostname]; claim != "" {
				return op, restoreConflict("application creation outcome remains unknown; inspect the marked import receipt before retrying; refusing duplicate creation")
			}
			m = metadata{Kind: s.Kind, Hostname: s.Hostname, Environment: b.target.ID, ImportDigest: importDigest(s), Importing: true, Volume: s.Volume, SpreadGroup: s.SpreadGroup, Resources: s.Resources, Min: s.MinContainers, Max: s.MaxContainers, Vars: map[string]string{}}
			if m.Min == 0 {
				m.Min = 1
			}
			if m.Max == 0 {
				m.Max = 1
			}
			if m.Min < 1 || m.Max < m.Min {
				return op, reject("invalid container bounds")
			}
			if m.Volume != "" && m.Max != 1 {
				return op, reject("a volume runtime requires one replica")
			}
			for k, v := range s.Vars {
				m.Vars[k] = v
				m.VarKeys = append(m.VarKeys, k)
			}
			for k := range s.Env {
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
					return op, err
				}
				m.Node, err = chooseNode(ns, ms, m.SpreadGroup, s.SpreadReplaces)
				if err != nil {
					return op, err
				}
			} else if m.Volume != "" {
				v, ok := existing[m.Volume]
				if !ok {
					return op, reject("runtime references an absent volume")
				}
				vm, ok := parseMetadata(v.Description)
				if !ok || vm.Kind != infra.KindVolume {
					return op, reject("runtime volume is not a volume service")
				}
				m.Node = vm.Node
				m.VolumeID = v.ID
				m.VolumeOwner = vm.Vars["KARAKORAM_OWNER"]
			}
			// Publish a non-serving ownership/spec receipt in the create itself.
			// Never roll it back on an ambiguous reply: it is the recovery identity.
			b.state.imports[s.Hostname] = m.ImportDigest
			err = b.c.post(ctx, "application.create", map[string]any{"name": s.Hostname, "appName": s.Hostname, "description": m.description(), "environmentId": b.target.ID, "sourceType": "docker"}, &a)
			if err != nil {
				var api *APIError
				if errors.As(err, &api) && api.StatusCode == 429 {
					delete(b.state.imports, s.Hostname)
					return op, err
				}
				if errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrUnauthorized) {
					delete(b.state.imports, s.Hostname)
					return op, err
				}
				recovered, lookupErr := b.importCandidate(ctx, s)
				if lookupErr != nil {
					return op, errors.Join(err, lookupErr)
				}
				if recovered == nil {
					return op, err
				}
				rm, ok := parseMetadata(recovered.Description)
				if !ok || rm.ImportDigest != m.ImportDigest || rm.Environment != b.target.ID || !rm.Importing {
					return op, restoreConflict("creation reply was lost and its ownership receipt cannot be verified; refusing adoption")
				}
				a = *recovered
				m = rm
			}
			if a.ID == "" || a.AppName == "" {
				return op, silentFailure("application creation returned no identity; inspect the marked import receipt before retrying")
			}
			b.remember(s.Hostname, a.ID)
		}
		ids[s.Hostname] = a.ID
		existing[s.Hostname] = a
		ms = append(ms, m)
		settings := resourceSettings(s.Resources)
		settings["dockerImage"] = baseImage
		settings["replicas"] = m.Max
		if m.Node != "" {
			settings["placementSwarm"] = map[string]any{"Constraints": []string{"node.hostname==" + m.Node}}
		}
		if err = b.update(ctx, &a, m, settings); err != nil {
			return op, err
		}
		all := map[string]string{}
		for k, v := range s.Env {
			all[k] = v
		}
		for k, v := range s.Vars {
			all[k] = v
		}
		if err = b.saveEnv(ctx, a.ID, all); err != nil {
			return op, err
		}
		if m.Kind == infra.KindVolume {
			err = b.ensureImportMount(ctx, &a, a.AppName, "/data")
		} else if m.Volume != "" {
			v := existing[m.Volume]
			if v.AppName == "" {
				if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": v.ID}, &v); err != nil {
					return op, err
				}
				existing[m.Volume] = v
			}
			err = b.ensureImportMount(ctx, &a, v.AppName, b.Layout().VolumePath(m.Volume))
		}
		if err != nil {
			return op, err
		}
		a.Description = m.description()
		existing[s.Hostname] = a
		pending = append(pending, struct {
			app  application
			meta metadata
		}{a, m})
	}
	for _, p := range pending {
		p.meta.Importing = false
		if err = b.update(ctx, &p.app, p.meta, nil); err != nil {
			return op, err
		}
		delete(b.state.imports, p.meta.Hostname)
	}
	if len(ids) == 0 {
		return infra.Op{}, nil
	}
	return encodeOp(operation{Action: "import", Services: ids}), nil
}

func (b *Backend) ensureImportMount(ctx context.Context, a *application, volume, path string) error {
	var full application
	if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full); err != nil {
		return err
	}
	for _, m := range full.Mounts {
		if m.Type == "volume" && m.VolumeName == volume && m.MountPath == path {
			return nil
		}
		if m.MountPath == path {
			return restoreConflict("import mount path is already owned by another volume; refusing overwrite")
		}
	}
	return b.c.post(ctx, "mounts.create", map[string]any{"type": "volume", "volumeName": volume, "mountPath": path, "serviceType": "application", "serviceId": a.ID}, nil)
}
