// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func profileDescription() map[string]any {
	var entries []any
	for _, id := range pgprofile.IDs() {
		if id == "supabase" {
			continue
		}
		d, _ := pgprofile.Get(id)
		majors := map[string]any{}
		for _, major := range d.Majors() {
			entry := map[string]any{"extensions": d.Extensions(major), "available": true}
			if err := d.Validate(major); err != nil {
				entry["available"] = false
				entry["reason"] = err.Error()
			}
			majors[strconv.Itoa(major)] = entry
		}
		entries = append(entries, map[string]any{"id": id, "majors": majors})
	}
	return map[string]any{"type": "array", "uniqueItems": true, "items": entries}
}

// selectedProfileRequirements snapshots selection without revalidating artifact
// availability: backup provenance must survive later manifest/source changes.
func (p *Core) selectedProfileRequirements(raw json.RawMessage) ([]string, error) {
	if p.options.Flavour != nil {
		// Flavours retain their existing provider-specific runtime contract.
		return nil, nil
	}
	o, err := p.parseOptions(raw)
	if err != nil {
		return nil, err
	}
	return normalizedProfileRequirements(o.Profiles), nil
}

func normalizedProfileRequirements(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

func (p *Core) validateBackupProfiles(b *domain.Backup, target *domain.Cluster) error {
	if b.Postgres == nil || len(b.Postgres.Profiles) == 0 {
		return nil
	}
	selected, err := p.selectedProfileRequirements(target.Spec.EngineOptions)
	if err != nil {
		return err
	}
	for _, id := range normalizedProfileRequirements(b.Postgres.Profiles) {
		if !slices.Contains(selected, id) {
			return engine.Invalid("physical recovery target requires source PostgreSQL profile %q", id)
		}
	}
	return nil
}
func (p *Core) profiles(raw json.RawMessage, major int) ([]pgprofile.Descriptor, error) {
	if p.options.Flavour != nil {
		d, err := pgprofile.Get(p.options.Flavour.Name())
		if err != nil {
			return nil, err
		}
		return []pgprofile.Descriptor{d}, nil
	}
	o, err := p.parseOptions(raw)
	if err != nil {
		return nil, err
	}
	return pgprofile.Resolve(o.Profiles, major)
}
func (p *Core) profileParameters(raw json.RawMessage) (map[string]string, error) {
	// Parameters do not vary by major; avoid capability/URL admission during
	// rolling phase planning, which operates on historical member options.
	o, err := p.parseOptions(raw)
	if err != nil {
		return nil, err
	}
	ids := o.Profiles
	if p.options.Flavour != nil {
		ids = []string{p.options.Flavour.Name()}
	}
	var out map[string]string
	for _, id := range ids {
		d, err := pgprofile.Get(id)
		if err != nil {
			return nil, err
		}
		for k, v := range d.Parameters() {
			if out == nil {
				out = map[string]string{}
			}
			if previous, ok := out[k]; ok && previous != v {
				return nil, fmt.Errorf("profiles conflict on parameter %s", k)
			}
			out[k] = v
		}
	}
	return out, nil
}
func (p *Core) profilePreloads(raw json.RawMessage, major int) ([]string, error) {
	profiles, err := p.profiles(raw, major)
	if err != nil {
		return nil, err
	}
	var selected []string
	if wrapper, ok := p.options.Flavour.(interface {
		SelectedExtensions(json.RawMessage) ([]string, error)
	}); ok {
		selected, err = wrapper.SelectedExtensions(raw)
		if err != nil {
			return nil, err
		}
	}
	var out []string
	for _, d := range profiles {
		for _, name := range d.Preloads(selected) {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out, nil
}
func (p *Core) profileBootstrap(rc *engine.Context, s *state) (pgspec.ProfileBootstrapRequest, string, error) {
	if p.options.Flavour != nil {
		return pgspec.ProfileBootstrapRequest{}, "", nil
	}
	profiles, err := p.profiles(rc.Cluster.Spec.EngineOptions, major(rc.Cluster.Spec.Version))
	if err != nil {
		return pgspec.ProfileBootstrapRequest{}, "", err
	}
	if len(profiles) == 0 {
		return pgspec.ProfileBootstrapRequest{}, "", nil
	}
	var ids []string
	var plan []initsql.Step
	for _, d := range profiles {
		ids = append(ids, d.ID())
		plan = append(plan, d.Plan(nil, nil, major(rc.Cluster.Spec.Version))...)
	}
	request := pgspec.ProfileBootstrapRequest{Profiles: ids}
	sum := sha256.Sum256(mustJSON(struct {
		Request    pgspec.ProfileBootstrapRequest
		Generation int
		Plan       []initsql.Step
	}{request, s.BootstrapGeneration, plan}))
	return request, hex.EncodeToString(sum[:]), nil
}

func (p *Core) profileBootstrapApplied(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	_, hash, err := p.profileBootstrap(rc, s)
	if err != nil || hash == "" {
		return err == nil, err
	}
	applied, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "postgres/profile-bootstrap-digest")
	if missing(err) {
		return false, nil
	}
	return err == nil && string(applied) == hash, err
}

func (p *Core) postReadyProfiles(ctx context.Context, rc *engine.Context) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	request, hash, err := p.profileBootstrap(rc, s)
	if err != nil || hash == "" {
		return err
	}
	applied, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "postgres/profile-bootstrap-digest")
	if err == nil && string(applied) == hash {
		return nil
	}
	if err != nil && !missing(err) {
		return err
	}
	for _, m := range s.data() {
		client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			continue
		}
		var status pgspec.StatusResponse
		err = client.Engine(ctx, "postgres", "status", struct{}{}, &status)
		if err == nil && status.Running && (status.Role == "primary" || status.Role == "standalone") {
			err = client.Engine(ctx, "postgres", pgspec.ActionProfileBootstrap, request, nil)
			client.Close()
			if err != nil {
				return fmt.Errorf("bootstrapping PostgreSQL profiles: %w", err)
			}
			return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "postgres/profile-bootstrap-digest", []byte(hash))
		}
		client.Close()
	}
	return engine.ErrUnavailable
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func (p *Core) profilesAdded(before, after json.RawMessage) bool {
	if p.options.Flavour != nil {
		return false
	}
	old, err := p.parseOptions(before)
	if err != nil {
		return true
	}
	next, err := p.parseOptions(after)
	if err != nil {
		return true
	}
	for _, id := range next.Profiles {
		if !slices.Contains(old.Profiles, id) {
			return true
		}
	}
	return false
}

func (p *Core) profileAvailableExtensions(_ context.Context, rc *engine.Context) ([]string, error) {
	profiles, err := p.profiles(rc.Cluster.Spec.EngineOptions, major(rc.Cluster.Spec.Version))
	if err != nil {
		return nil, engine.Invalid("%v", err)
	}
	var out []string
	if p.options.Flavour == nil {
		out = slices.Clone(contribExtensions)
	}
	for _, profile := range profiles {
		for _, name := range profile.ExtensionNames(major(rc.Cluster.Spec.Version)) {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out, nil
}

func (p *Core) selectedProfileExtensions(rc *engine.Context) []string {
	if wrapper, ok := p.options.Flavour.(interface {
		SelectedExtensions(json.RawMessage) ([]string, error)
	}); ok {
		selected, err := wrapper.SelectedExtensions(rc.Cluster.Spec.EngineOptions)
		if err != nil {
			return nil
		}
		out := []string{"pgcrypto", "uuid-ossp"}
		for _, name := range selected {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
		return out
	}
	profiles, err := p.profiles(rc.Cluster.Spec.EngineOptions, major(rc.Cluster.Spec.Version))
	if err != nil {
		return nil
	}
	var out []string
	for _, profile := range profiles {
		if !profile.UpgradeRules().UpdateExtensions {
			continue
		}
		for _, name := range profile.ExtensionNames(major(rc.Cluster.Spec.Version)) {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out
}
