// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type supabaseFlavour struct {
	core     *postgres.Core
	provider *Provider
}

var supabaseProfile = func() pgprofile.Descriptor {
	d, _ := pgprofile.Get("supabase")
	return d
}()

func (supabaseFlavour) Name() string { return "supabase" }
func (supabaseFlavour) PostgresOptions(raw json.RawMessage) (json.RawMessage, error) {
	return postgresOptions(raw)
}
func (supabaseFlavour) SelectedExtensions(raw json.RawMessage) ([]string, error) {
	o, err := ParseOptions(raw)
	return o.Extensions, err
}

func (f supabaseFlavour) AdminConsumers(ctx context.Context, rc *engine.Context) (string, error) {
	if _, err := f.provider.syncComponents(ctx, rc); err != nil {
		return "component configuration", err
	}
	state, err := loadState(rc.Cluster)
	if err != nil {
		return "component inventory", err
	}
	for _, def := range stackComponents {
		if rc.Cluster.Spec.Components.Extra[def.Name].Replicas == 0 {
			continue
		}
		m := state.Members[def.Name]
		if m == nil {
			return def.Name, nil
		}
		desired, err := f.provider.desiredSpec(ctx, rc, m)
		if err != nil {
			return def.Name, err
		}
		if m.ConfigHash != desired.RestartToken {
			return def.Name, nil
		}
		status, err := componentStatus(ctx, rc, m)
		if err != nil || !status.Ready {
			return def.Name, err
		}
	}
	return "", nil
}
func (f supabaseFlavour) PostReady(ctx context.Context, rc *engine.Context) error {
	if err := ensureCredentials(ctx, rc); err != nil {
		return err
	}
	credentials, err := loadCredentials(ctx, rc)
	if err != nil {
		return err
	}
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return err
	}
	request := pgspec.SQLBootstrapRequest{
		Components: stackComponents.Enabled(rc.Cluster.Spec.Components),
		Extensions: options.Extensions, Passwords: credentials.Roles,
	}
	members := f.core.DataMembers(rc.Cluster)
	if len(members) == 0 {
		return engine.ErrUnavailable
	}
	config, err := f.core.MemberConfig(ctx, rc, members[0])
	if err != nil {
		return err
	}
	var postgresSpec pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &postgresSpec); err != nil {
		return err
	}
	generation := 0
	if postgresSpec.Bootstrap != nil {
		generation = postgresSpec.Bootstrap.Generation
	}
	// Restore epochs, migration additions, bundle bytes and APT pins all
	// invalidate the receipt: CREATE-only ledgers do not describe SQL upgrades.
	bundles, err := bootstrapBundles()
	if err != nil {
		return err
	}
	hash := bootstrapDigest(request, generation, bundles)
	applied, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/bootstrap-digest")
	if err == nil && string(applied) == hash {
		return nil
	}
	if err != nil && !missing(err) {
		return err
	}
	for _, name := range members {
		client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
		if err != nil {
			continue
		}
		var status pgspec.StatusResponse
		// Writer discovery is an observation; SQL bootstrap below is a
		// mutation and must keep the original administration context.
		statusCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		err = client.Engine(statusCtx, "postgres", "status", struct{}{}, &status)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			client.Close()
			return fmt.Errorf("supabase bootstrap writer-discovery status read exceeded %s budget: inspect member %s responsiveness/connectivity: %w", engine.ObservationTimeout, name, err)
		}
		if err == nil && status.Running && (status.Role == "primary" || status.Role == "standalone") {
			err = client.Engine(ctx, "postgres", pgspec.ActionSQLBootstrap, request, nil)
			client.Close()
			if err != nil {
				return fmt.Errorf("bootstrapping Supabase database: %w", err)
			}
			// Restored catalogs may retain old extension SQL on current binaries.
			// Keep bootstrap incomplete until the per-database update succeeds.
			if err = f.core.UpdateProfileExtensions(ctx, rc); err != nil {
				return err
			}
			return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/bootstrap-digest", []byte(hash))
		}
		client.Close()
	}
	return engine.ErrUnavailable
}

func bootstrapBundles() ([]nativeartifacts.Artifact, error) {
	var bundles []nativeartifacts.Artifact
	for _, name := range supabaseProfile.Artifacts(17) {
		bundle, err := nativeartifacts.Get(name)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

func bootstrapDigest(request pgspec.SQLBootstrapRequest, generation int, bundles []nativeartifacts.Artifact) string {
	return configDigest(struct {
		Request    pgspec.SQLBootstrapRequest
		Generation int
		Plan       []initsql.Step
		Bundles    []nativeartifacts.Artifact
		Packages   []string
	}{request, generation, supabaseProfile.Plan(request.Components, request.Extensions, 17), bundles, supabaseProfile.Packages(17)})
}
func (supabaseFlavour) RelayTargets(c *domain.Cluster) []string {
	var out []string
	for name, code := range codes {
		if c.Spec.Components.Extra[name].Replicas == 0 {
			continue
		}
		host := c.Name + code
		for _, port := range []int{ports[name], 9443} {
			out = append(out, net.JoinHostPort(host, strconv.Itoa(port)))
		}
		if name == "pooler" {
			out = append(out, net.JoinHostPort(host, "6543"))
		}
	}
	return out
}
