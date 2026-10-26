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
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type supabaseFlavour struct{ core *postgres.Core }

func (supabaseFlavour) Name() string { return "supabase" }
func (supabaseFlavour) PostgresOptions(raw json.RawMessage) (json.RawMessage, error) {
	return postgresOptions(raw)
}
func (supabaseFlavour) PrepareCommands(l infra.Layout, major int) ([]string, error) {
	if major != 17 {
		return nil, engine.Invalid("Supabase requires PostgreSQL 17")
	}
	packages := []string{
		"ca-certificates", "curl", "xz-utils",
		"percona-postgresql-17-cron='1:1.6.8-1.noble'",
		"percona-postgresql-17-pgaudit='1:17.1-4.noble'",
		"percona-postgresql-17-pgvector='1:0.8.6-1.noble'",
		"percona-postgresql-17-postgis-3='2:3.5.7-2.noble'",
		"percona-postgresql-17-repack='1:1.5.3-5.noble'",
		"percona-pg-stat-monitor17='1:2.4.0-1.noble'",
		"percona-pgaudit17-set-user='1:4.2.0-6.noble'",
		"percona-postgresql-17-wal2json='1:2.6-4.noble'",
	}
	commands := []string{"sudo apt-get update && sudo apt-get install -y " + strings.Join(packages, " ")}
	if a, err := nativeartifacts.Get("supabase-pg17-extensions"); err == nil {
		commands = append(commands, "sudo apt-get install -y libcurl4t64 libsodium23 libssl3t64")
		u, err := nativeartifacts.ResolveURL(a)
		if err != nil {
			return nil, err
		}
		commands = append(commands, nativeartifacts.PrepareCommands(a, u, "/", l.User)...)
	}
	return commands, nil
}
func (supabaseFlavour) Parameters() map[string]string {
	return map[string]string{
		"wal_level": "logical", "max_replication_slots": "32", "max_wal_senders": "32",
		"cron.database_name": "postgres",
		// Percona restricts logical decoding to explicitly trusted output plugins.
		// Realtime requires wal2json in addition to PostgreSQL's default plugins.
		"output_plugin_libraries": "pgoutput, test_decoding, wal2json",
	}
}
func (supabaseFlavour) SharedPreloadLibraries() []string { return []string{"pg_stat_statements"} }
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
	// Restore epochs and release migration additions must reapply the current
	// plan even when component selections and credentials are unchanged.
	hash := configDigest(struct {
		Request    pgspec.SQLBootstrapRequest
		Generation int
		Plan       []initsql.Step
	}{Request: request, Generation: generation, Plan: initsql.Plan(request.Components, request.Extensions)})
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
			return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/bootstrap-digest", []byte(hash))
		}
		client.Close()
	}
	return engine.ErrUnavailable
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
