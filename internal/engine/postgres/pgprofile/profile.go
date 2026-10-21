// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pgprofile is the shared availability and curated SQL authority for
// PostgreSQL profiles. Descriptors own their data; callers receive fresh copies.
package pgprofile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/jackc/pgx/v5"
)

type Extension struct {
	Name    string `json:"name"`
	Source  string `json:"source"`
	Preload bool   `json:"preload"`
	Order   int    `json:"order"`
}
type UpgradeRules struct {
	InstallBothMajors bool
	UpdateExtensions  bool
}
type Descriptor struct{ id string }

var ids = []string{"supabase", "postgis", "pgvector", "timescaledb"}

func Get(id string) (Descriptor, error) {
	if !slices.Contains(ids, id) {
		return Descriptor{}, fmt.Errorf("unknown PostgreSQL profile %q", id)
	}
	return Descriptor{id: id}, nil
}
func IDs() []string             { return slices.Clone(ids) }
func (d Descriptor) ID() string { return d.id }
func (d Descriptor) Majors() []int {
	if d.id == "supabase" {
		return []int{17}
	}
	return []int{16, 17, 18}
}
func (d Descriptor) Supports(major int) bool    { return slices.Contains(d.Majors(), major) }
func (d Descriptor) UpgradeRules() UpgradeRules { return UpgradeRules{true, true} }
func (d Descriptor) Artifacts(major int) []string {
	switch d.id {
	case "supabase":
		return []string{"supabase-pg17-extensions"}
	case "timescaledb":
		return []string{fmt.Sprintf("timescaledb-pg%d", major)}
	}
	return nil
}

// Pins are from the Percona ppg-{16,17,18} noble binary-amd64 indexes.
// Supabase retains its historical PG17 package slice, including its order.
func (d Descriptor) Packages(major int) []string {
	switch d.id {
	case "supabase":
		return []string{"ca-certificates", "curl", "xz-utils", "percona-postgresql-17-cron='1:1.6.8-1.noble'", "percona-postgresql-17-pgaudit='1:17.1-4.noble'", "percona-postgresql-17-pgvector='1:0.8.6-1.noble'", "percona-postgresql-17-postgis-3='2:3.5.7-2.noble'", "percona-postgresql-17-repack='1:1.5.3-5.noble'", "percona-pg-stat-monitor17='1:2.4.0-1.noble'", "percona-pgaudit17-set-user='1:4.2.0-6.noble'", "percona-postgresql-17-wal2json='1:2.6-4.noble'"}
	case "pgvector":
		return []string{fmt.Sprintf("percona-postgresql-%d-pgvector='1:0.8.6-1.noble'", major)}
	case "postgis":
		pin := "2:3.5.7-2.noble"
		if major == 18 {
			pin = "2:3.5.7-3.noble"
		}
		return []string{fmt.Sprintf("percona-postgresql-%d-postgis-3='%s'", major, pin), fmt.Sprintf("percona-postgresql-%d-postgis-3-scripts='%s'", major, pin)}
	}
	return nil
}
func (d Descriptor) Parameters() map[string]string {
	switch d.id {
	case "supabase":
		return map[string]string{"wal_level": "logical", "max_replication_slots": "32", "max_wal_senders": "32", "cron.database_name": "postgres", "output_plugin_libraries": "pgoutput, test_decoding, wal2json"}
	case "timescaledb":
		return map[string]string{"timescaledb.telemetry_level": "off"}
	}
	return nil
}
func (d Descriptor) BasePreloads() []string {
	switch d.id {
	case "supabase":
		return []string{"pg_stat_statements"}
	case "timescaledb":
		return []string{"timescaledb"}
	}
	return nil
}
func SelectedExtensionPreloads(selected []string, bundle nativeartifacts.Artifact) []string {
	var out []string
	for _, name := range selected {
		switch name {
		case "pg_cron", "pgaudit", "pg_stat_monitor":
		default:
			if !slices.Contains(bundle.Extensions, name) || !bundle.ExtensionFlags[name].Preload {
				continue
			}
		}
		if !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	return out
}
func (d Descriptor) Preloads(selected []string) []string {
	out := d.BasePreloads()
	if d.id == "supabase" {
		a, _ := nativeartifacts.Get("supabase-pg17-extensions")
		for _, name := range SelectedExtensionPreloads(selected, a) {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out
}
func (d Descriptor) Extensions(major int) []Extension {
	var names []string
	source := "Percona"
	switch d.id {
	case "supabase":
		names = strings.Fields("pgcrypto uuid-ossp pg_stat_statements pg_cron pgaudit vector postgis pg_repack pg_stat_monitor set_user hstore citext btree_gist btree_gin unaccent fuzzystrmatch")
	case "postgis":
		names = []string{"postgis", "postgis_topology", "postgis_raster"}
	case "pgvector":
		names = []string{"vector"}
	case "timescaledb":
		names = []string{"timescaledb"}
		source = fmt.Sprintf("timescaledb-pg%d", major)
	}
	out := make([]Extension, 0, len(names))
	for _, name := range names {
		out = append(out, Extension{name, source, name == "timescaledb" || name == "pg_cron" || name == "pgaudit" || name == "pg_stat_monitor", len(out)})
	}
	if d.id == "supabase" {
		if a, err := nativeartifacts.Get("supabase-pg17-extensions"); err == nil {
			for _, name := range a.Extensions {
				out = append(out, Extension{name, a.Name, a.ExtensionFlags[name].Preload, len(out)})
			}
		}
	}
	return out
}
func (d Descriptor) ExtensionNames(major int) []string {
	var out []string
	for _, e := range d.Extensions(major) {
		out = append(out, e.Name)
	}
	return out
}
func (d Descriptor) ReservedRole(name string) bool {
	if d.id != "supabase" {
		return false
	}
	if strings.HasPrefix(name, "supabase_") || strings.HasPrefix(name, "kk_") || strings.HasPrefix(name, "pg_") {
		return true
	}
	return slices.Contains([]string{"postgres", "authenticator", "anon", "authenticated", "service_role", "dashboard_user", "pgbouncer", "supavisor"}, name)
}
func (d Descriptor) ReservedSchemas() []string {
	if d.id == "supabase" {
		return strings.Fields("_karakoram extensions auth storage realtime _realtime graphql_public pgbouncer _supavisor _analytics supabase_functions")
	}
	return []string{"_karakoram"}
}
func (d Descriptor) Validate(major int) error {
	if !d.Supports(major) {
		return fmt.Errorf("profile %s does not support PostgreSQL %d", d.id, major)
	}
	for _, name := range d.Artifacts(major) {
		a, err := d.artifact(name, major)
		if err != nil {
			return fmt.Errorf("profile %s: %w", d.id, err)
		}
		if _, err = nativeartifacts.ResolveURL(a); err != nil {
			return fmt.Errorf("profile %s: %w", d.id, err)
		}
	}
	return nil
}
func (d Descriptor) PrepareCommands(layout infra.Layout, major int) ([]string, error) {
	if err := d.Validate(major); err != nil {
		return nil, err
	}
	var commands []string
	if packages := d.Packages(major); len(packages) > 0 {
		commands = append(commands, "sudo apt-get update && sudo apt-get install -y "+strings.Join(packages, " "))
	}
	for _, name := range d.Artifacts(major) {
		a, err := d.artifact(name, major)
		if err != nil {
			return nil, err
		}
		if d.id == "supabase" {
			commands = append(commands, "sudo apt-get install -y libcurl4t64 libsodium23 libssl3t64")
		} else if len(a.RuntimeDeps) > 0 {
			commands = append(commands, "sudo apt-get install -y "+strings.Join(a.RuntimeDeps, " "))
		}
		url, err := nativeartifacts.ResolveURL(a)
		if err != nil {
			return nil, err
		}
		commands = append(commands, nativeartifacts.PrepareCommands(a, url, "/", layout.User)...)
	}
	return commands, nil
}
func (d Descriptor) artifact(name string, major int) (nativeartifacts.Artifact, error) {
	if d.id == "timescaledb" {
		return nativeartifacts.ExtensionArtifact("timescaledb", major)
	}
	return nativeartifacts.Get(name)
}

// Plan is curated, not caller SQL. Supabase retains its existing immutable plan.
func (d Descriptor) Plan(components, selected []string, major int) []initsql.Step {
	if d.id == "supabase" {
		return initsql.Plan(components, selected)
	}
	var out []initsql.Step
	for _, name := range d.ExtensionNames(major) {
		fixedSchema := ""
		if name == "postgis_topology" {
			fixedSchema = "topology"
		}
		out = append(out, initsql.Step{ID: d.id + "-extension-" + name + "-v1", SQL: CreateExtensionSQL(name, fixedSchema) + ";\n"})
	}
	return out
}

// CreateExtensionSQL keeps installation independent of the management search_path.
// fixedSchema is the control-file schema, or empty when the extension has none.
// Fixed-schema extensions select their own namespace; all others belong in public.
func CreateExtensionSQL(name, fixedSchema string) string {
	sql := "CREATE EXTENSION IF NOT EXISTS " + pgx.Identifier{name}.Sanitize()
	if fixedSchema == "" {
		sql += " WITH SCHEMA public"
	}
	return sql + " CASCADE"
}

// Resolve returns profiles in declaration order, irrespective of user ordering or duplicates.
func Resolve(selected []string, major int) ([]Descriptor, error) {
	for _, id := range selected {
		d, err := Get(id)
		if err != nil {
			return nil, err
		}
		if id == "supabase" {
			return nil, fmt.Errorf("profile supabase is implied only by the supabase provider")
		}
		if err = d.Validate(major); err != nil {
			return nil, err
		}
	}
	var out []Descriptor
	for _, id := range ids {
		if slices.Contains(selected, id) {
			d, _ := Get(id)
			out = append(out, d)
		}
	}
	return out, nil
}
