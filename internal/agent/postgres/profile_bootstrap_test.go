// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestProfilePlanAuthorization(t *testing.T) {
	s := pgspec.Spec{Role: pgspec.RoleData, Flavour: pgspec.FlavourPostgres, Major: 17, Profiles: []string{"postgis", "pgvector"}}
	steps, err := profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: []string{"pgvector", "postgis"}})
	if err != nil || len(steps) != 4 {
		t.Fatal(steps, err)
	}
	if steps[0].SQL != "CREATE EXTENSION IF NOT EXISTS \"postgis\" WITH SCHEMA public CASCADE;\n" || steps[3].SQL != "CREATE EXTENSION IF NOT EXISTS \"vector\" WITH SCHEMA public CASCADE;\n" {
		t.Fatal("plan order changed", steps)
	}
	for _, ids := range [][]string{nil, {"timescaledb"}, {"supabase"}, {"postgis", "postgis"}, {"postgis; DROP SCHEMA public"}} {
		if _, err = profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: ids}); err == nil {
			t.Fatal("unauthorized selection", ids)
		}
	}
	s.Role = pgspec.RoleProxy
	if _, err = profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: []string{"pgvector"}}); err == nil {
		t.Fatal("proxy admitted")
	}
	s.Role = pgspec.RoleData
	s.Flavour = pgspec.FlavourSupabase
	if _, err = profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: []string{"pgvector"}}); err == nil {
		t.Fatal("supabase admitted generic ledger")
	}
	s.Flavour = pgspec.FlavourPostgres
	s.Major = 15
	if _, err = profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: []string{"pgvector"}}); err == nil {
		t.Fatal("unsupported ABI admitted")
	}
}

type profileRow func(...any) error

func (r profileRow) Scan(dst ...any) error { return r(dst...) }

type profileConn struct {
	recovery bool
	ledger   map[string]string
	pending  map[string]string
	commands []string
	failSQL  string
}

func (*profileConn) Config() *pgx.ConnConfig { return nil }
func (c *profileConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return profileRow(func(dst ...any) error {
		if strings.Contains(sql, "pg_is_in_recovery") {
			*dst[0].(*bool) = c.recovery
			return nil
		}
		value, ok := c.ledger[args[0].(string)]
		if !ok {
			return pgx.ErrNoRows
		}
		*dst[0].(*string) = value
		return nil
	})
}
func (c *profileConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.commands = append(c.commands, sql)
	if sql == c.failSQL {
		return pgconn.CommandTag{}, errors.New("secret SQL must never escape")
	}
	switch {
	case sql == "BEGIN":
		c.pending = map[string]string{}
	case sql == "COMMIT":
		for id, sum := range c.pending {
			c.ledger[id] = sum
		}
		c.pending = nil
	case sql == "ROLLBACK":
		c.pending = nil
	case strings.HasPrefix(sql, "INSERT INTO _karakoram.profile_migrations"):
		c.pending[args[0].(string)] = args[1].(string)
	}
	return pgconn.CommandTag{}, nil
}
func TestProfileLedgerReplayAndRollback(t *testing.T) {
	s := pgspec.Spec{Role: pgspec.RoleData, Major: 17, Profiles: []string{"pgvector"}}
	plan, err := profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: s.Profiles})
	if err != nil {
		t.Fatal(err)
	}
	c := &profileConn{ledger: map[string]string{}}
	if err = applyProfilePlan(t.Context(), c, plan); err != nil {
		t.Fatal(err)
	}
	if len(c.ledger) != 1 {
		t.Fatal("missing receipt")
	}
	first := append([]string(nil), c.commands...)
	c.commands = nil
	if err = applyProfilePlan(t.Context(), c, plan); err != nil {
		t.Fatal(err)
	}
	for _, sql := range c.commands {
		if sql == plan[0].SQL || sql == "BEGIN" {
			t.Fatal("replayed migration", sql)
		}
	}
	if first[0] != "SET search_path = pg_catalog" || !strings.Contains(first[len(first)-1], "pg_advisory_unlock") {
		t.Fatal("missing locking/search-path protection", first)
	}
	c.ledger[plan[0].ID] = "mismatch"
	if err = applyProfilePlan(t.Context(), c, plan); err == nil || !strings.Contains(err.Error(), "checksum differs") {
		t.Fatal("changed receipt accepted", err)
	}
	c = &profileConn{ledger: map[string]string{}, failSQL: plan[0].SQL}
	err = applyProfilePlan(t.Context(), c, plan)
	if err == nil || strings.Contains(err.Error(), "secret SQL") || len(c.ledger) != 0 {
		t.Fatal("unsafe migration failure", err, c.ledger)
	}
	if !reflect.DeepEqual(c.commands[len(c.commands)-2:], []string{"ROLLBACK", "SELECT pg_catalog.pg_advisory_unlock($1)"}) {
		t.Fatal("failed transaction not rolled back/unlocked", c.commands)
	}
	c = &profileConn{recovery: true, ledger: map[string]string{}}
	if err = applyProfilePlan(t.Context(), c, plan); err == nil {
		t.Fatal("replica accepted")
	}
}
