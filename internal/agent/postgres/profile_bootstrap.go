// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

const profileLockID int64 = 0x4b4b50524f46494c
const profileLedgerSQL = `CREATE SCHEMA IF NOT EXISTS _karakoram;
REVOKE ALL ON SCHEMA _karakoram FROM PUBLIC;
CREATE TABLE IF NOT EXISTS _karakoram.profile_migrations (
 id text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON _karakoram.profile_migrations FROM PUBLIC;`

func profilePlan(s pgspec.Spec, req pgspec.ProfileBootstrapRequest) ([]initsql.Step, error) {
	if s.Role != pgspec.RoleData || s.Flavour == pgspec.FlavourSupabase {
		return nil, errors.New("profile bootstrap requires a plain PostgreSQL data member")
	}
	if len(req.Profiles) == 0 || len(req.Profiles) > 3 {
		return nil, errors.New("invalid profile bootstrap selection")
	}
	var steps []initsql.Step
	seen := map[string]bool{}
	// Canonical order is registry order; duplicate requests are rejected.
	for _, id := range req.Profiles {
		if seen[id] || !slices.Contains(s.Profiles, id) || id == "supabase" {
			return nil, errors.New("profile bootstrap selection is not authorized by member configuration")
		}
		seen[id] = true
		d, err := pgprofile.Get(id)
		if err != nil || !d.Supports(s.Major) {
			return nil, errors.New("profile is unavailable for member major")
		}
	}
	for _, id := range pgprofile.IDs() {
		if seen[id] {
			d, _ := pgprofile.Get(id)
			steps = append(steps, d.Plan(nil, nil, s.Major)...)
		}
	}
	return steps, nil
}
func (e *Runner) profileBootstrapRoute(mux *http.ServeMux, n *agent.Node) {
	mux.HandleFunc("POST /v1/postgres/"+pgspec.ActionProfileBootstrap, func(w http.ResponseWriter, r *http.Request) {
		var req pgspec.ProfileBootstrapRequest
		if err := decodeAction(w, r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
		defer cancel()
		err := n.WithLock(ctx, func(locked *agent.Node) error {
			s, err := decodeSpec(locked.Config())
			if err != nil {
				return err
			}
			steps, err := profilePlan(s, req)
			if err != nil {
				return err
			}
			if s.Topology == pgspec.TopologyHA {
				if err = patroniRequest(ctx, s, http.MethodGet, "/primary", nil, nil); err != nil {
					return errors.New("profile bootstrap requires the Patroni primary with its leader lock")
				}
			}
			c, err := localConnect(ctx, s, dataPaths(locked.DataDir()))
			if err != nil {
				return errors.New("PostgreSQL does not accept local connections")
			}
			defer c.Close(ctx)
			return applyProfilePlan(ctx, c, steps)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		actionJSON(w, http.StatusOK, pgspec.ActionResponse{OK: true})
	})
}

// The only caller supplies profilePlan's registry-derived SQL, never request SQL.
func applyProfilePlan(ctx context.Context, c initsql.Conn, steps []initsql.Step) (result error) {
	if _, err := c.Exec(ctx, "SET search_path = pg_catalog"); err != nil {
		return errors.New("protecting profile migration search path failed")
	}
	var recovery bool
	if err := c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
		return errors.New("checking profile bootstrap primary failed")
	}
	if recovery {
		return errors.New("profile bootstrap cannot run on a replica")
	}
	if _, err := c.Exec(ctx, "SELECT pg_catalog.pg_advisory_lock($1)", profileLockID); err != nil {
		return errors.New("locking profile migrations failed")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.Exec(cleanup, "SELECT pg_catalog.pg_advisory_unlock($1)", profileLockID); err != nil && result == nil {
			result = errors.New("unlocking profile migrations failed")
		}
	}()
	if _, err := c.Exec(ctx, profileLedgerSQL); err != nil {
		return errors.New("initializing profile migration ledger failed")
	}
	for _, step := range steps {
		sum := sha256.Sum256([]byte(step.SQL))
		checksum := hex.EncodeToString(sum[:])
		var previous string
		err := c.QueryRow(ctx, "SELECT checksum FROM _karakoram.profile_migrations WHERE id=$1", step.ID).Scan(&previous)
		if err == nil {
			if previous != checksum {
				return fmt.Errorf("checksum differs for profile migration %s", step.ID)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return errors.New("reading profile migration ledger failed")
		}
		if err = applyProfileStep(ctx, c, step, checksum); err != nil {
			return err
		}
	}
	return nil
}
func applyProfileStep(ctx context.Context, c initsql.Conn, step initsql.Step, checksum string) error {
	if _, err := c.Exec(ctx, "BEGIN"); err != nil {
		return errors.New("starting profile migration failed")
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = c.Exec(cleanup, "ROLLBACK")
		}
	}()
	if _, err := c.Exec(ctx, step.SQL); err != nil {
		return fmt.Errorf("applying profile migration %s failed", step.ID)
	}
	if _, err := c.Exec(ctx, "INSERT INTO _karakoram.profile_migrations(id,checksum) VALUES($1,$2)", step.ID, checksum); err != nil {
		return fmt.Errorf("recording profile migration %s failed", step.ID)
	}
	if _, err := c.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("committing profile migration %s failed", step.ID)
	}
	committed = true
	return nil
}
