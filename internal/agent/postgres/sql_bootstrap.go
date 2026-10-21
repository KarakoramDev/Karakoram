// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func (e *Runner) sqlBootstrapRoute(mux *http.ServeMux, n *agent.Node) {
	e.profileBootstrapRoute(mux, n)
	mux.HandleFunc("POST /v1/postgres/"+pgspec.ActionSQLBootstrap, func(w http.ResponseWriter, r *http.Request) {
		var req pgspec.SQLBootstrapRequest
		if err := decodeAction(w, r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.Components) > 16 || len(req.Extensions) > 32 || len(req.Passwords) > 64 {
			http.Error(w, "SQL bootstrap request exceeds limits", http.StatusBadRequest)
			return
		}
		for name, password := range req.Passwords {
			if !identifier.MatchString(name) || password == "" || !safeValue(password, 4096) {
				http.Error(w, "invalid bootstrap role credentials", http.StatusBadRequest)
				return
			}
		}
		if err := initsql.Validate(req.Components, req.Extensions, req.Passwords); err != nil {
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
			if s.Role != pgspec.RoleData || s.Flavour != pgspec.FlavourSupabase {
				return errors.New("SQL bootstrap requires a Supabase data member")
			}
			if s.Topology == pgspec.TopologyHA {
				if err = patroniRequest(ctx, s, http.MethodGet, "/primary", nil, nil); err != nil {
					return errors.New("SQL bootstrap requires the Patroni primary with its leader lock")
				}
			}
			c, err := localConnect(ctx, s, dataPaths(locked.DataDir()))
			if err != nil {
				return errors.New("PostgreSQL does not accept local connections")
			}
			defer c.Close(ctx)
			var recovery bool
			if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
				return errors.New("cannot establish PostgreSQL primary state")
			}
			if recovery {
				return errors.New("SQL bootstrap cannot run on a replica")
			}
			return initsql.Apply(ctx, c, initsql.Plan(req.Components, req.Extensions), req.Passwords)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		actionJSON(w, http.StatusOK, pgspec.ActionResponse{OK: true})
	})
}
