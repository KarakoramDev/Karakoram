// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"
)

// The journal is fsynced before fencing. It outlives both a failed cleanup and
// an agent restart; a missing in-memory epoch must not strand an owned fence.
// Values retain the old epoch so recovered fences still get a complete flush.
type proxyEpochRecoveryState struct {
	Pending        map[string]time.Time `json:"pending"`
	StatusRestores []string             `json:"statusRestores,omitempty"`
}

type proxyEpochRecovery struct {
	pending        map[string]time.Time
	statusRestores []string
	save           func([]byte) error
}

func readProxyEpochRecovery(path string, save func([]byte) error) (*proxyEpochRecovery, error) {
	r := &proxyEpochRecovery{pending: map[string]time.Time{}, save: save}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	var state proxyEpochRecoveryState
	if err = json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	r.pending, r.statusRestores = state.Pending, state.StatusRestores
	if r.pending == nil {
		r.pending = map[string]time.Time{}
	}
	return r, nil
}

func (r *proxyEpochRecovery) persist() error {
	data, err := json.Marshal(proxyEpochRecoveryState{Pending: r.pending, StatusRestores: r.statusRestores})
	if err != nil {
		return err
	}
	return r.save(data)
}

func (r *proxyEpochRecovery) begin(host string, old time.Time) error {
	r.pending[host] = old
	if err := r.persist(); err != nil {
		delete(r.pending, host)
		return err
	}
	return nil
}

// Persist the inverse before changing any unrelated server. A failed reset or
// a crash cannot leave configured SHUNNED behind without durable repair intent.
func (r *proxyEpochRecovery) rememberStatusRestore(sql string) error {
	r.statusRestores = append(r.statusRestores, sql)
	if err := r.persist(); err != nil {
		r.statusRestores = r.statusRestores[:len(r.statusRestores)-1]
		return err
	}
	return nil
}

func (r *proxyEpochRecovery) restoreStatuses(ctx context.Context, query proxyEpochQuery) error {
	if len(r.statusRestores) == 0 {
		return nil
	}
	for _, sql := range r.statusRestores {
		if _, err := query(ctx, sql); err != nil {
			return err
		}
	}
	statuses := r.statusRestores
	r.statusRestores = nil
	if err := r.persist(); err != nil {
		r.statusRestores = statuses
		return err
	}
	return nil
}

func (r *proxyEpochRecovery) restore(ctx context.Context, host string, query proxyEpochQuery) error {
	if err := r.restoreStatuses(ctx, query); err != nil {
		return err
	}
	filter := " WHERE hostname=" + sqlQuote(host) + " AND port=5432"
	if _, err := query(ctx, "UPDATE pgsql_servers SET status='ONLINE'"+filter); err != nil {
		return err
	}
	if err := loadProxyEpochServers(ctx, host, query, r); err != nil {
		return err
	}
	old := r.pending[host]
	delete(r.pending, host)
	if err := r.persist(); err != nil {
		r.pending[host] = old
		return err
	}
	return nil
}
