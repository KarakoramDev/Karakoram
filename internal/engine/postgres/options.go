// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"io"
	"strings"
)

// EngineOptions contains the safe user-controlled PostgreSQL settings.
type EngineOptions struct {
	SynchronousMode string            `json:"synchronousMode,omitempty"`
	Parameters      map[string]string `json:"parameters,omitempty"`
	Proxy           ProxyOptions      `json:"proxy,omitempty"`
}
type ProxyOptions struct {
	ReadWriteSplit bool `json:"readWriteSplit,omitempty"`
	ReaderPort     bool `json:"readerPort,omitempty"`
}

var allowedParameters = map[string]bool{
	"max_slot_wal_keep_size":    true,
	"max_prepared_transactions": true, "max_locks_per_transaction": true, "max_wal_senders": true,
	"max_connections": true, "shared_buffers": true, "effective_cache_size": true, "work_mem": true, "maintenance_work_mem": true, "temp_buffers": true, "wal_buffers": true, "min_wal_size": true, "max_wal_size": true, "checkpoint_completion_target": true, "checkpoint_timeout": true, "random_page_cost": true, "effective_io_concurrency": true, "default_statistics_target": true, "max_worker_processes": true, "max_parallel_workers": true, "max_parallel_workers_per_gather": true, "max_parallel_maintenance_workers": true, "log_min_duration_statement": true, "log_statement": true, "log_lock_waits": true, "deadlock_timeout": true, "statement_timeout": true, "lock_timeout": true, "idle_in_transaction_session_timeout": true, "idle_session_timeout": true, "timezone": true, "jit": true, "track_io_timing": true, "track_functions": true, "autovacuum": true, "autovacuum_max_workers": true, "autovacuum_naptime": true, "autovacuum_vacuum_scale_factor": true, "autovacuum_analyze_scale_factor": true, "autovacuum_vacuum_cost_limit": true, "autovacuum_work_mem": true,
}

// ParseOptions rejects unknown fields and parameters that escape engine ownership.
func ParseOptions(raw json.RawMessage) (EngineOptions, error) {
	o := EngineOptions{SynchronousMode: "off"}
	if len(raw) == 0 {
		return o, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return o, engine.Invalid("invalid PostgreSQL engineOptions: %v", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return o, engine.Invalid("engineOptions must contain one JSON object")
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return o, engine.Invalid("engineOptions must be an object")
	}
	if o.SynchronousMode != "off" && o.SynchronousMode != "on" && o.SynchronousMode != "strict" {
		return o, engine.Invalid("synchronousMode must be off, on or strict")
	}
	for k, v := range o.Parameters {
		if !allowedParameters[k] {
			return o, engine.Invalid("PostgreSQL parameter %s is protected or unsupported", k)
		}
		if strings.ContainsAny(v, "\x00\r\n") || len(v) > 1024 {
			return o, engine.Invalid("invalid value for PostgreSQL parameter %s", k)
		}
	}
	return o, nil
}
