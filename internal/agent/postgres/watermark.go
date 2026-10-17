// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type walSample struct {
	at  time.Time
	lsn uint64
}

type archiveWatermark struct {
	mu         sync.Mutex
	systemID   string
	timeline   uint64
	samples    []walSample
	lastSample time.Time
}

func (w *archiveWatermark) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.systemID, w.timeline, w.samples, w.lastSample = "", 0, nil, time.Time{}
}

func parseLSN(value string) (uint64, bool) {
	hi, lo, ok := strings.Cut(value, "/")
	if !ok {
		return 0, false
	}
	upper, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, false
	}
	lower, err := strconv.ParseUint(lo, 16, 32)
	return upper<<32 | lower, err == nil
}

// WAL names contain timeline/log/segment, with segment numbering dependent on
// wal_segment_size. History/partial/backup files are not complete WAL segments.
func archivedSegmentEnd(name string, size uint64) (timeline, end uint64, ok bool) {
	if len(name) != 24 || size < 1<<20 || size > 1<<30 || size&(size-1) != 0 {
		return 0, 0, false
	}
	timeline, err := strconv.ParseUint(name[:8], 16, 32)
	if err != nil || timeline == 0 {
		return 0, 0, false
	}
	log, err := strconv.ParseUint(name[8:16], 16, 32)
	if err != nil {
		return 0, 0, false
	}
	segment, err := strconv.ParseUint(name[16:], 16, 32)
	if err != nil || segment >= (1<<32)/size {
		return 0, 0, false
	}
	start := log<<32 | segment*size
	end = start + size
	return timeline, end, end > start
}

func (w *archiveWatermark) observe(systemID string, timeline uint64, sample walSample, archived string, size uint64) string {
	if w.systemID != systemID || w.timeline != timeline {
		w.systemID, w.timeline, w.samples = systemID, timeline, nil
	}
	if !sample.at.IsZero() {
		w.samples = append(w.samples, sample)
	}
	// A prolonged archive outage bounds memory, dropping old samples fail-closed.
	if len(w.samples) > 10000 {
		w.samples = w.samples[len(w.samples)-10000:]
	}
	archivedTimeline, end, ok := archivedSegmentEnd(archived, size)
	if !ok || archivedTimeline != timeline {
		return ""
	}
	latest := -1
	for i, sample := range w.samples {
		if sample.lsn <= end && (latest < 0 || sample.at.After(w.samples[latest].at)) {
			latest = i
		}
	}
	result := ""
	if latest >= 0 {
		result = w.samples[latest].at.UTC().Format(time.RFC3339Nano)
		// Keep the latest covered point and all newer samples for the next archive.
		w.samples = append(w.samples[:0], w.samples[latest:]...)
	}
	return result
}

func (w *archiveWatermark) recoverableUntil(ctx context.Context, c *pgx.Conn, systemID string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var sample walSample
	// Status calls may be frequent. A committed WAL heartbeat at most every ten
	// seconds both proves time-target reachability and makes archive_timeout work
	// on otherwise idle primaries. A raw wall-time/LSN read alone cannot prove a
	// later commit record exists for PostgreSQL's recovery_target_time check.
	if time.Since(w.lastSample) >= 10*time.Second {
		tx, err := c.Begin(ctx)
		if err != nil {
			return ""
		}
		defer tx.Rollback(ctx)
		if err = tx.QueryRow(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&sample.at); err != nil {
			return ""
		}
		if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = local"); err != nil {
			return ""
		}
		if _, err = tx.Exec(ctx, "SELECT pg_catalog.pg_logical_emit_message(true, 'karakoram.pitr', '')"); err != nil {
			return ""
		}
		if err = tx.Commit(ctx); err != nil {
			return ""
		}
		w.lastSample = time.Now()
	}
	var lsn, currentWAL, archived string
	var segmentSize uint64
	if err := c.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text, pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()), pg_catalog.pg_size_bytes(pg_catalog.current_setting('wal_segment_size')), COALESCE(last_archived_wal,'') FROM pg_catalog.pg_stat_archiver").Scan(&lsn, &currentWAL, &segmentSize, &archived); err != nil {
		return ""
	}
	timeline, _, ok := archivedSegmentEnd(currentWAL, segmentSize)
	if !ok {
		return ""
	}
	if !sample.at.IsZero() {
		var valid bool
		sample.lsn, valid = parseLSN(lsn)
		if !valid {
			return ""
		}
	}
	return w.observe(systemID, timeline, sample, archived, segmentSize)
}
