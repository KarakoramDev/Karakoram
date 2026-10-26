// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

const slotRetentionSQL = `SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',slot_name,'type',slot_type,'active',active,'retainedBytes',COALESCE(GREATEST(pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),restart_lsn),0),0)::bigint,'walStatus',COALESCE(wal_status,''),'safeWalBytes',safe_wal_size)),'[]'::jsonb) FROM pg_catalog.pg_replication_slots`

func sampleWALHealth(ctx context.Context, c *pgx.Conn, out *pgspec.StatusResponse) {
	out.WALStatusKnown = c.QueryRow(ctx, `SELECT COALESCE((SELECT pg_catalog.sum(size) FROM pg_catalog.pg_ls_waldir()),0)::bigint, (SELECT pg_catalog.count(*) FROM pg_catalog.pg_ls_archive_statusdir() WHERE name LIKE '%.ready')`).Scan(&out.WALBytes, &out.ArchivePending) == nil
	if out.Role == "primary" || out.Role == "standalone" {
		out.SlotStatusKnown = c.QueryRow(ctx, slotRetentionSQL).Scan(&out.ReplicationSlots) == nil
	}
}
