// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI, MonitoringPanel } from '../types.ts';
import { postgresChangeReview, specSummary, validateSpec } from './spec.ts';

/** Canonical names strip the karakoram_ exposition prefix; counter series are rates. */
export const postgresMonitoringPanels: MonitoringPanel[] = [
  { title: 'PostgreSQL connections / maximum', names: ['postgres_connections', 'postgres_max_connections'], unit: 'count' },
  { title: 'Committed transactions', names: ['postgres_transactions_committed_total'], unit: 'transactions/s' },
  { title: 'Replication lag', names: ['postgres_replication_lag_bytes'], unit: 'bytes' },
  { title: 'WAL archive age', names: ['postgres_last_archived_age_seconds'], unit: 's' },
  { title: 'Database size', names: ['postgres_database_size_bytes'], unit: 'bytes' },
  { title: 'ProxySQL connections', names: ['postgres_proxysql_connections_used', 'postgres_proxysql_connections_free'], unit: 'count' },
];
export const postgresMetadata: EngineUI = {
  label: 'PostgreSQL', description: 'Percona PostgreSQL with optional Patroni high availability and ProxySQL routing.', publicPort: 5432, publicPortLabel: 'PostgreSQL',
  validateSpec, specSummary, changeReview: postgresChangeReview, monitoringPanels: postgresMonitoringPanels, topologySummary: (cluster) => specSummary(cluster.spec),
};
export const supabaseMetadata: EngineUI = {
  ...postgresMetadata, label: 'Supabase', description: 'PostgreSQL 17 with independently scalable native Supabase services.',
  monitoringPanels: [...postgresMonitoringPanels, { title: 'Supabase components ready', names: ['supabase_up'], yMax: 1, description: 'One series per component/member: 1 is ready, 0 is unavailable.' }],
};
