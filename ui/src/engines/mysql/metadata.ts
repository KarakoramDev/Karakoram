// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI, MonitoringPanel } from '../types.ts';
import { mysqlChangeReview, specSummary, validateSpec } from './spec.ts';
export const mysqlMonitoringPanels: MonitoringPanel[] = [
  { title: 'MySQL connections / maximum', names: ['mysql_connections', 'mysql_max_connections'], unit: 'count' },
  { title: 'Queries', names: ['mysql_questions_total'], unit: 'queries/s' },
  { title: 'Threads running', names: ['mysql_threads_running'], unit: 'count' },
  { title: 'Network traffic', names: ['mysql_bytes_received_total', 'mysql_bytes_sent_total'], unit: 'bytes/s' },
  { title: 'Group Replication member state', names: ['mysql_gr_member_state'] },
  { title: 'Replication applier queue', names: ['mysql_replication_applier_queue'], unit: 'count' },
  { title: 'Replication lag', names: ['mysql_replication_lag_seconds'], unit: 's' },
  { title: 'Binlog archive lag', names: ['mysql_binlog_archive_lag_seconds'], unit: 's' },
  { title: 'ProxySQL backend state', names: ['mysql_proxysql_backend_state'] },
  { title: 'ProxySQL connections', names: ['mysql_proxysql_connections_used', 'mysql_proxysql_connections_free'], unit: 'count' },
];
export const mysqlMetadata: EngineUI = {
  label: 'MySQL', description: 'Percona Server for MySQL with optional Group Replication automatic failover and ProxySQL routing.', publicPort: 3306, publicPortLabel: 'MySQL',
  validateSpec, specSummary, changeReview: mysqlChangeReview, monitoringPanels: mysqlMonitoringPanels, topologySummary: (cluster) => specSummary(cluster.spec),
};
