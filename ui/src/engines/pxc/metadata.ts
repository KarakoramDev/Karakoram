// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI } from '../types.ts';
import { mysqlMetadata, mysqlMonitoringPanels } from '../mysql/metadata.ts';
export const pxcMetadata: EngineUI = {
  ...mysqlMetadata,
  label: 'Percona XtraDB Cluster',
  description: 'Three or five Galera members with encrypted state transfer and one ProxySQL-routed writer. Separate PXC versions and backup provenance.',
  monitoringPanels: mysqlMonitoringPanels.filter((panel) => !panel.names.some((name) => ['mysql_gr_member_state', 'mysql_replication_applier_queue', 'mysql_replication_lag_seconds'].includes(name))),
};
