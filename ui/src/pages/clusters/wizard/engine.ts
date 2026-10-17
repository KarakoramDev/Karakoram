// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

/** Provider-specific copy and fallback rules belong here, not in shared controls. */
export function engineRules(provider: string) {
  if (provider === 'psmdb') return {
    resourceMinimums: { cpu: 1, ram: 0.5, disk: 5 },
    runtimeDisk: { min: 1, max: 3 },
    topology: {
      replicaSet: { title: 'Replica set', description: 'One replicated database with 1, 3, 5 or 7 members. Most workloads start here.' },
      sharded: { title: 'Sharded cluster', description: 'Shard replica sets, config servers and routers distribute large datasets horizontally.' },
    } as Record<string, { title: string; description: string }>,
    exposureLabel: 'Expose MongoDB publicly on port 27017',
    configHint: 'Merged into mongod/mongos configuration and validated by the server.',
  };
  if (provider === 'postgres' || provider === 'supabase') return {
    resourceMinimums: { cpu: 1, ram: 1, disk: 5 },
    runtimeDisk: { min: 0, max: 0 },
    topology: {
      single: { title: 'Single PostgreSQL member', description: 'One database member without failover. Maintenance causes downtime.' },
      ha: { title: 'Highly available PostgreSQL', description: 'Three or five Patroni members with etcd quorum and a stable ProxySQL endpoint.' },
    } as Record<string, { title: string; description: string }>,
    exposureLabel: 'Expose PostgreSQL publicly on port 5432',
    configHint: 'Configure supported PostgreSQL parameters under engine options.',
  };
  if (provider === 'elasticsearch') return {
    resourceMinimums: { cpu: 1, ram: 1.5, disk: 0 },
    runtimeDisk: { min: 0, max: 0 },
    topology: {
      cluster: { title: 'Combined master and data nodes', description: 'Every Elasticsearch node is master-eligible and stores data. A compact topology for development and smaller production clusters.' },
      dedicatedMasters: { title: 'Dedicated masters + data nodes', description: 'Three or five master-only nodes manage cluster state separately from the data nodes that store and search your indices.' },
    } as Record<string, { title: string; description: string }>,
    exposureLabel: 'Expose Elasticsearch HTTPS publicly on port 9200',
    configHint: 'Configure supported settings under Elastic Stack options.',
  };
  return {
    resourceMinimums: { cpu: 1, ram: 0, disk: 0 },
    topology: {} as Record<string, { title: string; description: string }>,
    runtimeDisk: { min: 0, max: 0 },
    exposureLabel: 'Expose database publicly',
    configHint: 'Provider configuration overrides are validated by the server.',
  };
}

export function engineConsequences(spec: Schemas['ClusterSpec']): string[] {
  if (spec.provider === 'postgres' || spec.provider === 'supabase') return [
    spec.topology.type === 'single' ? 'A single data member has no failover; maintenance interrupts database access.' : 'Patroni failover briefly interrupts writes. Three or five etcd voters maintain the data-member quorum.',
    'ProxySQL is the application endpoint. Login-role passwords must be managed through Postgres access to synchronize proxy authentication; roles created directly in SQL are not routable through the proxy.',
    spec.backup?.pitr?.enabled ? 'PITR archives WAL to registered backup storage. Coverage requires a completed full backup and a continuous WAL archive.' : 'PITR is disabled; recovery is limited to completed backups.',
  ];
  if (spec.provider !== 'psmdb') return [];
  return [
    ...(spec.components.engine.replicas === 1 ? ['A single database member has no failover; maintenance makes its database unavailable.'] : []),
    spec.exposure?.public
      ? `Public TCP routes expose ${spec.topology.type === 'sharded' ? 'every router' : 'every database member'}. Replica-set discovery advertises private members; a public seed is not external HA/failover.`
      : 'Database endpoints remain private to the selected Zerops project.',
    spec.backup?.pitr?.enabled
      ? 'PITR continuously captures oplog slices to backup storage. Recovery coverage starts only after a completed base backup.'
      : 'PITR is disabled; recovery is limited to completed backups, if configured.',
    'Scheduled backups consume backup-storage capacity; retention deletes older scheduled backups according to the selected count.',
  ];
}
