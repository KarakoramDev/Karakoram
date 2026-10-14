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
  return {
    resourceMinimums: { cpu: 1, ram: 0, disk: 0 },
    topology: {} as Record<string, { title: string; description: string }>,
    runtimeDisk: { min: 0, max: 0 },
    exposureLabel: 'Expose database publicly',
    configHint: 'Provider configuration overrides are validated by the server.',
  };
}

export function engineConsequences(spec: Schemas['ClusterSpec']): string[] {
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
