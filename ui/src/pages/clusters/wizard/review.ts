// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { engineRules } from './engine.ts';
import { componentEntries } from './validation.ts';

export function totalFootprint(spec: Schemas['ClusterSpec']) {
  const totals = { members: 0, volumes: 0, minCpu: 0, maxCpu: 0, minRam: 0, maxRam: 0, minDisk: 0, maxDisk: 0, runtimeMinDisk: 0, runtimeMaxDisk: 0 };
  for (const [name, component] of componentEntries(spec.components)) {
    if (!component) continue;
    const count = component.replicas * (name === 'engine' && spec.topology.type === 'sharded' ? spec.topology.shards ?? 0 : 1);
    totals.members += count;
    totals.minCpu += count * component.resources.minCpu;
    totals.maxCpu += count * component.resources.maxCpu;
    totals.minRam += count * component.resources.minRam;
    totals.maxRam += count * component.resources.maxRam;
    if (name !== 'router') {
      totals.volumes += count;
      totals.minDisk += count * component.resources.minDisk;
      totals.maxDisk += count * component.resources.maxDisk;
    }
  }
  const runtime = engineRules(spec.provider).runtimeDisk;
  totals.runtimeMinDisk = totals.members * runtime.min;
  totals.runtimeMaxDisk = totals.members * runtime.max;
  return totals;
}

/** Every provider-specific public surface should be added to this single review seam. */
export function exposureSummary(spec: Schemas['ClusterSpec']): string {
  const exposure = spec.exposure;
  return exposure?.public
    ? `Public database (${exposure.ipType}), allowlist: ${(exposure.allowlist ?? []).join(', ') || 'missing (required)'}`
    : 'Database private to the selected Zerops project';
}
