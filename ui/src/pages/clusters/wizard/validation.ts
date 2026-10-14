// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { resourceMinimums, validateResources } from './resources.ts';
import { validateExposure, validateTLS } from './access.ts';
import { validateSchedules } from './schedules.ts';
import type { Validation } from './state.ts';

export function nameError(name: string, existing: string[]): string | undefined {
  if (!/^[a-z][a-z0-9]{1,11}$/.test(name)) return 'Use 2–12 lowercase letters/digits starting with a letter.';
  if (existing.includes(name)) return 'A cluster with this name already exists in this namespace.';
}

export function memberChoices(topology: Schemas['ProviderTopology'] | undefined, component: string): number[] {
  const limits = topology?.limits[component] as { allowedReplicas?: unknown } | undefined;
  return Array.isArray(limits?.allowedReplicas) ? limits.allowedReplicas.filter((n): n is number => typeof n === 'number') : [];
}

export function topologyErrors(spec: Schemas['ClusterSpec'], topology: Schemas['ProviderTopology'] | undefined): Record<string, string> {
  const errors: Record<string, string> = {};
  for (const [name, component] of Object.entries(spec.components)) {
    if (!component) continue;
    const limits = topology?.limits[name] as { minReplicas?: number; maxReplicas?: number } | undefined;
    const allowed = memberChoices(topology, name);
    if (!Number.isInteger(component.replicas) || component.replicas < (limits?.minReplicas ?? 1) || component.replicas > (limits?.maxReplicas ?? Infinity)) errors[name] = `Member count must be a whole number from ${limits?.minReplicas ?? 1}${limits?.maxReplicas ? ` to ${limits.maxReplicas}` : ' or more'}.`;
    if (allowed.length && !allowed.includes(component.replicas)) errors[name] = `Choose ${allowed.join(', ')} members.`;
  }
  if (spec.topology.type === 'sharded') {
    const schema = topology?.uiSchema?.properties as { shards?: { minimum?: number; maximum?: number } } | undefined;
    const min = schema?.shards?.minimum ?? 1;
    const max = schema?.shards?.maximum ?? Infinity;
    if (!Number.isInteger(spec.topology.shards) || (spec.topology.shards ?? 0) < min || (spec.topology.shards ?? 0) > max) errors.shards = `Shard count must be a whole number from ${min}${Number.isFinite(max) ? ` to ${max}` : ' or more'}.`;
    if (!spec.components.configServer) errors.configServer = 'Config servers are required.';
    if (!spec.components.router) errors.router = 'Routers are required.';
  }
  return errors;
}

export function validateWizard(name: string, namespace: string, spec: Schemas['ClusterSpec'], existing: string[], provider: Schemas['Provider'] | undefined, allAddressesConfirmed = false): Validation {
  const v: Validation = { basics: [], topology: [], resources: [], backups: [], access: [] };
  if (!namespace) v.basics.push('Choose the namespace (Zerops project) that will host the cluster.');
  const invalidName = nameError(name, existing);
  if (invalidName) v.basics.push(invalidName);
  const version = provider?.versions.find((item) => item.version === spec.version);
  if (!version) v.basics.push('Choose an available engine version.');
  else if (version.status === 'unavailable') v.basics.push('This version is not available for new clusters.');
  const topology = provider?.topologies.find((item) => item.type === spec.topology.type);
  v.topology.push(...Object.values(topologyErrors(spec, topology)));
  for (const [name, component] of Object.entries(spec.components)) {
    if (component) v.resources.push(...validateResources(component.resources, undefined, name === 'router', resourceMinimums(spec.provider, topology, name)).map((error) => `${name}: ${error}`));
  }
  const backup = spec.backup;
  if (backup) {
    const schedules = backup.schedules ?? [];
    if ((schedules.length || backup.pitr?.enabled) && !backup.storage) v.backups.push('Choose a primary backup storage for schedules and PITR.');
    v.backups.push(...validateSchedules(schedules));
    if (backup.pitr?.enabled && (!Number.isInteger(backup.pitr.oplogSpanMinutes) || backup.pitr.oplogSpanMinutes < 1)) v.backups.push('Oplog span must be a whole number of at least 1 minute.');
  }
  if (spec.dataSource) {
    if (!spec.dataSource.cluster) v.backups.push('Data source: choose a source cluster.');
    if (Boolean(spec.dataSource.backup) === Boolean(spec.dataSource.pitrTime)) v.backups.push('Data source: choose exactly one backup or point in time.');
  }
  v.access.push(...validateTLS(spec.tls), ...validateExposure(spec.exposure, allAddressesConfirmed));
  return v;
}
