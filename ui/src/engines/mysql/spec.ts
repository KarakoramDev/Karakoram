// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client.ts';
import type { ComponentValidation, EngineChangeReview } from '../types.ts';
import { object, component, specPatch } from '../postgres/spec.ts';
export { object, component, defaultComponent, withComponent } from '../postgres/spec.ts';
type Spec = Schemas['ClusterSpec'];
export function isMySQL(provider: string): boolean { return provider === 'mysql' || provider === 'pxc'; }
export const CONSISTENCIES = ['BEFORE_ON_PRIMARY_FAILOVER', 'EVENTUAL', 'BEFORE', 'AFTER', 'BEFORE_AND_AFTER'];
export const PARAMETER_BOUNDS: Record<string, [number, number]> = {
  innodb_buffer_pool_instances: [1, 64], table_open_cache_instances: [1, 64],
  max_connections: [1, 100000], innodb_buffer_pool_size: [5242880, 2 ** 50], innodb_io_capacity: [100, 10000000], innodb_io_capacity_max: [100, 10000000], innodb_lock_wait_timeout: [1, 1073741824], lock_wait_timeout: [1, 31536000], wait_timeout: [1, 31536000], interactive_timeout: [1, 31536000], max_allowed_packet: [1024, 2 ** 30], sort_buffer_size: [128, 2 ** 30], join_buffer_size: [128, 2 ** 30], read_buffer_size: [128, 2 ** 30], read_rnd_buffer_size: [128, 2 ** 30], tmp_table_size: [1024, 2 ** 40], max_heap_table_size: [1024, 2 ** 40], table_open_cache: [1, 1000000], thread_cache_size: [1, 1000000],
};
export const BOOLEAN_PARAMETERS = ['slow_query_log', 'log_queries_not_using_indexes', 'innodb_print_all_deadlocks'];
export const PARAMETERS = [...Object.keys(PARAMETER_BOUNDS), ...BOOLEAN_PARAMETERS, 'long_query_time', 'transaction_isolation'];
export function validateSpec(spec: Spec): string[] {
  const errors: string[] = [], o = object(spec.engineOptions), group = object(o.groupReplication), binlog = object(o.binlog);
  if (spec.engineConfig?.trim()) errors.push('Use supported MySQL parameters instead of raw engine configuration.');
  if (spec.tls && spec.tls.mode !== 'requireTLS') errors.push('MySQL requires TLS; choose requireTLS.');
  for (const [key, value] of Object.entries(object(o.parameters))) {
    let valid = typeof value === 'string' && value.length > 0 && !/[\0\r\n#;']/.test(value) && PARAMETERS.includes(key);
    const bounds = PARAMETER_BOUNDS[key];
    if (bounds) valid &&= /^[+-]?\d+$/.test(String(value)) && Number(value) >= bounds[0] && Number(value) <= bounds[1];
    if (BOOLEAN_PARAMETERS.includes(key)) valid &&= ['ON', 'OFF', '0', '1'].includes(String(value));
    if (key === 'long_query_time') valid &&= Number.isFinite(Number(value)) && Number(value) >= 0 && Number(value) <= 31536000;
    if (key === 'transaction_isolation') valid &&= ['READ-UNCOMMITTED', 'READ-COMMITTED', 'REPEATABLE-READ', 'SERIALIZABLE'].includes(String(value));
    if (!valid) errors.push(`MySQL parameter ${key} is unsupported or has an invalid value. The backend is authoritative.`);
  }
  if (spec.provider === 'pxc' && 'groupReplication' in o) errors.push('PXC uses Galera, not Group Replication. Remove groupReplication options.');
  if (spec.provider !== 'pxc' && !CONSISTENCIES.includes(String(group.consistency ?? CONSISTENCIES[0]))) errors.push('Choose a supported Group Replication consistency.');
  if (spec.provider !== 'pxc' && (!Number.isInteger(group.unreachableMajorityTimeout ?? 30) || Number(group.unreachableMajorityTimeout ?? 30) < 1 || Number(group.unreachableMajorityTimeout ?? 30) > 31536000)) errors.push('Majority timeout must be an integer between 1 and 31536000.');
  for (const [label, value, min, max] of [['Binlog archive interval', binlog.archiveInterval ?? 300, 1, 86400], ['Binlog expiry', binlog.expireSeconds ?? 604800, Number(binlog.archiveInterval ?? 300) + 1, 2147483647]] as const) if (!Number.isInteger(value) || Number(value) < min || Number(value) > max) errors.push(`${label} must be an integer between ${min} and ${max}.`);
  return errors;
}
export function validateComponents(spec: Spec, topology?: Schemas['ProviderTopology'], resources?: (r: Schemas['Resources'], stateless: boolean, name: string) => string[]): ComponentValidation {
  const errors: ComponentValidation = { topology: [], resources: [] };
  const pxc = spec.provider === 'pxc';
  if (!(pxc ? ['cluster'] : ['single', 'group']).includes(spec.topology.type)) errors.topology.push(pxc ? 'PXC requires cluster topology.' : 'Choose single or group topology.');
  if (!(spec.topology.type === 'single' && !pxc ? [1] : [3, 5]).includes(spec.components.engine.replicas)) errors.topology.push(pxc ? 'PXC requires 3 or 5 data members.' : 'Single requires 1 member; group requires 3 or 5.');
  const proxy = component(spec, 'proxy')?.replicas ?? 0;
  if (!Number.isInteger(proxy) || proxy < (pxc || spec.topology.type === 'group' ? 1 : 0) || proxy > 5) errors.topology.push(pxc ? 'PXC requires 1–5 proxy replicas.' : 'Proxy replicas must be 0–5 for single or 1–5 for group.');
  for (const [name, c] of [['engine', spec.components.engine], ...Object.entries(spec.components.extra ?? {})] as [string, Schemas['Component']][]) {
    if (name !== 'engine' && name !== 'proxy') errors.topology.push(`Unsupported MySQL component ${name}.`);
    if (c.replicas && resources) errors.resources.push(...resources(c.resources, name !== 'engine', name).map((e) => `${name}: ${e}`));
    const limits = object(topology?.limits[name]);
    if (typeof limits.maxReplicas === 'number' && c.replicas > limits.maxReplicas) errors.topology.push(`${name}: at most ${limits.maxReplicas} replicas.`);
  }
  return errors;
}
export function specSummary(spec: Spec): string { return `${spec.provider === 'pxc' ? 'Galera cluster · single routed writer' : spec.topology.type === 'group' ? 'Group Replication' : 'Single (not HA)'} · ${spec.components.engine.replicas} data · ${component(spec, 'proxy')?.replicas ?? 0} proxy`; }
export function mysqlMajorUpgrade(current: string, target: string): boolean { return current.split('.').slice(0, 2).join('.') !== target.split('.').slice(0, 2).join('.'); }
export function transitionErrors(current: Spec, draft: Spec): string[] {
  const errors: string[] = [];
  if (current.provider !== draft.provider) errors.push('MySQL and PXC are separate providers; conversion is not supported.');
  if (current.provider === 'pxc' && draft.topology.type !== 'cluster') errors.push('PXC requires cluster topology; single and Group Replication are not supported.');
  if (current.topology.type === 'group' && draft.topology.type !== 'group') errors.push('Group cannot be converted to single.');
  if (draft.version.localeCompare(current.version, undefined, { numeric: true }) < 0) errors.push('MySQL downgrades are blocked.');
  return errors;
}
export const mysqlChangeReview: EngineChangeReview = {
  buildPatch: specPatch,
  disruptions: (current, draft) => JSON.stringify(current) === JSON.stringify(draft) ? [] : [
    ...transitionErrors(current, draft),
    ...(mysqlMajorUpgrade(current.version, draft.version) ? ['MySQL 8.0 → 8.4 major upgrade requires a recent successful backup. Binary downgrades are not supported. Use the Upgrades tab.'] : []),
    ...(current.topology.type === 'single' && draft.topology.type === 'group' ? ['Single → group enables Group Replication with automatic failover; a majority is required and writes block on quorum loss.'] : []),
    ...(JSON.stringify(object(current.engineOptions).parameters) !== JSON.stringify(object(draft.engineOptions).parameters) ? [current.provider === 'pxc' ? 'Dynamic parameters can apply live; restart-required changes roll Galera members while preserving a Primary component.' : 'Dynamic parameters can apply live; restart-required parameter changes roll secondaries before moving the primary. Single-member restarts cause downtime.'] : []),
    current.provider === 'pxc' ? 'PXC maintenance requires a Primary component and Synced members. ProxySQL routes one writer; failover interrupts access and timed-out commits may have succeeded. Never force-bootstrap an unfenced partition.' : 'Single-member maintenance causes downtime. Group maintenance requires a majority; failover briefly interrupts writes.',
  ],
};
