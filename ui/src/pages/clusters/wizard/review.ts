// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { managementIsPublic, PLUGIN_GROUPS, selectedPluginGroups } from '../../../engines/rabbitmq/helpers.ts';
import { readOptions } from '../../../engines/elasticsearch/spec.ts';
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

/** Database exposure is independent from provider-specific management endpoints. */
export function exposureSummary(spec: Schemas['ClusterSpec']): string {
  const exposure = spec.exposure;
  const database = exposure?.public
    ? `Public database (${exposure.ipType}), allowlist: ${(exposure.allowlist ?? []).join(', ') || 'missing (required)'}`
    : 'Database private to the selected Zerops project';
  if (spec.provider !== 'elasticsearch') return database;
  const options = readOptions(spec);
  const routes = (['kibana', 'apm'] as const).filter((name) => options[name].public);
  return [database, ...routes.map((name) => `Public ${name === 'kibana' ? 'Kibana' : 'APM Server'} HTTPS: internet reachable, no IP allowlist, authentication only (${options[name].allowAllAddresses ? 'acknowledged' : 'acknowledgement required'})`)].join('; ');
}

export function exposureReviewRows(spec: Schemas['ClusterSpec']): [string, string][] {
  const rows: [string, string][] = [[spec.provider === 'rabbitmq' ? 'AMQP exposure' : 'Exposure', exposureSummary(spec)]];
  if (spec.provider === 'rabbitmq') rows.push(['Management UI exposure', managementIsPublic(spec.engineOptions)
    ? 'Public RabbitMQ management UI/API over HTTPS, using RabbitMQ credentials. Not limited by the AMQP allowlist; routing includes a private plaintext HTTP hop.'
    : 'RabbitMQ management UI/API private to the selected Zerops project']);
  return rows;
}

export function configurationReviewRows(spec: Schemas['ClusterSpec']): [string, string][] {
  const rows: [string, string][] = [['Engine configuration', spec.engineConfig?.trim() || 'Provider defaults']];
  if (spec.provider === 'rabbitmq') {
    const selected = selectedPluginGroups(spec.engineOptions);
    rows.push(['Optional plugins', PLUGIN_GROUPS.filter((group) => selected.includes(group.id)).map((group) => group.label).join(', ') || 'none']);
  }
  return rows;
}
