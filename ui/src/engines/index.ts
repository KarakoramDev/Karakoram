// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../api/client';
import type { EngineUI, MonitoringPanel } from './types';
export type { EngineUI, MonitoringPanel, TopologyEditorProps, WizardSectionProps } from './types';

/**
 * Provider name → engine UI descriptor. Descriptors with React components are attached by
 * `./register.ts` (`./<provider>/index.tsx`) so this module stays loadable by `node --test`.
 */
export const engines: Record<string, EngineUI> = {
  psmdb: {
    label: 'Percona Server for MongoDB',
    description: 'Replica sets and sharded MongoDB clusters.',
    publicPort: 27017,
    monitoringPanels: [
      { title: 'Operations', names: ['opcounters.total'], unit: 'ops/s' },
      { title: 'Connections', names: ['connections.current'], unit: 'count' },
      { title: 'Replication lag', names: ['repl.lagSeconds'], unit: 's' },
      { title: 'Cache used', names: ['cache.usedBytes', 'cache.maxBytes'], unit: 'bytes' },
    ],
  },
};

export function publicExposureMetadata(engine: EngineUI | undefined, spec: Schemas['ClusterSpec']): { publicPort?: number; publicPortLabel?: string } {
  const port = engine?.publicPort;
  const label = engine?.publicPortLabel;
  return {
    publicPort: typeof port === 'function' ? port(spec) : port,
    publicPortLabel: typeof label === 'function' ? label(spec) : label,
  };
}

export const genericMonitoringPanels: MonitoringPanel[] = [
  { title: 'CPU', names: ['cpu.percent'], unit: '%', yMax: 100 },
  { title: 'Memory used', names: ['ram.usedBytes', 'ram.totalBytes'], unit: 'bytes' },
  { title: 'Disk used', names: ['disk.usedBytes', 'disk.totalBytes'], unit: 'bytes' },
];

const PSMDB_CAPABILITIES = ['databaseUsers', 'databaseRoles', 'systemUsers', 'balancer', 'pitr', 'backups', 'restores', 'fcv', 'publicExposure', 'tls', 'monitoring', 'engineConfig'];
const PSMDB_OPERATIONS = ['backup', 'delete-backup', 'restore', 'raise-fcv', 'restart', 'replace-member', 'rotate-system-user', 'rotate-tls'];
type Metadata = Pick<Schemas['Provider'], 'capabilities' | 'operations' | 'processes'>;

/** Catalog access is optional for readers with cluster-scoped permissions. */
export function detailProvider(name: string, provider: Schemas['Provider'] | undefined): Schemas['Provider'] | undefined {
  if (provider) return provider;
  if (name === 'psmdb') return undefined;
  return { name, displayName: name, capabilities: [], operations: [], processes: [], backupTypes: [], presets: [], topologies: [], upgradePaths: [], versions: [] };
}

export function hasCapability(provider: Metadata | undefined, capability: string): boolean {
  return (provider?.capabilities ?? PSMDB_CAPABILITIES).includes(capability);
}

export function supportsOperation(provider: Metadata | undefined, operation: string): boolean {
  return (provider?.operations ?? (provider?.capabilities === undefined ? PSMDB_OPERATIONS : [])).includes(operation);
}

export function providerProcesses(provider: Metadata | undefined): string[] {
  return [...new Set([...(provider?.processes ?? (provider?.capabilities === undefined ? ['mongod', 'mongos', 'pbm-agent'] : [])), 'agent'])];
}

export function defaultLogProcess(engine: string, role: Schemas['Member']['role'] | undefined, provider: Metadata | undefined): string {
  if (engine === 'psmdb') return role === 'router' ? 'mongos' : 'mongod';
  return providerProcesses(provider)[0]!;
}

export function defaultProvider<T extends { name: string }>(providers: T[]): T | undefined {
  return providers.find((provider) => provider.name === 'psmdb') ?? providers[0];
}

export function tabAvailable(provider: Metadata | undefined, tab: string): boolean {
  if (tab === 'users') return hasCapability(provider, 'databaseUsers') || hasCapability(provider, 'systemUsers');
  if (tab === 'backups' || tab === 'restores') return hasCapability(provider, tab);
  return true;
}

export function monitoringPanels(provider: string): MonitoringPanel[] {
  return [...(engines[provider]?.monitoringPanels ?? []), ...genericMonitoringPanels];
}

export function engineTopologySummary(cluster: Schemas['Cluster']): string | undefined {
  if (cluster.spec.provider === 'psmdb') return undefined;
  return engines[cluster.spec.provider]?.topologySummary?.(cluster) ?? `${cluster.spec.components.engine.replicas + Object.values(cluster.spec.components.extra ?? {}).reduce((total, component) => total + component.replicas, 0) + (cluster.spec.components.configServer?.replicas ?? 0) + (cluster.spec.components.router?.replicas ?? 0)} nodes`;
}

export interface ReplicaLimits {
  minReplicas?: number;
  maxReplicas?: number;
  allowedReplicas?: number[];
}

export function replicaLimits(topology: Schemas['ProviderTopology'] | undefined, component: string): ReplicaLimits {
  const value = topology?.limits[component];
  if (!value || typeof value !== 'object') return {};
  const limits = value as Record<string, unknown>;
  return {
    minReplicas: typeof limits.minReplicas === 'number' ? limits.minReplicas : undefined,
    maxReplicas: typeof limits.maxReplicas === 'number' ? limits.maxReplicas : undefined,
    allowedReplicas: Array.isArray(limits.allowedReplicas) && limits.allowedReplicas.every((n) => typeof n === 'number') ? limits.allowedReplicas : undefined,
  };
}

export function validateReplicas(replicas: number, limits: ReplicaLimits): string[] {
  if (!Number.isInteger(replicas)) return ['Node count must be an integer.'];
  if (limits.allowedReplicas?.length && !limits.allowedReplicas.includes(replicas)) return [`Node count must be ${limits.allowedReplicas.join(', ')}.`];
  if (limits.minReplicas !== undefined && replicas < limits.minReplicas) return [`Node count must be at least ${limits.minReplicas}.`];
  if (limits.maxReplicas !== undefined && replicas > limits.maxReplicas) return [`Node count must be at most ${limits.maxReplicas}.`];
  return [];
}
