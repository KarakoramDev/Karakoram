// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';

export const NODE_COUNTS = [1, 3, 5, 7] as const;
export const PLUGIN_GROUPS = [
  { id: 'shovel', label: 'Shovel', description: 'Move messages between brokers.', plugins: ['rabbitmq_shovel', 'rabbitmq_shovel_management'] },
  { id: 'federation', label: 'Federation', description: 'Federate exchanges and queues across brokers.', plugins: ['rabbitmq_federation', 'rabbitmq_federation_management'] },
  { id: 'stream', label: 'Stream', description: 'Enable the stream protocol and its management interface.', plugins: ['rabbitmq_stream', 'rabbitmq_stream_management'] },
] as const;
export type PluginGroup = (typeof PLUGIN_GROUPS)[number]['id'];
export interface RabbitMQOptions {
  plugins: string[];
  managementUI: { public: boolean };
}

/** Plugin management extensions are enabled with their corresponding protocol plugin. */
export function buildEngineOptions(groups: readonly PluginGroup[], publicManagement: boolean): RabbitMQOptions {
  return {
    plugins: PLUGIN_GROUPS.filter((group) => groups.includes(group.id)).flatMap((group) => [...group.plugins]),
    managementUI: { public: publicManagement },
  };
}

export function selectedPluginGroups(options: Schemas['ClusterSpec']['engineOptions']): PluginGroup[] {
  const plugins = Array.isArray(options?.plugins) ? options.plugins : [];
  return PLUGIN_GROUPS.filter((group) => group.plugins.some((plugin) => plugins.includes(plugin))).map((group) => group.id);
}

export function managementIsPublic(options: Schemas['ClusterSpec']['engineOptions']): boolean {
  const management = options?.managementUI;
  return typeof management === 'object' && management !== null && !Array.isArray(management) && (management as Record<string, unknown>).public === true;
}

export function quorumFailureTolerance(nodes: number): number {
  return Math.max(0, Math.floor((nodes - 1) / 2));
}

const MANAGED_CONFIG_PREFIXES = [
  'listeners.', 'ssl_options.', 'management.tcp.', 'management.ssl.',
  'prometheus.', 'cluster_formation.', 'vm_memory_high_watermark',
  'total_memory_available_override_value', 'disk_free_limit', 'log.',
  'default_user', 'default_pass', 'loopback_users', 'distribution.',
  'quorum_queue.initial_cluster_size', 'default_queue_type',
  'management.load_definitions', 'load_definitions', 'definitions.',
  'auth_backends', 'ssl_cert_login',
];

export function validateConfigOverrides(config: string): string[] {
  const errors: string[] = [];
  config.split(/\r?\n/).forEach((line, index) => {
    const text = line.trim();
    if (!text || text.startsWith('#')) return;
    const match = /^([A-Za-z0-9_][A-Za-z0-9_.-]*)\s*=\s*(\S.*)$/.exec(text);
    if (!match) errors.push(`Line ${index + 1}: use rabbitmq.conf key = value syntax.`);
    else if (MANAGED_CONFIG_PREFIXES.some((prefix) => match[1] === prefix.replace(/\.$/, '') || match[1]!.startsWith(prefix))) errors.push(`Line ${index + 1}: ${match[1]} is managed by Karakoram.`);
  });
  return errors;
}

export function validateRabbitMQSpec(spec: Schemas['ClusterSpec']): string[] {
  const errors: string[] = [];
  if (!(NODE_COUNTS as readonly number[]).includes(spec.components.engine.replicas)) errors.push('RabbitMQ node count must be 1, 3, 5 or 7.');
  const options = spec.engineOptions;
  if (options) {
    const unknown = Object.keys(options).filter((key) => key !== 'plugins' && key !== 'managementUI');
    if (unknown.length) errors.push(`Unknown RabbitMQ engine options: ${unknown.join(', ')}.`);
    if (options.plugins !== undefined) {
      const allowed: readonly string[] = PLUGIN_GROUPS.flatMap((group) => [...group.plugins]);
      if (!Array.isArray(options.plugins) || options.plugins.some((plugin) => typeof plugin !== 'string' || !allowed.includes(plugin)) || new Set(options.plugins).size !== options.plugins.length) errors.push('Choose only supported RabbitMQ plugins, without duplicates.');
    }
    if (options.managementUI !== undefined) {
      const management = options.managementUI;
      if (typeof management !== 'object' || management === null || Array.isArray(management) || Object.keys(management).some((key) => key !== 'public') || typeof (management as Record<string, unknown>).public !== 'boolean') errors.push('Management UI exposure must be a public boolean.');
    }
  }
  if ((spec.tls?.mode ?? 'requireTLS') === 'requireTLS' && managementIsPublic(options)) errors.push('Public management is unavailable with requireTLS: Zerops routing forwards plaintext HTTP.');
  errors.push(...validateConfigOverrides(spec.engineConfig ?? ''));
  return errors;
}

export function parseDefinition(text: string): Record<string, unknown> {
  const value: unknown = JSON.parse(text);
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Definition must be a JSON object.');
  return value as Record<string, unknown>;
}

export function parseOptionalLimit(text: string, label: string): number | undefined {
  if (!text.trim()) return undefined;
  const value = Number(text);
  if (!Number.isInteger(value) || value < -1 || !Number.isSafeInteger(value)) throw new Error(`${label} must be a whole number of at least -1 (-1 means unlimited).`);
  return value;
}

export function parsePriority(text: string): number {
  const value = Number(text);
  if (!text.trim() || !Number.isSafeInteger(value) || value < 0) throw new Error('Priority must be a nonnegative whole number.');
  return value;
}

export function parseUserTags(text: string): Schemas['RabbitMQUser']['tags'] {
  const allowed: readonly string[] = ['management', 'policymaker', 'monitoring', 'administrator', 'impersonator'];
  const tags = text.split(/[\n,]+/).map((tag) => tag.trim()).filter(Boolean);
  const unknown = tags.filter((tag) => !allowed.includes(tag));
  if (unknown.length) throw new Error(`Unsupported user tags: ${unknown.join(', ')}.`);
  return [...new Set(tags)] as Schemas['RabbitMQUser']['tags'];
}
