// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { engineRules } from './engine.ts';
import { MIN_RAM, type ComponentName } from '../../../engines/elasticsearch/spec.ts';

type Resources = Schemas['Resources'];
export interface ResourceMinimums { cpu: number; ram: number; disk: number }

function object(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

/** Read the component's JSON Schema; defaults/presets are recommendations, not minimums. */
export function resourceMinimums(provider: string, topology: Schemas['ProviderTopology'] | undefined, component: string): ResourceMinimums {
  const fallback = { ...engineRules(provider).resourceMinimums };
  if (provider === 'elasticsearch' && component in MIN_RAM) fallback.ram = MIN_RAM[component as ComponentName];
  const schema = object(topology?.componentSchemas[component]);
  const resources = object(object(schema.properties).resources);
  const properties = object(resources.properties);
  const minimum = (field: string, otherwise: number) => {
    const n = object(properties[field]).minimum;
    return typeof n === 'number' && Number.isFinite(n) ? n : otherwise;
  };
  return { cpu: minimum('minCpu', fallback.cpu), ram: minimum('minRam', fallback.ram), disk: minimum('minDisk', fallback.disk) };
}

export function resourceErrors(r: Resources, persisted?: Resources, noDisk = false, minimums: ResourceMinimums = engineRules('psmdb').resourceMinimums): Partial<Record<keyof Resources, string>> {
  const errors: Partial<Record<keyof Resources, string>> = {};
  for (const field of ['minCpu', 'maxCpu', 'startCpu'] as const) {
    const n = r[field];
    if (n === undefined) continue;
    if (!Number.isFinite(n) || n < minimums.cpu) errors[field] = `CPU must be at least ${minimums.cpu} core${minimums.cpu === 1 ? '' : 's'}.`;
    else if (!Number.isInteger(n)) errors[field] = 'CPU must be a whole number of cores.';
  }
  for (const field of ['minRam', 'maxRam'] as const) {
    if (!Number.isFinite(r[field]) || r[field] < minimums.ram) errors[field] = `RAM must be at least ${minimums.ram} GiB.`;
  }
  if (!noDisk) for (const field of ['minDisk', 'maxDisk'] as const) {
    if (!Number.isFinite(r[field]) || r[field] < minimums.disk) errors[field] = `Disk must be at least ${minimums.disk} GiB.`;
    else if (persisted && r[field] < persisted[field]) errors[field] = `Disk can only grow; currently ${persisted[field]} GiB.`;
  }
  if (r.minCpu > r.maxCpu) errors.maxCpu = 'Maximum CPU must be at least minimum CPU.';
  if (r.minRam > r.maxRam) errors.maxRam = 'Maximum RAM must be at least minimum RAM.';
  if (!noDisk && r.minDisk > r.maxDisk) errors.maxDisk = 'Maximum disk must be at least minimum disk.';
  if (r.startCpu !== undefined && (r.startCpu < r.minCpu || r.startCpu > r.maxCpu)) errors.startCpu = 'Start CPU must lie between minimum and maximum CPU.';
  return errors;
}

export function validateResources(r: Resources, persisted?: Resources, noDisk = false, minimums?: ResourceMinimums): string[] {
  return [...new Set(Object.values(resourceErrors(r, persisted, noDisk, minimums)))];
}
