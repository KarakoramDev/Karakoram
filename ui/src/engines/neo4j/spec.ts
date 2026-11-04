// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';

export const settingKeys = ['db.transaction.timeout', 'db.transaction.concurrent.maximum', 'db.lock.acquisition.timeout', 'db.logs.query.enabled', 'db.logs.query.threshold', 'db.query.default_language', 'dbms.cypher.planner'] as const;
export function options(spec: Schemas['ClusterSpec']): Record<string, unknown> {
  return spec.engineOptions && typeof spec.engineOptions === 'object' && !Array.isArray(spec.engineOptions) ? spec.engineOptions as Record<string, unknown> : {};
}
export function validateSpec(spec: Schemas['ClusterSpec']): string[] {
  const value = options(spec);
  const ram = spec.components.engine.resources.minRam * 1024;
  const heap = value.heapMB ?? Math.max(256, Math.min(31744, Math.floor(ram * 0.4 / 64) * 64));
  const page = value.pageCacheMB ?? Math.max(64, Math.floor(ram * 0.25 / 64) * 64);
  const errors: string[] = [];
  if (typeof heap !== 'number' || !Number.isInteger(heap) || heap < 256 || heap > 31744) errors.push('Heap must be an integer from 256 to 31744 MiB.');
  if (typeof page !== 'number' || !Number.isInteger(page) || page < 64) errors.push('Page cache must be an integer of at least 64 MiB.');
  if (typeof heap === 'number' && typeof page === 'number' && heap + page > ram * 0.8) errors.push('Heap and page cache together must not exceed 80% of minimum RAM.');
  if (spec.topology.type !== 'single' || spec.components.engine.replicas !== 1) errors.push('Neo4j Community requires a single instance; high availability is not supported.');
  if (spec.engineConfig) errors.push('Neo4j does not accept engineConfig; use engineOptions.settings.');
  return errors;
}
export function boltURI(host?: string): string {
  return `neo4j+s://${host || 'HOST:7687'}`;
}
