// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import type { EngineUI } from '../types';
import { specSummary, validateSpec } from './helpers.ts';
export const weaviateMetadata: EngineUI = {
  label: 'Weaviate',
  description: 'Native vector database with API-key authentication, RBAC and persistent storage.',
  publicPort: 8080,
  publicPortLabel: 'REST',
  validateSpec,
  specSummary,
  topologySummary: (cluster) => specSummary(cluster.spec),
  monitoringPanels: [
    { title: 'Availability', names: ['weaviate.up'], yMax: 1 },
    { title: 'Objects and collections', names: ['weaviate.objects', 'weaviate.collections', 'weaviate.shards'], unit: 'count' },
    { title: 'Requests and queries', names: ['weaviate.requests.total', 'weaviate.queries.total', 'weaviate.batch.objects.total'], unit: 'count' },
    { title: 'Vector index', names: ['weaviate.vectorIndex.capacity', 'weaviate.vectorIndex.tombstones'], unit: 'count' },
    { title: 'LSM storage', names: ['weaviate.lsm.bytes'], unit: 'bytes' },
    { title: 'Async queue', names: ['weaviate.asyncQueue.length'], unit: 'count' },
  ],
};
