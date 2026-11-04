// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI } from '../types';
import { validateSpec } from './spec.ts';

export const neo4jMetadata: EngineUI = {
  label: 'Neo4j Community',
  description: 'Single-instance graph database. No high availability; backups and maintenance cause a full outage.',
  publicPort: 7687,
  publicPortLabel: 'Bolt',
  validateSpec,
  specSummary: () => 'Single instance (Community)',
  topologySummary: () => 'Single instance (Community)',
  monitoringPanels: [
    { title: 'Availability', names: ['neo4j_up'], unit: 'count', yMax: 1 },
    { title: 'JVM heap', names: ['neo4j_heap_used_bytes', 'neo4j_heap_committed_bytes', 'neo4j_heap_max_bytes', 'neo4j_heap_configured_bytes'], unit: 'bytes' },
    { title: 'Configured page cache', names: ['neo4j_pagecache_configured_bytes'], unit: 'bytes' },
    { title: 'Graph size', names: ['neo4j_nodes', 'neo4j_relationships'], unit: 'count' },
    { title: 'Active transactions', names: ['neo4j_transactions_active'], unit: 'count' },
    { title: 'Store size', names: ['neo4j_store_size_bytes'], unit: 'bytes' },
  ],
};
