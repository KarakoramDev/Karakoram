// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement } from 'react';
import { renderToString } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '@/api/client';
import type { ClusterContext } from './ClusterDetail';

register('../../../../test/tsx-loader.mjs', import.meta.url);
// Static import runs before loader registration; this test must load TSX afterward.
const { OverviewTab } = await import('./OverviewTab.tsx');

for (const scenario of [
  { name: 'capacity', health: 'yellow', message: 'Shard drain blocked for searchn2: insufficient data nodes for index replicas. Restore the node count or lower index replicas; never force removal.' },
  { name: 'green pinned primary', health: 'green', message: 'Shard drain blocked for searchn2: index pinned has no eligible destination; allocation filters reject destination nodes. Remove restrictive index allocation filters or restore the desired data-node count to cancel.' },
]) {
test(`Overview renders the blocked-drain callout above its tables: ${scenario.name}`, () => {
  const message = scenario.message;
  const cluster: Schemas['Cluster'] = {
    name: 'search', namespace: 'default', generation: 2,
    createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z',
    spec: {
      provider: 'elasticsearch', version: '9.5.5', topology: { type: 'cluster' },
      components: { engine: { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 2, minDisk: 5, maxDisk: 20 } } },
    },
    status: {
      phase: 'Scaling', observedGeneration: 1,
      members: [
        { name: 'searchn0', set: '', role: 'master,data', state: 'joined', health: true },
        { name: 'searchn1', set: '', role: 'master,data', state: 'joined', health: true },
        { name: 'searchn2', set: '', role: 'master,data', state: 'draining', health: false },
      ],
      conditions: [
        { type: 'ShardDrainBlocked', status: 'True', reason: 'ShardAllocationBlocked', message, lastTransitionTime: '2026-01-01T00:00:00Z' },
        { type: 'ClusterHealth', status: 'True', reason: scenario.health, message: scenario.health === 'green' ? 'All shards are allocated' : 'Replica shards are unassigned', lastTransitionTime: '2026-01-01T00:00:00Z' },
      ],
    },
  };
  const context: ClusterContext = { cluster, provider: undefined, namespace: 'default', name: 'search', base: '/clusters/default/search', can: () => false };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{
    path: '/', element: createElement(Outlet, { context }),
    children: [{ index: true, element: createElement(OverviewTab) }],
  }]);
  try {
    const markup = renderToString(createElement(QueryClientProvider, { client }, createElement(RouterProvider, { router })));
    const summary = markup.slice(0, markup.indexOf('<table'));
    assert.match(summary, /callout callout-warn/);
    assert.match(summary, /Shard drain blocked/);
    assert.ok(summary.includes(message));
    assert.match(summary, /Elasticsearch nodes healthy/);
    assert.match(summary, /1 desired · 3 observed · 1 draining/);
    assert.match(markup, /searchn2/);
    assert.match(markup, /Elasticsearch scaling guide/);
    if (scenario.health === 'green') {
      assert.match(summary, /allocation filters reject destination nodes/);
      assert.match(summary, /restore the desired data-node count to cancel/);
      assert.doesNotMatch(summary, /Elasticsearch health: yellow/);
    }
  } finally {
    router.dispose();
    client.clear();
  }
});
}
