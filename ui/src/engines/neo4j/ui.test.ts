// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client';

// TSX/alias imports must run after registering the test loader; static imports run before it.
register('../../../test/tsx-loader.mjs', import.meta.url);
const { neo4jUI } = await import('./ui.tsx');
const { WizardSection } = await import('./WizardSection.tsx');
const { ConnectionHelp } = await import('./ConnectionHelp.tsx');
const { UsersTab } = await import('./UsersTab.tsx');
const spec: Schemas['ClusterSpec'] = { provider: 'neo4j', version: '2026.10.0', topology: { type: 'single' }, components: { engine: { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 4, minDisk: 5, maxDisk: 20 } } }, engineOptions: { heapMB: 512, pageCacheMB: 256, acceptBackupDowntime: true } };
const provider: Schemas['Provider'] = { name: 'neo4j', displayName: 'Neo4j Community', versions: [], topologies: [], backupTypes: ['dump'], capabilities: ['neo4jAccess', 'systemUsers'], presets: [], upgradePaths: [] };
const cluster: Schemas['Cluster'] = { namespace: 'default', name: 'graph', generation: 1, createdAt: '', updatedAt: '', spec, status: { observedGeneration: 1, phase: 'Ready', members: [], conditions: [] } };

function renderUsers(allowed = true) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['cluster', 'default', 'graph', 'neo4j-users'], { items: [{ name: 'neo4j', passwordChangeRequired: false }, { name: 'application', passwordChangeRequired: true }] });
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster, provider, namespace: 'default', name: 'graph', can: () => allowed } }), children: [{ index: true, element: createElement(UsersTab) }] }]);
  try { return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(RouterProvider, { router }))); }
  finally { router.dispose(); client.clear(); }
}

test('Neo4j wizard renders bounded optional memory, outage consent and only allowlisted settings', () => {
  const html = renderToStaticMarkup(createElement(WizardSection, { provider, spec, onChange: () => {} }));
  assert.match(html, /min="256" max="31744"/);
  assert.match(html, /value="512"/);
  assert.match(html, /value="256"/);
  assert.match(html, /80% of minimum RAM/);
  assert.match(html, /Accept scheduled backup downtime/);
  assert.match(html, /full outage/);
  assert.match(html, /db.transaction.timeout/);
  assert.doesNotMatch(html, /server.bolt.enabled/);
});

test('Neo4j connection help requires verified Bolt TLS and downloading the CA', () => {
  const html = renderToStaticMarkup(createElement(ConnectionHelp, { connection: { hosts: ['graph:7687'], publicHosts: [], uri: 'neo4j+s://graph:7687' } }));
  for (const phrase of ['neo4j+s://graph:7687', 'ca.pem', 'trusted certificate store', 'Do not disable certificate verification', 'Browser remain private']) assert.ok(html.includes(phrase), phrase);
});

test('Neo4j native users tab explains full privileges and protects the managed admin', () => {
  assert.equal(neo4jUI.detailTabs?.[0]?.capability, 'neo4jAccess');
  const html = renderUsers();
  assert.match(html, /full administrative privileges/);
  assert.match(html, /System users/);
  assert.match(html, /application/);
  assert.match(html, /Password change required/);
  assert.match(html, /Password \(empty generates one\)/);
  const managed = html.slice(html.indexOf('<td class="mono">neo4j'), html.indexOf('<td class="mono">application'));
  assert.equal((managed.match(/disabled=""/g) ?? []).length, 2);
  const denied = renderUsers(false);
  assert.match(denied, /Reading users requires database-users read permission/);
  assert.match(denied, /fieldset[^>]*disabled=""/);
});
