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
register('../../../test/tsx-loader.mjs', import.meta.url);
// TSX and alias imports must wait until the test loader has registered.
const { MySQLAccessTab, PRIVILEGES } = await import('./AccessTab.tsx');
const { OptionsSection, TopologySection } = await import('./WizardSections.tsx');
const { ConnectionHelp } = await import('./ConnectionHelp.tsx');
const { mysqlUI } = await import('./index.tsx');
const { defaultComponent } = await import('./spec.ts');
const { tabAvailable } = await import('../index.ts');
const spec: Schemas['ClusterSpec'] = { provider: 'mysql', version: '8.4.11-11', topology: { type: 'group' }, components: { engine: { ...defaultComponent(undefined, 'engine'), replicas: 3 }, extra: { proxy: defaultComponent(undefined, 'proxy') } } };
const provider: Schemas['Provider'] = { name: 'mysql', displayName: 'MySQL', topologies: [{ type: 'group', displayName: 'Group', defaults: spec, componentSchemas: {}, limits: {} }], versions: [], backupTypes: ['full', 'incremental'], capabilities: ['mysqlAccess'], presets: [], upgradePaths: [] };
const cluster: Schemas['Cluster'] = { namespace: 'default', name: 'db', generation: 1, createdAt: '', updatedAt: '', spec, status: { observedGeneration: 1, phase: 'Ready', members: [], conditions: [] } };
function renderAccess(allowed = true, phase = cluster.status.phase) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['cluster', 'default', 'db', 'mysql-access', 'users'], { items: [{ name: 'app', host: '%', locked: false, maxUserConnections: 10, passwordRetained: true, routableThroughProxy: true }] });
  client.setQueryData(['cluster', 'default', 'db', 'mysql-access', 'databases'], { items: [{ name: 'appdb', characterSet: 'utf8mb4', collation: 'utf8mb4_0900_ai_ci' }] });
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: { ...cluster, status: { ...cluster.status, phase } }, provider, namespace: 'default', name: 'db', can: () => allowed } }), children: [{ index: true, element: createElement(MySQLAccessTab) }] }]);
  try { return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(RouterProvider, { router }))); }
  finally { router.dispose(); client.clear(); }
}
test('access tab renders cached API users, database fields and all privilege choices', () => {
  const markup = renderAccess();
  for (const label of ['app@%', 'appdb', 'Create user', 'Max user connections', 'Locked', 'Rotate password', 'Discard old password', 'Database characterSet', 'Database collation', 'Filter by user', 'Filter by database', 'Grant privileges', ...PRIVILEGES]) assert.ok(markup.includes(label), label);
  assert.doesNotMatch(markup, /Save this password now/);
  assert.match(renderAccess(false), /database-users read permission/);
  assert.match(renderAccess(false), /<fieldset disabled=""/);
  assert.match(renderAccess(true, 'Suspended'), /<fieldset disabled=""/);
});
test('wizard describes majority loss honestly and exposes backend options', () => {
  const props = { provider, spec, onChange: () => {} };
  const topology = renderToStaticMarkup(createElement(TopologySection, props));
  assert.match(topology, /automatic failover.*majority.*quorum loss/);
  assert.match(topology, /value="3"/); assert.match(topology, /value="5"/);
  const single = renderToStaticMarkup(createElement(TopologySection, { ...props, spec: { ...spec, topology: { type: 'single' } } }));
  assert.match(single, /not highly available/);
  const options = renderToStaticMarkup(createElement(OptionsSection, props));
  for (const label of ['3307', 'not a consistency guarantee', 'Group Replication consistency', 'Binlog archive interval', 'MySQL parameters']) assert.ok(options.includes(label), label);
  assert.deepEqual(mysqlUI.detailTabs?.map((t) => t.id), ['mysql']);
  assert.equal(tabAvailable(provider, 'mysql'), true); assert.equal(tabAvailable({ ...provider, capabilities: [] }, 'mysql'), false);
});
test('connection help shows CA-verified CLI, URI and TLS authentication warning', () => {
  const markup = renderToStaticMarkup(createElement(ConnectionHelp, { connection: { hosts: ['dbproxy:3306'], uri: 'mysql://dbproxy:3306' } }));
  for (const value of ['mysql --host=', '--ssl-mode=VERIFY_CA', '--ssl-ca=ca.pem', 'mysql://USER:PASSWORD@dbproxy:3306', 'caching_sha2_password requires TLS']) assert.ok(markup.includes(value), value);
});
