// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client.ts';
import { defaultComponent, mysqlChangeReview, specSummary, transitionErrors, validateComponents, validateSpec } from '../mysql/spec.ts';
import { pxcMetadata } from './metadata.ts';
import { engines } from '../index.ts';
register('../../../test/tsx-loader.mjs', import.meta.url);
// Register the TSX/alias loader before loading React descriptors and their registration boundary.
const { pxcUI } = await import('./index.tsx');
await import('../register.ts');
const { TopologySection, OptionsSection } = await import('../mysql/WizardSections.tsx');
const { TopologyEditor } = await import('../mysql/TopologyEditor.tsx');
const { RecoveryTab, QUORUM_CONFIRMATION } = await import('./RecoveryTab.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const spec: Schemas['ClusterSpec'] = { provider: 'pxc', version: '8.4.11-11.1', topology: { type: 'cluster' }, components: { engine: { ...defaultComponent(undefined, 'engine'), replicas: 3 }, extra: { proxy: defaultComponent(undefined, 'proxy') } } };
const provider: Schemas['Provider'] = { name: 'pxc', displayName: 'Percona XtraDB Cluster', topologies: [{ type: 'cluster', displayName: 'Galera cluster', defaults: spec, componentSchemas: {}, limits: {} }], versions: [], backupTypes: ['full', 'incremental'], capabilities: ['mysqlAccess'], operations: ['recover-quorum'], presets: [], upgradePaths: [] };
const cluster: Schemas['Cluster'] = { namespace: 'default', name: 'db', generation: 1, createdAt: '', updatedAt: '', spec, status: { observedGeneration: 1, phase: 'Ready', members: [], conditions: [] } };
test('PXC is discoverable separately and reuses native MySQL access', () => {
  assert.equal(engines.pxc, pxcUI);
  assert.equal(pxcMetadata.label, 'Percona XtraDB Cluster');
  assert.equal(pxcUI.publicPort, 3306);
  assert.deepEqual(pxcUI.detailTabs?.map((tab) => [tab.id, tab.capability]), [['mysql', 'mysqlAccess'], ['recovery', undefined]]);
  assert.match(specSummary(spec), /Galera.*single routed writer/);
  assert.ok(!pxcMetadata.monitoringPanels?.some((panel) => panel.names.includes('mysql_gr_member_state')));
});
test('PXC restricts topology, quorum counts, proxy and replication options', () => {
  assert.deepEqual(validateSpec(spec), []);
  for (const replicas of [3, 5]) assert.deepEqual(validateComponents({ ...spec, components: { ...spec.components, engine: { ...spec.components.engine, replicas } } }).topology, []);
  for (const replicas of [1, 2, 4, 6]) assert.ok(validateComponents({ ...spec, components: { ...spec.components, engine: { ...spec.components.engine, replicas } } }).topology.length);
  assert.ok(validateComponents({ ...spec, components: { engine: spec.components.engine } }).topology.length);
  assert.ok(validateComponents({ ...spec, topology: { type: 'group' } }).topology.length);
  assert.ok(validateSpec({ ...spec, engineOptions: { groupReplication: {} } }).length);
  assert.match(transitionErrors(spec, { ...spec, provider: 'mysql' }).join(' '), /separate providers/);
  assert.match(mysqlChangeReview.disruptions(spec, { ...spec, version: '8.4.12-12.1' }).join(' '), /Primary component.*timed-out commits/);
});
test('PXC wizard and editor distinguish Galera from Group Replication', () => {
  const props = { provider, spec, onChange: () => {} };
  const topology = renderToStaticMarkup(createElement(TopologySection, props));
  for (const label of ['Galera', 'ENFORCING', 'primary keys', 'idempotent', 'quorum', 'value="3"', 'value="5"']) assert.ok(topology.toLowerCase().includes(label.toLowerCase()), label);
  const options = renderToStaticMarkup(createElement(OptionsSection, props));
  assert.match(options, /IST.*SST/);
  assert.match(options, /not a consistency guarantee/);
  assert.doesNotMatch(options, /<label[^>]*>Group Replication consistency|Unreachable majority timeout/);
  const editor = renderToStaticMarkup(createElement(TopologyEditor, { cluster, provider, editable: true, can: () => true, busy: false, submit: async () => cluster }));
  assert.match(editor, /PXC topology/);
  assert.match(editor, /Cluster-traffic certificate rotation is offline/);
  assert.match(editor, /stops all data members.*full-outage recovery/);
  assert.match(editor, /replaces the dedicated traffic CA and shared leaf\/key/);
  assert.doesNotMatch(editor, /<option value="single"|<option value="group"|<option value="ha"/);
});
test('PXC recovery shows fencing confirmation and respects update permission', () => {
  const renderRecovery = (allowed: boolean) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: { ...cluster, status: { ...cluster.status, phase: 'Degraded' } }, provider, namespace: 'default', name: 'db', base: '/clusters/default/db', can: () => allowed } }), children: [{ index: true, element: createElement(RecoveryTab) }] }]);
    try { return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router })))); }
    finally { router.dispose(); client.clear(); }
  };
  const markup = renderRecovery(true);
  for (const value of ['Recover quorum', QUORUM_CONFIRMATION, 'split brain', 'highest committed position', 'fencing completion']) assert.ok(markup.includes(value), value);
  assert.match(renderRecovery(false), /Recovery requires cluster update permission/);
  assert.match(renderRecovery(false), /<button[^>]*disabled=""[^>]*>Recover quorum/);
});
