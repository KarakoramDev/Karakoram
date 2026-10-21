// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '@/api/client';
import type { ClusterContext } from './ClusterDetail';
import { buildDataMcpPatch, dataMcpConnectCommands, dataMcpDraft, dataMcpEndpoint, dataMcpView, scopeFields, supportsDataMcp, validateDataMcpDraft } from './datamcp.ts';

register('../../../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before loader registration; TSX must load afterward.
const { DataMcpSection } = await import('./DataMcpSection.tsx');
const { OverviewTab } = await import('./OverviewTab.tsx');
const { ToastProvider } = await import('../../../components/ui/Toast.tsx');

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 10, maxDisk: 20 };

function cluster(provider: string, dataMcp?: Schemas['DataMcpSpec'], status?: Schemas['DataMcpStatus']): Schemas['Cluster'] {
  return {
    name: 'db', namespace: 'team', generation: 1, createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z',
    spec: { provider, version: '1.0.0', topology: { type: 'replicaSet' }, components: { engine: { replicas: 1, resources } }, ...(dataMcp ? { dataMcp } : {}) },
    status: { phase: 'Ready', observedGeneration: 1, members: [], conditions: [], ...(status ? { dataMcp: status } : {}) },
  };
}

function render(element = createElement(DataMcpSection), value = cluster('psmdb'), allowed = true): string {
  const context: ClusterContext = { cluster: value, provider: undefined, namespace: value.namespace, name: value.name, base: `/clusters/${value.namespace}/${value.name}`, can: () => allowed };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context }), children: [{ index: true, element }] }]);
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
  } finally {
    router.dispose();
    client.clear();
  }
}

test('every shipped engine gets the data MCP section; unknown providers do not', () => {
  for (const provider of ['psmdb', 'postgres', 'supabase', 'elasticsearch', 'rabbitmq']) assert.equal(supportsDataMcp(provider), true, provider);
  assert.equal(supportsDataMcp('redis'), false);
  assert.match(render(createElement(OverviewTab), cluster('rabbitmq')), /Data MCP/);
  assert.doesNotMatch(render(createElement(OverviewTab), cluster('redis')), /Data MCP/);
});

test('only the engine-relevant scope field renders', () => {
  const expected: Record<string, string[]> = {
    psmdb: ['Databases'],
    postgres: ['Databases', 'Schemas'],
    supabase: ['Databases', 'Schemas'],
    elasticsearch: ['Index patterns'],
    rabbitmq: ['Virtual hosts'],
  };
  const all = ['Databases', 'Schemas', 'Index patterns', 'Virtual hosts'];
  for (const [provider, labels] of Object.entries(expected)) {
    assert.deepEqual(scopeFields(provider).map((f) => f.label), labels, provider);
    const markup = render(createElement(DataMcpSection), cluster(provider));
    for (const label of all) assert.equal(markup.includes(`>${label}</label>`), labels.includes(label), `${provider}: ${label}`);
    assert.match(markup, /Denied tools/);
    assert.match(markup, /Require confirmation for write tools/);
    assert.match(markup, /Record query text in audit events/);
  }
});

test('status reads Disabled without a spec and reflects the observed backend when enabled', () => {
  assert.deepEqual(dataMcpView(undefined, undefined), { enabled: false, phase: 'Disabled', tone: 'neutral', message: undefined });
  assert.equal(dataMcpView({ enabled: false }, { phase: 'Ready', access: 'read' }).phase, 'Disabled');
  assert.deepEqual(dataMcpView({ enabled: true, access: 'readWrite' }, undefined), { enabled: true, phase: 'Provisioning', tone: 'progress', kind: undefined, upstream: undefined, access: 'readWrite', message: undefined });
  const ready = dataMcpView({ enabled: true }, { phase: 'Ready', kind: 'companion', upstream: 'mongodb-mcp-server', upstreamVersion: '1.2.3', access: 'read' });
  assert.equal(ready.tone, 'ok');
  assert.equal(ready.upstream, 'mongodb-mcp-server 1.2.3');
  assert.equal(dataMcpView({ enabled: true }, { phase: 'Degraded', message: 'companion unreachable' }).tone, 'warn');

  const disabled = render(createElement(DataMcpSection), cluster('psmdb'));
  assert.match(disabled, /badge[^>]*>Disabled</);
  assert.match(disabled, /Data access through MCP is off/);
  const markup = render(createElement(DataMcpSection), cluster('psmdb', { enabled: true }, { phase: 'Ready', kind: 'companion', upstream: 'mongodb-mcp-server', upstreamVersion: '1.2.3', access: 'read', message: 'serving' }));
  assert.match(markup, /badge badge-ok[^>]*>Ready</);
  assert.match(markup, /Companion service · <span class="mono small">mongodb-mcp-server 1.2.3<\/span>/);
  assert.match(markup, /read-only/);
  assert.match(markup, /Message<\/dt><dd><span class="small">serving/);
  const native = render(createElement(DataMcpSection), cluster('elasticsearch', { enabled: true }, { phase: 'Provisioning', kind: 'native', upstream: 'native-elasticsearch' }));
  assert.match(native, /badge badge-progress[^>]*>Provisioning</);
  assert.match(native, /Native toolset/);
});

test('endpoint and connect commands are cluster-scoped; the write variant needs provisioned writes', () => {
  assert.equal(dataMcpEndpoint('https://kk.example', 'team', 'db'), 'https://kk.example/mcp/clusters/team/db');
  assert.deepEqual(dataMcpConnectCommands('team', 'db', 'read'), ['karactl mcp config --cluster team/db']);
  assert.deepEqual(dataMcpConnectCommands('team', 'db', undefined), ['karactl mcp config --cluster team/db']);
  assert.deepEqual(dataMcpConnectCommands('team', 'db', 'readWrite'), ['karactl mcp config --cluster team/db', 'karactl mcp config --cluster team/db --write']);

  const read = render(createElement(DataMcpSection), cluster('postgres', { enabled: true, access: 'readWrite' }, { phase: 'Ready', access: 'read' }));
  assert.match(read, /\/mcp\/clusters\/team\/db</);
  assert.match(read, /karactl mcp config --cluster team\/db</);
  assert.doesNotMatch(read, /--write/);
  assert.match(read, /cluster-data<\/span> permission/);
  assert.match(read, /KARAKORAM_MCP=readwrite/);
  assert.ok((read.match(/Copy to clipboard/g) ?? []).length >= 2, 'endpoint and command have copy buttons');
  const write = render(createElement(DataMcpSection), cluster('postgres', { enabled: true, access: 'readWrite' }, { phase: 'Ready', access: 'readWrite' }));
  assert.match(write, /karactl mcp config --cluster team\/db --write</);
  assert.match(write, /read and write/);
});

test('merge patch carries only changed dataMcp fields and clears lists with null', () => {
  const persisted: Schemas['DataMcpSpec'] = { enabled: true, access: 'read', scope: { databases: ['app'], schemas: ['public'] }, tools: { deny: ['drop'], confirmWrites: true }, auditQueryText: false };
  assert.deepEqual(buildDataMcpPatch(persisted, dataMcpDraft(persisted, 'postgres'), 'postgres'), {});
  assert.deepEqual(buildDataMcpPatch(undefined, dataMcpDraft(undefined, 'psmdb'), 'psmdb'), {});

  const draft = dataMcpDraft(persisted, 'postgres');
  draft.access = 'readWrite';
  draft.scope.schemas = [];
  draft.scope.databases = ['app', 'reports'];
  draft.deny = [];
  draft.auditQueryText = true;
  assert.deepEqual(buildDataMcpPatch(persisted, draft, 'postgres'), {
    access: 'readWrite',
    scope: { databases: ['app', 'reports'], schemas: null },
    tools: { deny: null },
    auditQueryText: true,
  });

  const enable = dataMcpDraft(undefined, 'rabbitmq');
  enable.enabled = true;
  enable.scope.vhosts = ['/'];
  enable.confirmWrites = true;
  assert.deepEqual(buildDataMcpPatch(undefined, enable, 'rabbitmq'), { enabled: true, scope: { vhosts: ['/'] }, tools: { confirmWrites: true } });

  // Scope keys of other engines are never part of the draft or the patch.
  const es = dataMcpDraft({ enabled: true, scope: { databases: ['stale'], indexPatterns: ['logs-*'] } }, 'elasticsearch');
  assert.deepEqual(Object.keys(es.scope), ['indexPatterns']);
  const disable = { ...es, enabled: false };
  assert.deepEqual(buildDataMcpPatch({ enabled: true, scope: { databases: ['stale'], indexPatterns: ['logs-*'] } }, disable, 'elasticsearch'), { enabled: false });
});

test('draft validation mirrors server limits and rejects hidden index patterns', () => {
  const es = dataMcpDraft({ enabled: true }, 'elasticsearch');
  es.scope.indexPatterns = ['.kibana*'];
  assert.match(validateDataMcpDraft(es, 'elasticsearch').join(' '), /leading dot/);
  es.scope.indexPatterns = ['logs-*', 'logs-*'];
  assert.match(validateDataMcpDraft(es, 'elasticsearch').join(' '), /unique/);
  es.scope.indexPatterns = ['logs-*'];
  es.deny = ['drop index'];
  assert.match(validateDataMcpDraft(es, 'elasticsearch').join(' '), /Denied tools/);
  es.deny = ['delete_index'];
  assert.deepEqual(validateDataMcpDraft(es, 'elasticsearch'), []);
  const many = dataMcpDraft(undefined, 'psmdb');
  many.scope.databases = Array.from({ length: 65 }, (_, i) => `db${i}`);
  assert.match(validateDataMcpDraft(many, 'psmdb').join(' '), /at most 64/);
});

test('settings are gated on the cluster update permission', () => {
  const viewer = render(createElement(DataMcpSection), cluster('psmdb', { enabled: true }, { phase: 'Ready', access: 'read' }), false);
  assert.match(viewer, /Data MCP settings are read-only/);
  assert.doesNotMatch(viewer, /Save settings/);
  assert.match(viewer, /<fieldset disabled=""/);
  assert.match(viewer, /karactl mcp config/, 'connection help stays visible to readers');
  const editor = render(createElement(DataMcpSection), cluster('psmdb', { enabled: true }, { phase: 'Ready', access: 'read' }), true);
  assert.match(editor, /Save settings/);
  assert.doesNotMatch(editor, /Data MCP settings are read-only/);
  assert.doesNotMatch(editor, /<fieldset disabled=""/);
});

test('write access and query-text audit render their warnings', () => {
  const rw = render(createElement(DataMcpSection), cluster('supabase', { enabled: true, access: 'readWrite', auditQueryText: true }));
  assert.match(rw, /Agents can modify data/);
  assert.match(rw, /Audit records will contain query text/);
  const ro = render(createElement(DataMcpSection), cluster('supabase', { enabled: true }));
  assert.doesNotMatch(ro, /Agents can modify data|Audit records will contain query text/);
});
