// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Backend, NamespaceForm } from './namespace-form';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Load JSX and aliased imports only after registering the source loader.
const { NamespaceBackendCell, NamespaceCapabilities, NamespaceBackendFields } = await import('./namespace-backend.tsx');
const { CreateNamespaceDialog } = await import('./Namespaces.tsx');
const { namespaceCreateBody, selectedBackend } = await import('./namespace-form.ts');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { keys } = await import('../../api/keys.ts');

const zerops: Backend = { kind: 'zerops', displayName: 'Zerops', experimental: false, relay: true, configKeys: [], capabilities: ['publicTcp'] };
const alternative: Backend = { kind: 'dokploy', displayName: 'Dokploy', experimental: true, relay: false, configKeys: ['endpoint', 'region'], capabilities: ['exec'] };
const form: NamespaceForm = { name: 'staging', backend: 'zerops', projectId: ' target-1 ', token: ' token-value ', description: ' staging target ', config: {} };
const unchanged = () => { assert.fail('Rendering must not change the form'); };

function fields(backends: Backend[], kind = 'zerops') {
  return renderToStaticMarkup(createElement(NamespaceBackendFields, {
    backends, backend: selectedBackend(backends, kind), form: { ...form, backend: kind }, onChange: unchanged,
  }));
}

test('namespace backend column uses registered display names, marks experimental backends and falls back to kind', () => {
  const cell = (kind: string, backends: Backend[]) => renderToStaticMarkup(createElement(NamespaceBackendCell, { kind, backends }));
  assert.match(cell('zerops', [zerops]), /^<td class="small">.*Zerops.*<\/td>$/);
  assert.match(cell('dokploy', [alternative]), /Dokploy.*badge-warn.*experimental/);
  assert.match(cell('custom-backend', [zerops]), /custom-backend/);
  assert.match(cell('zerops', []), /zerops/);
});

test('namespace details explicitly list all known capabilities, including unsupported capabilities', () => {
  const markup = renderToStaticMarkup(createElement(NamespaceCapabilities, { capabilities: ['publicTcp', 'exec'] }));
  const labels = ['Public TCP', 'TCP allowlist', 'Public HTTPS', 'Volume growth', 'Volume quota', 'Anti-affinity placement', 'Exec', 'Platform logs', 'Platform metrics', 'Scoped tokens', 'Dedicated CPU', 'Horizontal scaling', 'Env change restarts'];
  for (const label of labels) assert.ok(markup.includes(label), `${label} must always be visible`);
  assert.equal((markup.match(/>Supported</g) ?? []).length, 2);
  assert.equal((markup.match(/>Not supported</g) ?? []).length, 11);
  assert.match(markup, /Public TCP<\/dt><dd>.*?badge-ok.*?>Supported</);
  assert.match(markup, /TCP allowlist<\/dt><dd><span class="badge {2}badge-plain">Not supported</);
  const empty = renderToStaticMarkup(createElement(NamespaceCapabilities, { capabilities: [] }));
  assert.equal((empty.match(/>Not supported</g) ?? []).length, 13);
});

test('create defaults to Zerops with its existing credential wording and a registered backend select', () => {
  const markup = fields([zerops, alternative]);
  assert.match(markup, /<select[^>]*required/);
  assert.match(markup, /value="zerops" selected="">Zerops/);
  assert.match(markup, /Dokploy \(experimental\)/);
  assert.match(markup, /Zerops project ID/);
  assert.match(markup, /Project token/);
  assert.match(markup, /project-scoped Zerops access token/);
  assert.doesNotMatch(markup, /endpoint \(optional\)/);
});

test('a sole registered backend is read-only, uses generic labels and exposes optional config inputs', () => {
  const markup = fields([alternative]);
  assert.doesNotMatch(markup, /<select/);
  assert.match(markup, /readonly=""[^>]*value="Dokploy \(experimental\)"/);
  assert.match(markup, /Target ID/);
  assert.match(markup, /API token/);
  assert.match(markup, /endpoint \(optional\)/);
  assert.match(markup, /region \(optional\)/);
  assert.doesNotMatch(markup, /Zerops project ID|Project token/);
  assert.match(fields([zerops, alternative], 'dokploy'), /Target ID/);
});

test('namespace create payload includes backend and nonempty selected-backend config only', () => {
  assert.deepEqual(namespaceCreateBody({ ...form, backend: 'dokploy', config: { endpoint: ' https://api.example.com ', region: '  ', unrelated: 'ignored' } }, alternative), {
    name: 'staging', backend: 'dokploy', projectId: 'target-1', token: ' token-value ', description: 'staging target', config: { endpoint: 'https://api.example.com' },
  });
  assert.deepEqual(namespaceCreateBody({ ...form, description: ' ', config: { endpoint: ' ', region: '' } }, alternative), {
    name: 'staging', backend: 'dokploy', projectId: 'target-1', token: ' token-value ',
  });
  assert.deepEqual(namespaceCreateBody({ ...form, description: '', config: { endpoint: 'not-for-zerops' } }, zerops), {
    name: 'staging', backend: 'zerops', projectId: 'target-1', token: ' token-value ',
  });
});

test('create dialog explains relay deployment only for a relay backend', () => {
  for (const backend of [zerops, alternative]) {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(keys.backends, { items: [backend] });
    try {
      const markup = renderToStaticMarkup(createElement(QueryClientProvider, { client },
        createElement(ToastProvider, null, createElement(CreateNamespaceDialog, { onClose: unchanged }))));
      assert.equal(markup.includes('Karakoram deploys a relay service'), backend.relay);
      assert.equal(markup.includes('Zerops project ID'), backend.kind === 'zerops');
    } finally {
      client.clear();
    }
  }
});

test('create cannot select an unregistered default when multiple non-Zerops backends are registered', () => {
  const backends = [alternative, { ...alternative, kind: 'nomad', displayName: 'Nomad' }];
  assert.equal(selectedBackend(backends, 'zerops'), undefined);
  assert.equal(selectedBackend(backends, 'nomad')?.kind, 'nomad');
});
