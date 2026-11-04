// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { readFileSync } from 'node:fs';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { Schemas } from '../../api/client';
register('../../../test/tsx-loader.mjs', import.meta.url);
// TSX registry imports must wait until the Node loader is registered.
await import('../register.ts');
const { engines } = await import('../index.ts');
const { weaviateUI } = await import('./index.tsx');
const { default: WizardSection } = await import('./WizardSection.tsx');
test('Weaviate registry attaches wizard and capability-gated native access', () => {
  assert.equal(engines.weaviate, weaviateUI);
  assert.ok(weaviateUI.wizardSection);
  assert.deepEqual(weaviateUI.detailTabs?.map(({ id, label, capability }) => ({ id, label, capability })), [{ id: 'weaviate', label: 'Weaviate access', capability: 'weaviateAccess' }]);
});
test('secret calls avoid React Query caches and native access exposes each operation', () => {
  const source = readFileSync(new URL('./AccessTab.tsx', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /useMutation|localStorage|sessionStorage|console\./);
  for (const fragment of ['rotate-key', 'parsePermissions(permissions)', "can('update', 'clusters')", 'protectedUser(user)', 'BUILTIN_ROLES.includes', 'Copy API key', 'lastUsedAt', 'ConfirmDialog']) assert.ok(source.includes(fragment), fragment);
});
test('key rotation gates both its control and handler on privileged-role cluster authority', () => {
  const source = readFileSync(new URL('./AccessTab.tsx', import.meta.url), 'utf8');
  assert.match(source, /disabled=\{!can\('update', 'database-users'\) \|\| \(privilegedRoles\(user\.roles\) && !can\('update', 'clusters'\)\) \|\| busy\} onClick=\{\(\) => \{ void run/);
  assert.match(source, /if \(!can\('update', 'database-users'\) \|\| \(privilegedRoles\(user\.roles\) && !can\('update', 'clusters'\)\)\) return;\s*const result = await unwrap\(api\.POST\('\/clusters\/\{cluster\}\/weaviate\/users\/\{user\}\/rotate-key'/);
});
test('wizard renders module choices, inference URLs and bounded options', () => {
  const spec: Schemas['ClusterSpec'] = {
    provider: 'weaviate', version: '1.40.1', topology: { type: 'single' },
    components: { engine: { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 1, minRam: 2, maxRam: 2, minDisk: 10, maxDisk: 10 } } },
  };
  const provider = { name: 'weaviate' } as Schemas['Provider'];
  const markup = renderToStaticMarkup(createElement(WizardSection, { provider, spec, onChange: () => {} }));
  for (const text of ['Replication minimum factor', 'Query defaults limit', 'Disk warning percent', 'Disk readonly percent', 'Async replication', 'text2vec-openai', 'text2vec-transformers', 'backup-s3', 'isolated private network']) assert.ok(markup.includes(text), text);
  assert.doesNotMatch(markup, /name="engineConfig"/);
});
