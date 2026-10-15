// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { applicationDrafts, applicationGrants, forgetInvalidation, invalidationState, INVALIDATION_WAIT_MS, pendingInvalidations, rememberInvalidation } from './access-state.ts';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before the JSX/alias loader is registered.
const { ElasticEndpoints } = await import('./ElasticAccessTab.tsx');

class MemoryStorage {
  private items = new Map<string, string>();
  get length() { return this.items.size; }
  key(index: number) { return [...this.items.keys()][index] ?? null; }
  getItem(key: string) { return this.items.get(key) ?? null; }
  setItem(key: string, value: string) { this.items.set(key, value); }
  removeItem(key: string) { this.items.delete(key); }
}

test('application grants preserve existing roles and support adding, editing and removing grants', () => {
  const original = [{ application: 'kibana-.kibana', privileges: ['feature_discover.read', 'feature_dashboard.read'], resources: ['space:default', 'space:team'] }];
  const drafts = applicationDrafts(original);
  assert.deepEqual(applicationGrants(drafts), original);
  drafts.push({ application: ' custom-app ', privileges: 'read, write\nmonitor', resources: '*\nspace:other' });
  assert.deepEqual(applicationGrants(drafts)?.[1], { application: 'custom-app', privileges: ['read', 'write', 'monitor'], resources: ['*', 'space:other'] });
  drafts[0] = { ...drafts[0]!, privileges: 'feature_discover.all', resources: 'space:other' };
  assert.deepEqual(applicationGrants(drafts)?.[0], { application: 'kibana-.kibana', privileges: ['feature_discover.all'], resources: ['space:other'] });
  assert.deepEqual(applicationGrants(drafts.filter((_, i) => i !== 0))?.length, 1);
  assert.deepEqual(applicationGrants([]), []);
  assert.equal(original[0]!.privileges[0], 'feature_discover.read');
});

test('incomplete application grants cannot be submitted', () => {
  for (const draft of [
    { application: '', privileges: 'read', resources: '*' },
    { application: 'kibana-.kibana', privileges: ' ,\n ', resources: '*' },
    { application: 'kibana-.kibana', privileges: 'read', resources: '' },
  ]) assert.equal(applicationGrants([draft]), null);
});

test('invalidation intent survives reload scoped to namespace, cluster and key id', () => {
  const storage = new MemoryStorage();
  const marker = { id: 'id:/one', startedAt: 1000 };
  rememberInvalidation(storage, 'team', 'search', marker);
  rememberInvalidation(storage, 'other', 'search', { id: 'other', startedAt: 1000 });
  rememberInvalidation(storage, 'team', 'other', { id: 'elsewhere', startedAt: 1000 });
  const reloaded = pendingInvalidations(storage, 'team', 'search');
  assert.deepEqual(reloaded, [marker]);
  assert.equal(invalidationState(reloaded[0], { invalidated: false }, 2000), 'checking');
  assert.equal(invalidationState(reloaded[0], { invalidated: true }, 2000), 'invalidated');
  forgetInvalidation(storage, 'team', 'search', marker.id);
  assert.deepEqual(pendingInvalidations(storage, 'team', 'search'), []);
  assert.equal(storage.length, 2);
});

test('unconfirmed invalidation offers retry after the bounded wait, and retry renews intent', () => {
  const storage = new MemoryStorage();
  const marker = { id: 'key', startedAt: 1000 };
  assert.equal(invalidationState(marker, { invalidated: false }, marker.startedAt + INVALIDATION_WAIT_MS), 'retry');
  assert.equal(invalidationState(marker, undefined, marker.startedAt + INVALIDATION_WAIT_MS), 'invalidated');
  rememberInvalidation(storage, 'team', 'search', marker);
  rememberInvalidation(storage, 'team', 'search', { ...marker, startedAt: 100_000 });
  const retried = pendingInvalidations(storage, 'team', 'search');
  assert.equal(retried.length, 1);
  assert.equal(invalidationState(retried[0], { invalidated: false }, 100_001), 'checking');
  assert.equal(invalidationState(undefined, { invalidated: false }, 100_001), 'active');
});

test('access panel renders Kibana startup instead of an actionable link until every Kibana member is ready', () => {
  const connection = { hosts: [], uri: '', endpoints: [{ name: 'Kibana', url: 'https://dashboards.example', public: true }] };
  const ready = { role: 'kibana' as const, state: 'joined', health: true };
  const starting = { ...ready, state: 'starting', health: false };
  for (const members of [[], [starting], [ready, starting]]) {
    const markup = renderToStaticMarkup(createElement(ElasticEndpoints, { connection, members }));
    assert.match(markup, /Kibana is starting—retry shortly/);
    assert.doesNotMatch(markup, /href="https:\/\/dashboards\.example"/);
  }
  const markup = renderToStaticMarkup(createElement(ElasticEndpoints, { connection, members: [ready] }));
  assert.match(markup, /href="https:\/\/dashboards\.example"/);
  assert.doesNotMatch(markup, /Kibana is starting/);
});

test('role and user dialogs keep native errors visible, and submit edited application grants', () => {
  const source = readFileSync(new URL('./ElasticAccessTab.tsx', import.meta.url), 'utf8');
  assert.match(source, /save\.error \? <Callout tone="danger">\{errorMessage\(save\.error\)\}/);
  assert.match(source, /error \? <Callout tone="danger">\{error\}/);
  assert.match(source, /applications: grants \?\? \[\]/);
  assert.match(source, /DLS\/FLS requires an Elasticsearch licence tier/);
});
