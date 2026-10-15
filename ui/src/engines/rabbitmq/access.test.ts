// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { register } from 'node:module';
import createClient from 'openapi-fetch';
import type { paths } from '../../api/schema';
import { fetchBackupInventory } from '../../pages/clusters/detail/backup-inventory.ts';
import { latestSuccessfulBackup } from '../../pages/clusters/detail/detail-state.ts';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static TSX imports run before this test installs its JSX loader; this import
// intentionally crosses the loader boundary after registration.
const { SYSTEM_USERS, SYSTEM_USER_EXPLANATION } = await import('./AccessEditor.tsx');

test('system user copy limits rotation to karakoram and admin and distinguishes reserved names', () => {
  assert.deepEqual([...SYSTEM_USERS].sort(), ['admin', 'guest', 'karakoram', 'monitor']);
  assert.match(SYSTEM_USER_EXPLANATION, /Only karakoram and admin.*rotate them with system-users rotate/);
  assert.match(SYSTEM_USER_EXPLANATION, /guest and monitor are reserved names that cannot be created or modified/);
  assert.match(SYSTEM_USER_EXPLANATION, /user and grant changes are protected/);
});

test('typed access client serializes raw virtual host names exactly once', async () => {
  const routes = [
    '/clusters/{cluster}/rabbitmq/vhosts/{vhost}',
    '/clusters/{cluster}/rabbitmq/permissions/{vhost}/{user}',
    '/clusters/{cluster}/rabbitmq/topic-permissions/{vhost}/{user}',
    '/clusters/{cluster}/rabbitmq/policies/{vhost}/{name}',
    '/clusters/{cluster}/rabbitmq/operator-policies/{vhost}/{name}',
  ] as const;
  for (const vhost of ['/', '%2F', 'app/events']) {
    for (const route of routes) {
      const requests: Request[] = [];
      const client = createClient<paths>({ baseUrl: 'https://example.test/api/v1', fetch: async (request) => {
        requests.push(request);
        return request.method === 'DELETE' ? new Response(null, { status: 204 }) : new Response('{}', { headers: { 'Content-Type': 'application/json' } });
      } });
      const params = { path: { cluster: 'brokers', vhost, user: 'app', name: 'app' } };
      const body = { name: route.includes('/vhosts/') ? vhost : 'app', vhost, user: 'app', tags: [], configure: '', write: '', read: '', exchange: 'events', pattern: '.*', applyTo: 'queues' as const, priority: 0, definition: {} };
      await client.GET(route, { params });
      await client.PUT(route, { params, body });
      await client.DELETE(route, { params });
      const expected = '/api/v1' + route.replace('{cluster}', 'brokers').replace('{vhost}', encodeURIComponent(vhost)).replace('{user}', 'app').replace('{name}', 'app');
      assert.equal(requests.length, 3);
      for (const request of requests) assert.equal(new URL(request.url).pathname, expected);
      assert.equal((await requests[1]!.json()).vhost, vhost);
    }
  }
});

test('definitions backup inventory finds the last successful record beyond the first page', async () => {
  const successful = { name: 'definitions-saved', type: 'definitions', phase: 'Succeeded', completedAt: '2026-01-01T00:00:00Z' };
  const cursors: (string | undefined)[] = [];
  const inventory = await fetchBackupInventory(async (cursor) => {
    cursors.push(cursor);
    if (!cursor) return { items: [{ name: 'definitions-failed', type: 'definitions', phase: 'Failed', completedAt: '2026-01-03T00:00:00Z' }], nextCursor: 'page2' };
    if (cursor === 'page2') return { items: [{ name: 'definitions-pending', type: 'definitions', phase: 'Pending', completedAt: '2026-01-02T00:00:00Z' }], nextCursor: 'page3' };
    return { items: [successful] };
  });
  assert.deepEqual(cursors, [undefined, 'page2', 'page3']);
  assert.equal(latestSuccessfulBackup(inventory.items), successful);
});
