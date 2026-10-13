// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { dataSourceFor, dataSourceLabel, wireDataSource } from './data-source.ts';

test('wizard state always records the source namespace, even when it equals the target', () => {
  assert.deepEqual(dataSourceFor('team-a', 'orders'), { namespace: 'team-a', cluster: 'orders' });
  assert.deepEqual(dataSourceFor('team-a', 'orders', { backup: 'nightly-01' }), { namespace: 'team-a', cluster: 'orders', backup: 'nightly-01' });
  assert.deepEqual(dataSourceFor('team-a', 'orders', { pitrTime: '2026-10-01T00:00:00Z' }), { namespace: 'team-a', cluster: 'orders', pitrTime: '2026-10-01T00:00:00Z' });
  assert.deepEqual(dataSourceFor('team-a', 'orders', { backup: undefined, pitrTime: undefined }), { namespace: 'team-a', cluster: 'orders' });
});

test('changing the target namespace after picking a same-namespace source keeps the source where it was', () => {
  // Source picked while the target was team-a: same namespace, not sent on the wire.
  const picked = dataSourceFor('team-a', 'orders', { backup: 'nightly-01' });
  assert.deepEqual(wireDataSource(picked, 'team-a'), { cluster: 'orders', backup: 'nightly-01' });
  assert.equal(dataSourceLabel(picked, 'team-a'), 'orders');

  // Target moved to team-b: the request now names team-a explicitly instead of
  // silently resolving "orders" inside team-b.
  assert.deepEqual(wireDataSource(picked, 'team-b'), { cluster: 'orders', backup: 'nightly-01', namespace: 'team-a' });
  assert.equal(dataSourceLabel(picked, 'team-b'), 'team-a/orders');
});

test('wire shape omits the namespace only when it matches the target', () => {
  assert.deepEqual(wireDataSource({ namespace: 'shared', cluster: 'src', pitrTime: 't' }, 'team-a'), { cluster: 'src', pitrTime: 't', namespace: 'shared' });
  assert.deepEqual(wireDataSource({ namespace: 'team-a', cluster: 'src', pitrTime: 't' }, 'team-a'), { cluster: 'src', pitrTime: 't' });
  // A value seeded without a namespace is treated as same-namespace.
  assert.deepEqual(wireDataSource({ cluster: 'src', backup: 'b' }, 'team-a'), { cluster: 'src', backup: 'b' });
  assert.equal(dataSourceLabel({ cluster: 'src' }, 'team-a'), 'src');
});
