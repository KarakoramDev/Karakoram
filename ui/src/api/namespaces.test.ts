// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fetchAllNamespaces } from './namespaces.ts';

test('namespace enumeration follows opaque cursors and exposes every page', async () => {
  const cursors: (string | undefined)[] = [];
  const result = await fetchAllNamespaces(async (cursor) => {
    cursors.push(cursor);
    if (cursor === undefined) return { items: [{ name: 'dev' }], nextCursor: 'page/+two=' };
    assert.equal(cursor, 'page/+two=');
    return { items: [{ name: 'prod' }] };
  });
  assert.deepEqual(cursors, [undefined, 'page/+two=']);
  assert.deepEqual(result, { items: [{ name: 'dev' }, { name: 'prod' }] });
});

test('namespace enumeration follows an empty page with a cursor and stops on an empty cursor', async () => {
  const cursors: (string | undefined)[] = [];
  const result = await fetchAllNamespaces(async (cursor) => {
    cursors.push(cursor);
    if (cursor === undefined) return { items: [], nextCursor: 'second' };
    assert.equal(cursor, 'second');
    return { items: ['prod'], nextCursor: '' };
  });
  assert.deepEqual(cursors, [undefined, 'second']);
  assert.deepEqual(result, { items: ['prod'] });
});

test('namespace enumeration preserves an empty authorized collection', async () => {
  let requests = 0;
  const result = await fetchAllNamespaces(async () => {
    requests++;
    return { items: [] };
  });
  assert.equal(requests, 1);
  assert.deepEqual(result, { items: [] });
});

test('namespace enumeration rejects a later page failure instead of returning partial choices', async () => {
  const failure = new Error('namespace read denied');
  const cursors: (string | undefined)[] = [];
  await assert.rejects(fetchAllNamespaces(async (cursor) => {
    cursors.push(cursor);
    if (cursor === undefined) return { items: ['dev'], nextCursor: 'second' };
    throw failure;
  }), (error) => error === failure);
  assert.deepEqual(cursors, [undefined, 'second']);
});
