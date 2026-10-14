// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { deleteWithReconciliation } from './storage-outcome.ts';

const notFound = (error: unknown) => error === 'missing';

test('successful deletion does not need a second read', async () => {
  let reads = 0;
  await deleteWithReconciliation(async () => {}, async () => { reads++; }, notFound);
  assert.equal(reads, 0);
});

test('lost deletion response is successful when the storage is confirmed absent', async () => {
  await deleteWithReconciliation(async () => { throw new TypeError('network'); }, async () => { throw 'missing'; }, notFound);
});

test('lost deletion response reports a confirmed surviving storage', async () => {
  await assert.rejects(deleteWithReconciliation(async () => { throw new TypeError('network'); }, async () => ({}), notFound), /storage still exists/);
});

test('reconciliation outage is explicitly unknown rather than fabricated success', async () => {
  await assert.rejects(deleteWithReconciliation(async () => { throw new TypeError('network'); }, async () => { throw new TypeError('offline'); }, notFound), /outcome is not yet known/);
});

test('definite server refusals keep their original field and resource details', async () => {
  const conflict = { status: 409, details: { resource: 'backup' } };
  let reads = 0;
  await assert.rejects(deleteWithReconciliation(async () => { throw conflict; }, async () => { reads++; }, notFound), (error) => error === conflict);
  assert.equal(reads, 0);
});

test('already absent storage and gateway failure are reconciled honestly', async () => {
  await deleteWithReconciliation(async () => { throw 'missing'; }, async () => { throw new Error('must not read'); }, notFound);
  await deleteWithReconciliation(async () => { throw { status: 502 }; }, async () => { throw 'missing'; }, notFound);
});
