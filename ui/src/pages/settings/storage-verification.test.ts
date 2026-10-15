// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { replaceStorageInList, storageFailureLabel } from './storage-verification.ts';
import type { Schemas } from '@/api/client';

test('every storage verification failure reason has a human-readable label', () => {
  assert.equal(storageFailureLabel('access_denied'), 'Access denied');
  assert.equal(storageFailureLabel('bucket_not_found'), 'Bucket not found');
  assert.equal(storageFailureLabel('endpoint_not_allowed'), 'Endpoint not allowed');
  assert.equal(storageFailureLabel('tls_error'), 'TLS verification failed');
  assert.equal(storageFailureLabel('unreachable'), 'Endpoint unreachable');
  assert.equal(storageFailureLabel('timeout'), 'Verification timed out');
  assert.equal(storageFailureLabel('unknown'), 'Unknown verification failure');
});

test('verification without a categorized failure adds no invented label', () => {
  assert.equal(storageFailureLabel(undefined), undefined);
});

const storage: Schemas['BackupStorage'] = {
  namespace: 'default', name: 'archive', type: 's3', endpointURL: 'https://s3.example.test',
  region: 'region', bucket: 'bucket', forcePathStyle: true, verifyTLS: true, credentialsConfigured: true,
};

test('a saved credential-only edit immediately clears a cached successful verification', () => {
  const previous = { ...storage, verification: { verified: true, checkedAt: '2026-01-01T00:00:00Z', message: 'Read, write and delete access verified' } };
  const otherNamespace = { ...previous, namespace: 'production' };
  const current = { items: [previous, otherNamespace], nextCursor: 'next-page' };
  const updated = replaceStorageInList(current, storage)!;
  assert.equal(updated.items[0], storage);
  assert.equal(updated.items[0]!.verification, undefined);
  assert.equal(updated.items[1], otherNamespace);
  assert.equal(updated.nextCursor, 'next-page');
  assert.equal(current.items[0]!.verification.verified, true);
});

test('description-only saves preserve the server verification result', () => {
  const saved = { ...storage, description: 'Updated', verification: { verified: true, checkedAt: '2026-01-01T00:00:00Z', message: 'Access verified' } };
  assert.equal(replaceStorageInList({ items: [storage] }, saved)!.items[0], saved);
  assert.equal(replaceStorageInList(undefined, saved), undefined);
});
