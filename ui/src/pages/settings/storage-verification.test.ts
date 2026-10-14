// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { storageFailureLabel } from './storage-verification.ts';

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
