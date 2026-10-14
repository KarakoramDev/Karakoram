// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { idempotencyHeaders } from './idempotency.ts';

test('idempotency retry headers retain the exact submission key', () => {
  const key = crypto.randomUUID();
  assert.deepEqual(idempotencyHeaders(key), { 'Idempotency-Key': key });
  assert.deepEqual(idempotencyHeaders(key), idempotencyHeaders(key));
  assert.notEqual(idempotencyHeaders(crypto.randomUUID())['Idempotency-Key'], key);
});
