// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { accountNameError, passwordError, hasAdminAssignment, loginFailureMessage, PASSWORD_CHANGED_MESSAGE } from './validation.ts';

test('account names use the server alphabet and 64-character bound', () => {
  for (const name of ['a', 'Admin@example.org', 'a_b.c-d', 'a'.repeat(64)]) assert.equal(accountNameError(name), undefined);
  for (const name of ['', 'bad name', '1reader', 'a'.repeat(65)]) assert.match(accountNameError(name)!, /1–64/);
});

test('password policy checks Unicode character and byte bounds and rejects reuse', () => {
  assert.match(passwordError('short')!, /12/);
  assert.match(passwordError('x'.repeat(1025))!, /1024/);
  assert.equal(passwordError('😀'.repeat(12)), undefined);
  assert.match(passwordError('a-long-password', 'a-long-password')!, /differ/);
  assert.match(PASSWORD_CHANGED_MESSAGE, /Password changed.*new password/);
});

test('login throttling displays server retry seconds', () => {
  assert.match(loginFailureMessage('Login throttled', { retryAfterSeconds: 42 }), /42 seconds/);
  assert.equal(loginFailureMessage('Invalid credentials', undefined), 'Invalid credentials');
});

test('lockout warning detects missing admin bindings, inherited roles and cycles', () => {
  assert.equal(hasAdminAssignment('g, user:admin, role:admin'), true);
  assert.equal(hasAdminAssignment('g, group:admins, role:custom\ng, role:custom, role:admin'), true);
  assert.equal(hasAdminAssignment('# g, user:admin, role:admin\ng, user:reader, role:viewer'), false);
  assert.equal(hasAdminAssignment('g, user:reader, role:a\ng, role:a, role:b\ng, role:b, role:a'), false);
});
