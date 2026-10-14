// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { tokenNameError, tokenExpiry, tokenSubmission } from './token-form.ts';

test('token form explains blank and oversized names with the API limit', () => {
  for (const name of ['', ' \t']) assert.match(tokenNameError(name)!, /required/);
  assert.match(tokenNameError('x'.repeat(129))!, /128/);
  assert.equal(tokenNameError('  nightly-automation  '), undefined);
  assert.equal(tokenNameError('😀'.repeat(128)), undefined);
});

test('expiry validation never throws on malformed dates and requires future values', () => {
  const now = Date.parse('2026-01-01T00:00:00Z');
  assert.deepEqual(tokenExpiry('', now), {});
  assert.match(tokenExpiry('invalid', now).error!, /valid date/);
  assert.match(tokenExpiry('2020-01-01T00:00:00Z', now).error!, /future/);
  assert.match(tokenExpiry('2026-01-01T00:00:00Z', now).error!, /future/);
  assert.equal(tokenExpiry('2027-01-01T00:00:00Z', now).expiresAt, '2027-01-01T00:00:00.000Z');
});

test('double-click guard is synchronous and retries reuse the body-bound key', () => {
  let generated = 0;
  const submission = tokenSubmission(() => `id-${++generated}`);
  const first = submission.begin(' automation ', '2027-01-01T00:00:00Z');
  assert.equal(submission.begin('automation', '2027-01-01T00:00:00Z'), null);
  submission.release();
  assert.deepEqual(submission.begin('automation', '2027-01-01T00:00:00Z'), first);
  submission.release();
  assert.notEqual(submission.begin('automation', '2028-01-01T00:00:00Z')!.key, first!.key);
  assert.equal(generated, 2);
});
