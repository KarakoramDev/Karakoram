// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { buildTokenRestrictions, parseTokenList, tokenListError, tokenNameError, tokenExpiry, tokenRestrictionSummary, tokenSubmission } from './token-form.ts';

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

const emptyRestrictions = {
  readOnly: false, namespaces: '', clusters: '', permissions: '', allowCredentialReveal: false,
};

test('restriction lists accept commas and newlines, trim and deduplicate', () => {
  assert.deepEqual(parseTokenList(' production, staging\r\nproduction\n , \t'), ['production', 'staging']);
  assert.deepEqual(parseTokenList(' \n, '), []);
});

test('restriction lists validate namespace, cluster and permission syntax', () => {
  assert.equal(tokenListError('namespaces', ['default', 'production', 'team-1']), undefined);
  assert.equal(tokenListError('clusters', ['default/db', 'production/mongo1']), undefined);
  assert.equal(tokenListError('permissions', ['clusters:read', 'cluster-data:*']), undefined);
  for (const value of ['*', 'Upper', 'a', 'bad space', 'bad/', 'end-', 'a'.repeat(33)]) {
    assert.match(tokenListError('namespaces', [value])!, /Invalid namespace/);
  }
  for (const value of ['db', 'production/', '/db', 'production/db/extra', 'production/a', 'production/db-name', 'production/abcdefghijklm']) {
    assert.match(tokenListError('clusters', [value])!, /Invalid cluster/);
  }
  for (const value of ['clusters', '*:read', 'clusters:', 'clusters:read:write', 'clusters:r*', 'clusters:read write']) {
    assert.match(tokenListError('permissions', [value])!, /Invalid permission/);
  }
});

test('empty restrictions are omitted and invalid lists cannot build restrictions', () => {
  assert.deepEqual(buildTokenRestrictions(emptyRestrictions), { errors: {} });
  assert.deepEqual(buildTokenRestrictions({ ...emptyRestrictions, namespaces: ' , \n' }), { errors: {} });
  const invalid = buildTokenRestrictions({ ...emptyRestrictions, namespaces: '*', clusters: 'db', permissions: 'read' });
  assert.equal(invalid.restrictions, undefined);
  assert.deepEqual(Object.keys(invalid.errors), ['namespaces', 'clusters', 'permissions']);
});

test('building restrictions preserves explicit reveal choice and parsed scopes', () => {
  assert.deepEqual(buildTokenRestrictions({ ...emptyRestrictions, readOnly: true }), {
    restrictions: { readOnly: true, allowCredentialReveal: false }, errors: {},
  });
  assert.deepEqual(buildTokenRestrictions({ ...emptyRestrictions, allowCredentialReveal: true }), {
    restrictions: { readOnly: false, allowCredentialReveal: true }, errors: {},
  });
  assert.deepEqual(buildTokenRestrictions({
    ...emptyRestrictions, namespaces: 'production, staging', clusters: 'production/db\nproduction/db', permissions: 'clusters:read,backups:*',
  }), {
    restrictions: { readOnly: false, allowCredentialReveal: false, namespaces: ['production', 'staging'], clusters: ['production/db'], permissions: ['clusters:read', 'backups:*'] },
    errors: {},
  });
});

test('restriction summaries distinguish unrestricted tokens and reveal denial', () => {
  assert.equal(tokenRestrictionSummary(), 'Unrestricted');
  assert.equal(tokenRestrictionSummary({}), 'No credential reveal');
  assert.equal(tokenRestrictionSummary({ readOnly: true, namespaces: ['production', 'staging'], clusters: ['production/db'], permissions: ['clusters:read'] }), 'Read-only · 2 namespaces · 1 cluster · 1 permission · No credential reveal');
  assert.equal(tokenRestrictionSummary({ allowCredentialReveal: true }), 'Credential reveal allowed');
});

test('restriction changes rotate the body-bound key and snapshot submitted scopes', () => {
  let generated = 0;
  const submission = tokenSubmission(() => `id-${++generated}`);
  const restrictions = { readOnly: true, namespaces: ['production'] };
  const first = submission.begin('automation', undefined, restrictions)!;
  assert.equal(submission.begin('automation', undefined, restrictions), null);
  submission.release();
  assert.deepEqual(submission.begin('automation', undefined, { readOnly: true, namespaces: ['production'] }), first);
  submission.release();
  restrictions.namespaces.push('staging');
  assert.deepEqual(first.restrictions?.namespaces, ['production']);
  const changed = submission.begin('automation', undefined, restrictions)!;
  assert.notEqual(changed.key, first.key);
  submission.release();
  const unrestricted = submission.begin('automation')!;
  assert.notEqual(unrestricted.key, changed.key);
  assert.equal('restrictions' in unrestricted, false);
  assert.equal(generated, 3);
});
