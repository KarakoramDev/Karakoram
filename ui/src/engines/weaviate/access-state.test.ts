// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BUILTIN_ROLES, parsePermissions, privilegedRoles, protectedUser, secretState } from './access-state.ts';
test('secret reveal is ephemeral and clears instead of retaining previous keys', () => {
  let secret = secretState(null, { type: 'reveal', key: 'first' });
  assert.equal(secret, 'first');
  secret = secretState(secret, { type: 'clear' });
  assert.equal(secret, null);
  secret = secretState(secret, { type: 'reveal', key: 'second' });
  assert.equal(secret, 'second');
  assert.equal(secretState(secret, { type: 'clear' }), null);
});
test('system and environment identities and built-in roles are read-only', () => {
  const user = { name: 'application', userType: 'db_user', active: true, roles: ['viewer'] };
  assert.equal(protectedUser(user), false);
  for (const name of ['karakoram-admin', 'karakoram-cluster']) assert.equal(protectedUser({ ...user, name }), true);
  assert.equal(protectedUser({ ...user, userType: 'db_env_user' }), true);
  assert.deepEqual(BUILTIN_ROLES, ['root', 'admin', 'viewer', 'read-only', 'operator_metadata_reader']);
  assert.equal(privilegedRoles(['viewer', 'read-only']), false);
  assert.equal(privilegedRoles(['custom-reader']), true);
});
test('permission editor validates array shape and required actions without narrowing native selectors', () => {
  const permissions = [{ action: 'read_data', data: { collection: 'Articles', tenant: '*', object: '*' } }];
  assert.deepEqual(parsePermissions(JSON.stringify(permissions)), permissions);
  assert.deepEqual(parsePermissions('[]'), []);
  for (const text of ['{', '{}', '[null]', '[[]]', '[{}]', '[{"action":""}]', '[{"action":5}]']) assert.throws(() => parsePermissions(text));
});
