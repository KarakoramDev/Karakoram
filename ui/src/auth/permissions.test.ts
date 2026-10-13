// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { evaluate } from './permissions.ts';

test('scoped audit grants expose the entry point without granting global audit', () => {
  const permissions = [{ resource: 'audit', action: 'read', object: 'dev/**', effect: 'allow' as const }];
  assert.equal(evaluate(permissions, 'audit', 'read'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'dev'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'dev/orders'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'prod/orders'), false);
  assert.equal(evaluate(permissions, 'audit', 'read', '*'), false);
});

test('legacy audit objects stay in default and namespace metadata stays separate', () => {
  const permissions = [{ resource: 'audit', action: 'read', object: 'orders', effect: 'allow' as const }];
  assert.equal(evaluate(permissions, 'audit', 'read', 'default/orders'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'dev/orders'), false);
  assert.equal(evaluate(permissions, 'namespaces', 'read', 'default'), false);
  assert.equal(evaluate(permissions, 'events', 'read', 'default/orders'), false);
});

test('global audit grants and namespace-scoped denies compose per object', () => {
  const permissions = [
    { resource: 'audit', action: 'read', object: '*', effect: 'allow' as const },
    { resource: 'audit', action: 'read', object: 'prod/**', effect: 'deny' as const },
  ];
  assert.equal(evaluate(permissions, 'audit', 'read', '*'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'dev/orders'), true);
  assert.equal(evaluate(permissions, 'audit', 'read', 'prod'), false);
  assert.equal(evaluate(permissions, 'audit', 'read', 'prod/orders'), false);
  assert.equal(evaluate(permissions, 'audit', 'read'), true);
});
