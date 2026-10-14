// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { isCancellable, operationPath, operationTargetLink } from './operations.ts';
import { clusterObject, evaluate } from '../auth/permissions.ts';

test('operation detail links encode the entire ID as one path segment', () => {
  assert.equal(operationPath('op/a ?#'), '/operations/op%2Fa%20%3F%23');
});

test('cluster targets link to their target name in the operation namespace', () => {
  assert.equal(operationTargetLink({ namespace: 'team/a', cluster: 'old', target: { kind: 'cluster', name: 'new ?#' } }), '/clusters/team%2Fa/new%20%3F%23');
  assert.equal(operationTargetLink({ namespace: '', cluster: 'orders', target: { kind: 'cluster', name: 'orders' } }), '/clusters/default/orders');
});

test('backup targets link to the backup row of the owning cluster', () => {
  assert.equal(operationTargetLink({ namespace: 'team-a', cluster: 'orders', target: { kind: 'backup', name: 'nightly/01 #' } }), '/clusters/team-a/orders/backups#backup-nightly%2F01%20%23');
  assert.equal(operationTargetLink({ namespace: '', cluster: 'orders', target: { kind: 'backup', name: 'nightly' } }), '/clusters/default/orders/backups#backup-nightly');
});

test('targets without a supported resource route remain plain text', () => {
  for (const kind of ['restore', 'system-user', 'member', 'future-resource']) {
    assert.equal(operationTargetLink({ namespace: 'team-a', cluster: 'orders', target: { kind, name: 'sample' } }), undefined);
  }
  assert.equal(operationTargetLink({ namespace: 'team-a', cluster: '', target: { kind: 'backup', name: 'nightly' } }), undefined);
  assert.equal(operationTargetLink({ namespace: 'team-a', cluster: 'orders', target: { kind: 'cluster', name: '' } }), undefined);
});

test('only active operations with cluster-update permission offer cancellation', () => {
  for (const phase of ['Pending', 'Running'] as const) {
    assert.equal(isCancellable({ phase }, true), true);
    assert.equal(isCancellable({ phase, cancelRequested: false }, true), true);
    assert.equal(isCancellable({ phase, cancelRequested: true }, true), false);
    assert.equal(isCancellable({ phase }, false), false);
  }
  for (const phase of ['Succeeded', 'Failed', 'Cancelled'] as const) {
    assert.equal(isCancellable({ phase }, true), false);
    assert.equal(isCancellable({ phase, cancelRequested: true }, true), false);
  }
});

test('cancellation permission uses the namespace-qualified cluster, not the target', () => {
  const permissions = [{ resource: 'clusters', action: 'update', object: 'team-a/orders', effect: 'allow' as const }];
  assert.equal(isCancellable({ phase: 'Running' }, evaluate(permissions, 'clusters', 'update', clusterObject('team-a', 'orders'))), true);
  assert.equal(isCancellable({ phase: 'Running' }, evaluate(permissions, 'clusters', 'update', clusterObject('team-b', 'orders'))), false);
  assert.equal(isCancellable({ phase: 'Running' }, evaluate(permissions, 'clusters', 'update', clusterObject('team-a', 'nightly'))), false);
});
