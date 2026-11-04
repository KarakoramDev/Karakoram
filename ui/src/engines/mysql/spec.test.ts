// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import type { Schemas } from '../../api/client.ts';
import { mysqlMetadata, mysqlMonitoringPanels } from './metadata.ts';
import { defaultComponent, mysqlChangeReview, mysqlMajorUpgrade, transitionErrors, validateComponents, validateSpec } from './spec.ts';
const spec: Schemas['ClusterSpec'] = { provider: 'mysql', version: '8.4.11-11', topology: { type: 'single' }, components: { engine: defaultComponent(undefined, 'engine'), extra: { proxy: defaultComponent(undefined, 'proxy') } } };
test('defaults validate with native single and group constraints', () => {
  assert.deepEqual(validateSpec(spec), []);
  assert.deepEqual(validateComponents(spec), { topology: [], resources: [] });
  for (const replicas of [3, 5]) assert.deepEqual(validateComponents({ ...spec, topology: { type: 'group' }, components: { ...spec.components, engine: { ...spec.components.engine, replicas } } }).topology, []);
  for (const replicas of [0, 2, 4, 7]) assert.ok(validateComponents({ ...spec, topology: { type: 'group' }, components: { ...spec.components, engine: { ...spec.components.engine, replicas } } }).topology.length);
  assert.ok(validateComponents({ ...spec, topology: { type: 'group' }, components: { engine: { ...spec.components.engine, replicas: 3 } } }).topology.length);
});
test('options mirror bounded backend allowlist and defaults', () => {
  assert.deepEqual(validateSpec({ ...spec, engineOptions: { parameters: { max_connections: '250', slow_query_log: 'ON', long_query_time: '0.5' }, groupReplication: { consistency: 'BEFORE' }, binlog: { archiveInterval: 300, expireSeconds: 604800 } } }), []);
  for (const parameters of [{ port: '3306' }, { max_connections: '0' }, { slow_query_log: 'true' }, { max_connections: '200\nunsafe' }, { transaction_isolation: 'unsafe' }]) assert.ok(validateSpec({ ...spec, engineOptions: { parameters } }).length);
  assert.ok(validateSpec({ ...spec, engineOptions: { groupReplication: { consistency: 'unsafe' } } }).length);
  assert.deepEqual(validateSpec({ ...spec, engineOptions: { parameters: { innodb_buffer_pool_instances: '4', table_open_cache_instances: '8' } } }), []);
  assert.ok(validateSpec({ ...spec, engineOptions: { parameters: { innodb_buffer_pool_instances: '65' } } }).length);
  assert.ok(validateSpec({ ...spec, engineOptions: { binlog: { archiveInterval: 300, expireSeconds: 300 } } }).length);
});
test('metadata and change review are MySQL native', () => {
  assert.equal(mysqlMetadata.label, 'MySQL'); assert.equal(mysqlMetadata.publicPort, 3306);
  assert.ok(mysqlMonitoringPanels.some((p) => p.names.includes('mysql_questions_total')));
  assert.equal(mysqlMajorUpgrade('8.0.46-37', '8.4.11-11'), true);
  assert.equal(mysqlMajorUpgrade('8.4.10-10', '8.4.11-11'), false);
  assert.match(mysqlChangeReview.disruptions({ ...spec, version: '8.0.46-37' }, spec).join(' '), /recent successful backup/);
  assert.match(transitionErrors(spec, { ...spec, version: '8.0.46-37' }).join(' '), /downgrades/);
  assert.match(transitionErrors({ ...spec, topology: { type: 'group' } }, spec).join(' '), /cannot/);
  assert.match(mysqlChangeReview.disruptions(spec, { ...spec, topology: { type: 'group' } }).join(' '), /majority.*quorum/);
  assert.deepEqual(mysqlChangeReview.buildPatch({ ...spec, engineOptions: { parameters: { max_connections: '200' } } }, { ...spec, engineOptions: { parameters: {} } }), { engineOptions: { parameters: { max_connections: null } } });
});
