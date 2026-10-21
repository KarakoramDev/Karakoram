// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import type { Schemas } from '../../api/client';
import { boltURI, validateSpec, settingKeys } from './spec.ts';
import { engineConsequences, engineRules } from '../../pages/clusters/wizard/engine.ts';

const spec: Schemas['ClusterSpec'] = { provider: 'neo4j', version: '2026.10.0', topology: { type: 'single' }, components: { engine: { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 4, minDisk: 5, maxDisk: 20 } } } };

test('Neo4j memory validation uses defaults and minimum RAM with integer bounds', () => {
  assert.deepEqual(validateSpec(spec), []);
  assert.deepEqual(validateSpec({ ...spec, engineOptions: { heapMB: 256, pageCacheMB: 64 } }), []);
  for (const engineOptions of [{ heapMB: 255 }, { heapMB: 31745 }, { heapMB: 256.5 }, { pageCacheMB: 63 }, { pageCacheMB: 64.5 }, { heapMB: 1500, pageCacheMB: 500 }]) {
    assert.ok(validateSpec({ ...spec, engineOptions }).length, JSON.stringify(engineOptions));
  }
  assert.match(validateSpec({ ...spec, engineConfig: 'server.memory.heap.max_size=1G' }).join(' '), /does not accept engineConfig/);
  assert.match(validateSpec({ ...spec, topology: { type: 'ha' } }).join(' '), /single instance/);
  assert.match(validateSpec({ ...spec, components: { engine: { ...spec.components.engine, replicas: 2 } } }).join(' '), /single instance/);
});

test('Neo4j wizard explains singleton, offline maintenance, full privileges and public Bolt', () => {
  const copy = engineConsequences(spec).join(' ');
  for (const phrase of ['no high availability', 'backup, restart, upgrade, TLS rotation and memory change', 'full outage', 'no incremental', 'point-in-time', 'full administrative privileges', 'System users']) assert.ok(copy.includes(phrase), phrase);
  assert.match(engineRules('neo4j').exposureLabel, /Bolt.*7687/);
  assert.equal(engineRules('neo4j').resourceMinimums.ram, 1);
  assert.equal(boltURI('graph.example.org:7687'), 'neo4j+s://graph.example.org:7687');
  assert.equal(boltURI(), 'neo4j+s://HOST:7687');
  assert.equal(settingKeys.length, 7);
  assert.ok(!settingKeys.some((key) => key.startsWith('server.')));
});
