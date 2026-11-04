// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import type { Schemas } from '../../api/client.ts';
import { applyMergePatch } from '../changes.ts';
import { normalizeWizardSpec } from '../../pages/clusters/wizard/state.ts';
import { configurationReviewRows, totalFootprint } from '../../pages/clusters/wizard/review.ts';
import { engineRules, engineConsequences } from '../../pages/clusters/wizard/engine.ts';
import { defaultComponent, normalizeSpec, postgresChangeReview, specPatch, validateComponents, validateSpec } from './spec.ts';

const data = defaultComponent(undefined, 'engine');
const proxy = defaultComponent(undefined, 'proxy');
const spec: Schemas['ClusterSpec'] = { provider: 'postgres', version: '17.11.1', topology: { type: 'single' }, components: { engine: data, extra: { proxy } } };

test('single and HA member counts and proxy constraints are PostgreSQL-native', () => {
  assert.deepEqual(validateComponents(spec), { topology: [], resources: [] });
  assert.deepEqual(validateComponents({ ...spec, components: { ...spec.components, extra: { proxy: { ...proxy, replicas: 0 } } } }).topology, []);
  for (const n of [3, 5]) assert.deepEqual(validateComponents({ ...spec, topology: { type: 'ha' }, components: { ...spec.components, engine: { ...data, replicas: n } } }).topology, []);
  for (const n of [0, 2, 4, 7, 3.5]) assert.ok(validateComponents({ ...spec, topology: { type: 'ha' }, components: { ...spec.components, engine: { ...data, replicas: n } } }).topology.length);
  assert.match(validateComponents({ ...spec, topology: { type: 'ha' }, components: { engine: { ...data, replicas: 3 } } }).topology.join(' '), /proxy/);
  assert.match(validateComponents({ ...spec, components: { ...spec.components, extra: { proxy: { ...proxy, replicas: -1 } } } }).topology.join(' '), /proxy/);
});

test('distributed options bound worker groups and preserve group-local replica semantics', () => {
  const distributed: Schemas['ClusterSpec'] = { ...spec, topology: { type: 'distributed' }, components: { ...spec.components, engine: { ...data, replicas: 3 } } };
  assert.deepEqual(validateComponents(distributed).topology, []);
  assert.deepEqual(validateSpec(distributed), []);
  for (const workerGroups of [0, 9, 1.5]) assert.match(validateSpec({ ...distributed, engineOptions: { citus: { workerGroups } } }).join(' '), /Worker groups/);
  for (const workerGroups of [1, 2, 8]) assert.deepEqual(validateSpec({ ...distributed, engineOptions: { citus: { workerGroups, database: 'app' } } }), []);
  assert.match(validateSpec({ ...distributed, engineOptions: { citus: { database: '' } } }).join(' '), /Citus database/);
  assert.ok(validateComponents({ ...distributed, components: { engine: { ...data, replicas: 3 } } }).topology.length);
  assert.deepEqual(validateComponents({ ...distributed, components: { engine: { ...data, replicas: 1 } } }).topology, []);
  assert.ok(validateComponents({ ...distributed, components: { ...spec.components, engine: { ...data, replicas: 5 } } }).topology.length);
  assert.equal(engineRules('postgres').topology.distributed?.title, 'Distributed PostgreSQL (Citus)');
  assert.equal(engineRules('supabase').topology.distributed, undefined);
});

test('safe engine options validate and single topology clears HA-only synchronous mode without Elastic rewriting', () => {
  const options = { synchronousMode: 'strict', parameters: { max_connections: '250' }, proxy: { readWriteSplit: true, readerPort: true } };
  const ha = { ...spec, topology: { type: 'ha' as const }, engineOptions: options };
  assert.deepEqual(validateSpec(ha), []);
  for (const key of ['archive_command', 'ssl_cert_file', 'listen_addresses', 'shared_preload_libraries']) assert.match(validateSpec({ ...ha, engineOptions: { parameters: { [key]: 'x' } } }).join(' '), /managed or invalid/);
  assert.ok(validateSpec({ ...ha, engineOptions: { synchronousMode: 'maybe' } }).length);
  assert.ok(validateSpec({ ...ha, engineOptions: { parameters: { work_mem: '1MB\nunsafe' } } }).length);
  assert.ok(validateSpec({ ...ha, engineConfig: 'unsafe: true' }).length);
  const normalized = normalizeWizardSpec({ ...spec, engineOptions: options });
  assert.equal(normalized.engineOptions?.synchronousMode, undefined);
  assert.deepEqual(normalized.engineOptions?.parameters, options.parameters);
  assert.equal(normalized.engineOptions?.elasticsearch, undefined);
  assert.equal(options.synchronousMode, 'strict');
  assert.equal(normalizeSpec(spec), spec);
});

test('review rows and resource footprint do not count stateless proxies as data volumes', () => {
  const draft = { ...spec, engineOptions: { parameters: { max_connections: '200' }, proxy: { readerPort: true } } };
  const rows = Object.fromEntries(configurationReviewRows(draft));
  assert.match(rows['PostgreSQL topology']!, /Single.*1 data.*1 proxy/);
  assert.match(rows.Parameters!, /max_connections=200/);
  assert.match(rows['Proxy routing']!, /5433/);
  assert.equal(totalFootprint(draft).volumes, 1);
  assert.equal(totalFootprint(draft).members, 2);
  assert.match(engineRules('postgres').topology.ha!.description, /Patroni/);
  assert.match(engineConsequences(draft).join(' '), /WAL|PITR/);
});

test('merge patch removes deleted parameter keys and preserves unrelated options', () => {
  const current = { ...spec, engineOptions: { parameters: { max_connections: '200', work_mem: '8MB' }, proxy: { readerPort: true } } };
  const draft = { ...spec, engineOptions: { parameters: { max_connections: '250' }, proxy: { readerPort: true } } };
  const patch = specPatch(current, draft);
  assert.deepEqual(patch, { engineOptions: { parameters: { max_connections: '250', work_mem: null } } });
  assert.deepEqual(applyMergePatch(current, patch), draft);
  assert.deepEqual(specPatch(spec, spec), {});
});

test('topology major-version review requires offline link-mode maintenance and a backup', () => {
  for (const [current, target] of [['16.13.1', '17.11.1'], ['17.11.1', '18.6.1']]) {
    for (const replicas of [1, 3]) {
      const before = { ...spec, version: current!, components: { ...spec.components, engine: { ...data, replicas } } };
      const copy = postgresChangeReview.disruptions(before, { ...before, version: target! }).join(' ');
      assert.match(copy, /OFFLINE.*all data members are stopped.*pg_upgrade --link/);
      assert.match(copy, /All database access is unavailable.*replica rebuild/);
      assert.match(copy, /successful backup is required/);
      assert.doesNotMatch(copy, /roll replicas|briefly interrupts/);
    }
  }
  assert.deepEqual(postgresChangeReview.disruptions(spec, spec), []);
  const minor = postgresChangeReview.disruptions(spec, { ...spec, version: '17.12.1' }).join(' ');
  assert.match(minor, /roll replicas.*primary/);
  assert.doesNotMatch(minor, /OFFLINE|pg_upgrade/);
});
