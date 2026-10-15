// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import type { Schemas } from '../api/client';
import type { EngineUI } from './types';
import { applyMergePatch, reviewChanges } from './changes.ts';
import { buildPatch, draftFrom, specFromDraft } from '../pages/clusters/detail/topology.ts';

const current: Schemas['ClusterSpec'] = {
  provider: 'test', version: '1.0', topology: { type: 'cluster' },
  components: { engine: { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 10 } } },
};

test('shared Apply review shows only consequences of the engine merge patch, not draft defaults', () => {
  const form = draftFrom(current);
  form.engineReplicas = 3;
  form.engineResources = { ...form.engineResources, startCpu: 1 };
  const draft = specFromDraft(current, form);
  assert.notDeepEqual(draft.tls, current.tls);
  assert.notDeepEqual(draft.exposure, current.exposure);
  const expectedPatch = { components: { engine: { replicas: 3 } } };
  let builds = 0;
  const engine: EngineUI = {
    label: 'Test',
    changeReview: {
      buildPatch: (persisted, proposed) => {
        builds++;
        assert.equal(persisted, current);
        assert.equal(proposed, draft);
        return expectedPatch;
      },
      disruptions: (persisted, applied) => {
        assert.equal(builds, 1);
        assert.equal(applied.components.engine.replicas, 3);
        return [
          ...(persisted.components.engine.replicas !== applied.components.engine.replicas ? ['replicas'] : []),
          ...(JSON.stringify(persisted.components.engine.resources) !== JSON.stringify(applied.components.engine.resources) ? ['resources'] : []),
          ...(JSON.stringify(persisted.tls) !== JSON.stringify(applied.tls) ? ['TLS'] : []),
          ...(JSON.stringify(persisted.exposure) !== JSON.stringify(applied.exposure) ? ['exposure'] : []),
          ...(JSON.stringify(persisted.monitoring) !== JSON.stringify(applied.monitoring) ? ['monitoring'] : []),
        ];
      },
    },
  };
  const review = reviewChanges(current, draft, engine.changeReview!);
  assert.equal(review.patch, expectedPatch);
  assert.deepEqual(review.consequences, ['replicas']);
  assert.equal(current.components.engine.replicas, 1);
  assert.equal(current.tls, undefined);
});

test('MongoDB form defaults still produce only a membership patch', () => {
  const mongo = { ...current, provider: 'psmdb', topology: { type: 'replicaSet' as const } };
  const draft = draftFrom(mongo);
  draft.engineReplicas = 3;
  assert.deepEqual(buildPatch(mongo, draft), { components: { engine: { replicas: 3 } } });
});

test('merge patch removes nested nulls, merges objects and replaces arrays without mutating inputs', () => {
  const before = { retained: true, nested: { remove: 1, retain: 2 }, array: [1, 2], scalar: 3 };
  const patch = { nested: { remove: null, add: 4 }, array: [null, 5], scalar: { value: 6 }, missing: null };
  assert.deepEqual(applyMergePatch(before, patch), { retained: true, nested: { retain: 2, add: 4 }, array: [null, 5], scalar: { value: 6 } });
  assert.deepEqual(before, { retained: true, nested: { remove: 1, retain: 2 }, array: [1, 2], scalar: 3 });
  assert.deepEqual(patch.nested, { remove: null, add: 4 });
  assert.equal(applyMergePatch(before, null), null);
  assert.deepEqual(applyMergePatch(before, [1]), [1]);
  assert.equal(applyMergePatch(before, 'replacement'), 'replacement');
  assert.deepEqual(applyMergePatch(null, { nested: { remove: null, add: 4 } }), { nested: { add: 4 } });
});

test('merge patch treats prototype-like keys as ordinary JSON members', () => {
  const patch = JSON.parse('{"__proto__":{"value":1},"constructor":{"prototype":{"value":2}}}');
  const applied = applyMergePatch({}, patch);
  assert.deepEqual(applied, patch);
  assert.equal(Object.getPrototypeOf(applied), Object.prototype);
  assert.equal(Object.hasOwn({}, 'value'), false);
});
