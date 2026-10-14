// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '@/api/client';
import { buildPatch, draftFrom } from './topology.ts';

const spec: Schemas['ClusterSpec'] = {
  provider: 'psmdb', version: '8.0.32-14', topology: { type: 'replicaSet' },
  components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 10 } } },
  tls: { mode: 'requireTLS', ca: 'selfSigned' },
  exposure: { public: false, ipType: 'IPV6', allowlist: [] },
};

test('topology exposure patches include the current public acknowledgement', () => {
  for (const allowlist of [['::/0'], ['::/1', '8000::/1']]) {
    const draft = draftFrom(spec);
    draft.exposure = { public: true, ipType: 'IPV6', allowlist, allowAllAddresses: true };
    assert.deepEqual(buildPatch(spec, draft).exposure, draft.exposure);
    draft.exposure.allowAllAddresses = false;
    assert.equal((buildPatch(spec, draft).exposure as Schemas['Exposure']).allowAllAddresses, false);
  }
  const exposed = { ...spec, exposure: { public: true, ipType: 'IPV6' as const, allowlist: ['::/0'], allowAllAddresses: true } };
  const draft = draftFrom(exposed);
  draft.exposure = { ...draft.exposure, public: false, allowAllAddresses: true };
  assert.deepEqual(buildPatch(exposed, draft).exposure, { public: false, ipType: 'IPV6', allowlist: [], allowAllAddresses: false });
});

test('unrelated topology changes omit unchanged legacy exposure', () => {
  for (const allowlist of [['::/0'], ['192.0.2.0/24']]) {
    const legacy = { ...spec, exposure: { public: true, ipType: 'IPV6' as const, allowlist } };
    const draft = draftFrom(legacy);
    assert.deepEqual(buildPatch(legacy, draft), {});
    draft.engineReplicas = 5;
    assert.deepEqual(buildPatch(legacy, draft), { components: { engine: { replicas: 5 } } });
  }
});
