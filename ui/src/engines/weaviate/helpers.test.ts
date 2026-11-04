// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import type { Schemas } from '../../api/client';
import { API_MODULES, INFERENCE_MODULES, normalizeOptions, options, validateSpec } from './helpers.ts';
import { normalizeWizardSpec } from '../../pages/clusters/wizard/state.ts';
import { engines } from '../index.ts';
export const spec: Schemas['ClusterSpec'] = {
  provider: 'weaviate', version: '1.40.1', topology: { type: 'cluster' },
  components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 1, minRam: 2, maxRam: 2, minDisk: 10, maxDisk: 10 } } },
};
test('module allowlists match the Go source exactly', () => {
  const source = readFileSync(new URL('../../../../internal/engine/weaviate/wvspec/modules.go', import.meta.url), 'utf8');
  const api = source.match(/var APIBasedModules = \[\]string\{([\s\S]*?)\n\}/)![1]!;
  assert.deepEqual(API_MODULES, [...api.matchAll(/"([^"]+)"/g)].map((match) => match[1]));
  const inference = source.match(/var inferenceModules = map\[string\]string\{([\s\S]*?)\n\}/)![1]!;
  assert.deepEqual(INFERENCE_MODULES, [...inference.matchAll(/:\s*"([^"]+)"/g)].map((match) => match[1]));
});
test('normalization preserves explicit options and uses topology defaults', () => {
  assert.equal(options(spec).replicationMinimumFactor, 3);
  assert.equal(options({ ...spec, topology: { type: 'single' } }).replicationMinimumFactor, 1);
  assert.equal(options({ ...spec, engineOptions: { asyncReplication: false } }).asyncReplication, false);
  const normalized = normalizeWizardSpec(spec);
  assert.deepEqual(normalized, normalizeOptions(spec));
  assert.deepEqual(normalizeOptions(normalized), normalized);
  assert.equal(normalizeOptions({ ...spec, provider: 'psmdb' }).engineOptions, undefined);
});
test('validation enforces member, integer, threshold and module constraints', () => {
  assert.deepEqual(validateSpec(spec), []);
  for (const replicas of [2, 10, 3.5]) assert.ok(validateSpec({ ...spec, components: { engine: { ...spec.components.engine, replicas } } }).length);
  for (const engineOptions of [
    { replicationMinimumFactor: 4 }, { replicationMinimumFactor: 1.5 }, { queryDefaultsLimit: 0 }, { queryDefaultsLimit: 10001 },
    { diskWarningPercent: 90 }, { diskReadonlyPercent: 96 }, { diskWarningPercent: 1.5 }, { asyncReplication: null },
    { modules: ['backup-s3'] }, { modules: ['text2vec-openai', 'text2vec-openai'] }, { modules: null },
  ]) assert.ok(validateSpec({ ...spec, engineOptions }).length, JSON.stringify(engineOptions));
  assert.deepEqual(validateSpec({ ...spec, engineOptions: { modules: ['text2vec-openai'], replicationMinimumFactor: 1, diskWarningPercent: 1, diskReadonlyPercent: 95 } }), []);
  assert.ok(validateSpec({ ...spec, engineConfig: 'raw' }).length);
});
test('inference allows verified HTTP endpoints without embedded secrets or URL modifiers', () => {
  for (const url of ['http://inference:8080', 'https://inference.example/path']) assert.deepEqual(validateSpec({ ...spec, engineOptions: { inference: { 'text2vec-transformers': { url } } } }), []);
  for (const url of ['ftp://host', 'https://user:secret@host', 'https://host?key=secret', 'https://host?', 'https://host#', 'not-a-url']) assert.ok(validateSpec({ ...spec, engineOptions: { inference: { 'text2vec-transformers': { url } } } }).length, url);
  assert.ok(validateSpec({ ...spec, engineOptions: { inference: { ollama: { url: 'http://host' } } } }).length);
});
test('metadata is present without React registration and covers catalog metrics', () => {
  assert.equal(engines.weaviate?.label, 'Weaviate');
  assert.equal(engines.weaviate?.publicPort, 8080);
  assert.equal(engines.weaviate?.engineConfigLabel, undefined);
  assert.match(engines.weaviate?.specSummary?.(spec) ?? '', /3 fixed Raft voters/);
  const source = readFileSync(new URL('../../../../internal/engine/weaviate/metrics_catalog.go', import.meta.url), 'utf8');
  const names = [...source.matchAll(/"(weaviate\.[^"]+)"/g)].map((match) => match[1]);
  assert.deepEqual(engines.weaviate?.monitoringPanels?.flatMap((panel) => panel.names).sort(), names.sort());
});
