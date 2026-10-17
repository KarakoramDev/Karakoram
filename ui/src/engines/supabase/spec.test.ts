// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import type { Schemas } from '../../api/client.ts';
import { SUPABASE_COMPONENTS, applyPreset, component, defaultComponent, dependencies, normalizeSpec, object, toggleComponent, validateComponents, validateSpec } from '../postgres/spec.ts';
import { configurationReviewRows, exposureSummary, totalFootprint } from '../../pages/clusters/wizard/review.ts';

const spec: Schemas['ClusterSpec'] = { provider: 'supabase', version: '17.11.1', topology: { type: 'single' }, components: { engine: defaultComponent(undefined, 'engine'), extra: { proxy: defaultComponent(undefined, 'proxy') } } };

test('minimal standard and full presets select the defined native services', () => {
  for (const [preset, count] of [['minimal', 3], ['standard', 7], ['full', 12]] as const) {
    const draft = applyPreset(spec, undefined, preset);
    assert.equal(SUPABASE_COMPONENTS.filter((name) => component(draft, name)?.replicas).length, count);
    assert.deepEqual(validateComponents(draft).topology, []);
    assert.equal(totalFootprint(draft).volumes, 1);
  }
  assert.equal(component(applyPreset(spec, undefined, 'standard'), 'imgproxy')?.replicas, 0);
  assert.equal(spec.components.extra?.gateway, undefined, 'presets must not mutate the source');
});

test('enabling dependencies and disabling dependents converge transitively', () => {
  const studio = toggleComponent(spec, undefined, 'studio', true);
  for (const name of ['studio', 'meta', 'gateway']) assert.equal(component(studio, name)?.replicas, 1);
  assert.equal(component(toggleComponent(studio, undefined, 'meta', false), 'studio')?.replicas, 0);
  const image = toggleComponent(spec, undefined, 'imgproxy', true);
  assert.equal(component(image, 'storage')?.replicas, 1);
  assert.equal(component(toggleComponent(image, undefined, 'storage', false), 'imgproxy')?.replicas, 0);
  const logs = toggleComponent(spec, undefined, 'vector', true);
  assert.equal(component(logs, 'analytics')?.replicas, 1);
  assert.equal(component(toggleComponent(logs, undefined, 'analytics', false), 'vector')?.replicas, 0);
});

test('catalog component requirements override fallback dependencies and handle cycles', () => {
  const topology: Schemas['ProviderTopology'] = { type: 'single', displayName: 'Single', defaults: spec, limits: {}, componentSchemas: { studio: { requires: ['meta'] }, meta: { requires: ['studio'] }, auth: { requires: [] } } };
  assert.deepEqual(dependencies(topology, 'studio'), ['meta']);
  assert.deepEqual(dependencies(topology, 'auth'), []);
  const enabled = toggleComponent(spec, topology, 'studio', true);
  assert.equal(component(enabled, 'studio')?.replicas, 1);
  assert.equal(component(enabled, 'meta')?.replicas, 1);
  assert.equal(component(enabled, 'gateway'), undefined);
  assert.deepEqual(validateComponents(enabled, topology).topology, []);
});

test('options validate PostgreSQL major, URLs, SMTP ranges, storage and public dependencies', () => {
  const minimal = applyPreset(spec, undefined, 'minimal');
  assert.deepEqual(validateSpec(minimal), []);
  assert.match(validateSpec({ ...minimal, version: '18.6.1' }).join(' '), /PostgreSQL 17/);
  for (const siteUrl of ['bad', 'ftp://example.org', 'https://user:pass@example.org', 'https://example.org/#secret']) assert.match(validateSpec({ ...minimal, engineOptions: { siteUrl } }).join(' '), /Site URL/);
  assert.match(validateSpec({ ...minimal, engineOptions: { auth: { jwtExpiry: -1, smtp: { port: 65536 } }, rest: { maxRows: 0 } } }).join(' '), /JWT expiry.*SMTP port.*REST max rows/);
  assert.match(validateSpec(toggleComponent(minimal, undefined, 'storage', true)).join(' '), /backend storage/);
  const withStorage = { ...toggleComponent(minimal, undefined, 'storage', true), engineOptions: { storage: { backendStorage: 'objects' } } };
  assert.deepEqual(validateSpec(withStorage), [], 'registered backend bucket is used when bucket override is omitted');
  assert.match(validateSpec({ ...minimal, engineOptions: { storage: { imageTransformation: true } } }).join(' '), /imgproxy/);
  assert.match(validateSpec({ ...spec, engineOptions: { gateway: { publicHttps: true } } }).join(' '), /gateway/);
});

test('normalization and review preserve Supabase options without Elastic assumptions', () => {
  const draft = { ...spec, engineOptions: { siteUrl: 'https://app.example.org', auth: { disableSignup: true }, storage: { imageTransformation: true }, gateway: { publicHttps: true } } };
  const normalized = normalizeSpec(draft);
  assert.equal(object(normalized.engineOptions?.storage).imageTransformation, false);
  assert.equal(object(normalized.engineOptions?.gateway).publicHttps, false);
  assert.equal(normalized.engineOptions?.siteUrl, 'https://app.example.org');
  const rows = Object.fromEntries(configurationReviewRows(applyPreset(draft, undefined, 'minimal')));
  assert.match(rows.Components!, /gateway.*auth.*rest/);
  assert.equal(rows.siteUrl, '"https://app.example.org"');
  assert.match(exposureSummary(draft), /Gateway HTTPS public/);
});

test('gateway CORS accepts exact origins and defaults to unrestricted browser origins', () => {
  const minimal = applyPreset(spec, undefined, 'minimal');
  for (const corsOrigins of [undefined, [], ['*'], ['https://app.example.org', 'http://localhost:3000']]) {
    assert.deepEqual(validateSpec({ ...minimal, engineOptions: { gateway: { corsOrigins } } }), []);
  }
  for (const corsOrigins of [['null'], ['*', 'https://app.example.org'], ['https://user:pass@example.org'], ['https://app.example.org/'], ['https://app.example.org/path'], ['https://app.example.org?x=1'], ['https://app.example.org#fragment'], ['ftp://example.org']]) {
    assert.match(validateSpec({ ...minimal, engineOptions: { gateway: { corsOrigins } } }).join(' '), /CORS origins/);
  }
});
