// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import type { Schemas } from '../../api/client.ts';
import { applicationGrants, applicationWarnings } from './access-state.ts';
import { DEFAULT_RESOURCES, disruptions, elasticsearchChangeReview, readOptions, specPatch, validateSpec, writeOptions } from './spec.ts';
import { exposureSummary } from '../../pages/clusters/wizard/review.ts';
import { reviewChanges } from '../changes.ts';

const current: Schemas['ClusterSpec'] = {
 provider: 'elasticsearch', version: '9.5.5', topology: { type: 'cluster' },
 components: { engine: { replicas: 1, resources: DEFAULT_RESOURCES.engine }, extra: { kibana: { replicas: 1, resources: DEFAULT_RESOURCES.kibana }, apm: { replicas: 1, resources: DEFAULT_RESOURCES.apm } } },
};

test('replicas-only patch consequences ignore normalized security defaults', () => {
 const draft: Schemas['ClusterSpec'] = { ...structuredClone(current), tls: { mode: 'requireTLS', ca: 'selfSigned' }, exposure: { public: false, ipType: 'IPV6', allowlist: [] }, monitoring: { enabled: true } };
 draft.components.engine.replicas = 3;
 assert.deepEqual(specPatch(current, draft), { components: { engine: { replicas: 3 } } });
 assert.equal(disruptions(current, draft).length, 1);
 assert.match(disruptions(current, draft)[0]!, /1 → 3/);
 assert.doesNotMatch(disruptions(current, draft).join(' '), /TLS|Exposure|restarts|metrics/);
 const review = reviewChanges(current, draft, elasticsearchChangeReview);
 assert.deepEqual(review.patch, { components: { engine: { replicas: 3 } } });
 assert.deepEqual(review.consequences, disruptions(current, draft));
 const tls = { ...draft, tls: { mode: 'requireTLS' as const, ca: 'userProvided' as const, caCertificate: 'CERT', caPrivateKey: 'KEY' } };
 assert.match(disruptions(draft, tls).join(' '), /TLS change/);
 assert.deepEqual(disruptions(tls, structuredClone(tls)), []);
});

test('public stack routes require fresh confirmation and review every public surface', () => {
 const publicSpec = writeOptions(current, { ...readOptions(current), kibana: { public: true }, apm: { public: true, apiKeys: true } });
 assert.equal(validateSpec(publicSpec).filter((message) => message.startsWith('Confirm')).length, 2);
 const confirmed = writeOptions(publicSpec, { ...readOptions(publicSpec), kibana: { public: true, allowAllAddresses: true }, apm: { public: true, apiKeys: true, allowAllAddresses: true } });
 assert.deepEqual(validateSpec(confirmed), []);
 assert.deepEqual(validateSpec(publicSpec, publicSpec), []);
 const review = exposureSummary(confirmed);
 assert.match(review, /Database private/);
 assert.match(review, /Public Kibana HTTPS/);
 assert.match(review, /Public APM Server HTTPS/);
 assert.match(review, /internet reachable, no IP allowlist, authentication only \(acknowledged\)/);
 assert.match(exposureSummary(publicSpec), /acknowledgement required/);
 const privateSpec = writeOptions(confirmed, { ...readOptions(confirmed), kibana: { public: false, allowAllAddresses: true }, apm: { public: false, apiKeys: true, allowAllAddresses: true } });
 assert.doesNotMatch(JSON.stringify(privateSpec.engineOptions), /allowAllAddresses/);
 assert.doesNotMatch(exposureSummary(privateSpec), /Public Kibana|Public APM/);
 assert.match(disruptions(current, confirmed).join(' '), /Kibana public HTTPS enabled/);
 assert.match(disruptions(current, confirmed).join(' '), /APM Server public HTTPS enabled/);
});

test('custom application privileges warn without blocking native role grants', () => {
 for (const grant of [
  { application: 'unknown-kibana', privileges: 'feature_discover.read', resources: '*' },
  { application: 'kibana-.kibana', privileges: 'unknown_privilege', resources: 'space:default' },
 ]) {
  assert.equal(applicationWarnings([grant]).length, 1);
  assert.ok(applicationGrants([grant]));
 }
 assert.deepEqual(applicationWarnings([{ application: 'kibana-.kibana', privileges: 'all,read,feature_dashboard.all,feature_discover.read', resources: '*' }]), []);
 for (const privileges of ['feature_dashboard.minimal_all', 'feature_discover.minimal_read', 'feature_dashboard.url_create', 'feature_discover.store_search_session', 'space_all,space_read']) {
  const grant = { application: 'kibana-.kibana', privileges, resources: '*' };
  assert.deepEqual(applicationWarnings([grant]), [], privileges);
  assert.ok(applicationGrants([grant]));
 }
 const source = readFileSync(new URL('./ElasticAccessTab.tsx', import.meta.url), 'utf8');
 assert.match(source, /applicationWarnings\(applications\).*Callout/);
});

test('topology heading and review use the human topology label', () => {
 const source = readFileSync(new URL('../../pages/clusters/ClusterWizard.tsx', import.meta.url), 'utf8');
 assert.match(source, /<Card title=\{mongo[^\n]+rules\.topology\[topology\.type\]\?\.title \?\? topology\.displayName/);
 assert.match(source, /\['Topology'[^\n]+rules\.topology\[topology\.type\]\?\.title/);
});
