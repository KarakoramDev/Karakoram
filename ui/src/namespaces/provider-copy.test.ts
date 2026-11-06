// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { Namespace, ClusterSpec } from '../api/hooks';
import type { Schemas } from '../api/client';

register('../../test/tsx-loader.mjs', import.meta.url);
// TSX and aliased dependencies must load after registering the source loader.
const { namespaceBackend, namespaceBackendInfo, namespaceHint, clusterNameHint, clustersDescription, BillingNote } = await import('./provider-copy.tsx');
const { ExposureEditor } = await import('../pages/clusters/editors/AccessEditor.tsx');
const { exposureReviewRows } = await import('../pages/clusters/wizard/review.ts');
const { engineConsequences } = await import('../pages/clusters/wizard/engine.ts');

const spec: ClusterSpec = {
  provider: 'psmdb', version: '8.0', topology: { type: 'replicaSet' },
  components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 10 } } },
  exposure: { public: false, ipType: 'IPV6', allowlist: [] },
};

function backendInfo(kind: string, capabilities: Schemas['Capability'][] = []): Schemas['BackendInfo'] {
  return { kind, displayName: kind, capabilities, experimental: false, relay: true, configKeys: [] };
}

test('namespace backend follows the target, never a global or missing-provider default', () => {
  const namespaces = [{ name: 'one', backend: 'zerops' }, { name: 'two', backend: 'dokploy' }, { name: 'three', backend: 'nomad' }] as Namespace[];
  assert.equal(namespaceBackend(namespaces, 'one'), 'zerops');
  assert.equal(namespaceBackend(namespaces, 'two'), 'dokploy');
  assert.equal(namespaceBackend(namespaces, 'three'), 'nomad');
  assert.equal(namespaceBackend(namespaces, 'unknown'), undefined);
  assert.equal(namespaceBackend(undefined, 'default'), undefined);
  const dokploy = backendInfo('dokploy');
  assert.equal(namespaceBackendInfo(namespaces, 'two', [dokploy]), dokploy);
  assert.equal(namespaceBackendInfo(namespaces, 'one', [dokploy]), undefined);
  assert.equal(namespaceBackendInfo(undefined, 'two', [dokploy]), undefined);
  assert.equal(namespaceBackendInfo(namespaces, 'two', undefined), undefined);
});

for (const backend of ['zerops', 'dokploy', 'nomad', undefined]) {
  test(`${backend ?? 'unknown'} wizard basics and billing only show relevant provider facts`, () => {
    assert.match(namespaceHint, /project or environment/);
    assert.doesNotMatch(namespaceHint, /Zerops/);
    const hint = clusterNameHint(backend);
    const billing = renderToStaticMarkup(createElement(BillingNote, { backend }));
    assert.equal(hint.includes('Zerops hostname prefix'), backend === 'zerops');
    assert.equal(billing.includes('https://zerops.io/pricing'), backend === 'zerops');
    if (backend !== 'zerops') {
      assert.match(hint, /member service name prefix/);
      assert.match(billing, /provider’s own pricing/);
      assert.doesNotMatch(billing, /autoscaling|Zerops/);
    }
  });

  test(`${backend ?? 'unknown'} review, consequences and clusters description are neutral`, () => {
    const copy = JSON.stringify([exposureReviewRows(spec), engineConsequences(spec), clustersDescription(), clustersDescription('example'), clustersDescription(undefined, true)]);
    assert.doesNotMatch(copy, /Zerops/);
    assert.match(copy, /selected namespace/);
    assert.match(copy, /private network/);
    assert.match(clustersDescription('example'), /namespace example/);
    assert.match(clustersDescription(undefined, true), /across every namespace/);
  });

  test(`${backend ?? 'unknown'} access editor gates project addresses and explains external filtering`, () => {
    const props = { backend: backend ? backendInfo(backend, backend === 'zerops' ? ['publicTcp', 'tcpAllowlist'] : ['publicTcp']) : undefined, value: { public: true, ipType: 'IPV6' as const, allowlist: ['::/0'], allowAllAddresses: true }, onChange() {}, allAddressesConfirmed: true, onAllAddressesConfirmedChange() {} };
    const markup = renderToStaticMarkup(createElement(ExposureEditor, props));
    assert.equal(markup.includes('Included with every Zerops project'), backend === 'zerops');
    assert.equal(markup.includes('Zerops public TCP needs'), backend === 'zerops');
    if (backend !== 'zerops') {
      assert.doesNotMatch(markup, /Zerops|Dedicated IPv4|add-on/);
      assert.equal(markup.includes('does not enforce source-address filtering'), backend !== undefined);
      assert.match(markup, /firewall or load balancer/);
      assert.doesNotMatch(markup, /Dokploy|Nomad/);
    }
    const privateMarkup = renderToStaticMarkup(createElement(ExposureEditor, { ...props, value: { ...props.value, public: false } }));
    assert.match(privateMarkup, /namespace’s private network/);
    assert.doesNotMatch(privateMarkup, /Zerops/);
  });
}

test('source-address guidance follows capabilities rather than backend names', () => {
  for (const kind of ['zerops', 'custom']) {
    for (const tcpAllowlist of [true, false]) {
      const markup = renderToStaticMarkup(createElement(ExposureEditor, {
        backend: backendInfo(kind, tcpAllowlist ? ['publicTcp', 'tcpAllowlist'] : ['publicTcp']),
        value: { public: true, ipType: 'IPV4', allowlist: ['0.0.0.0/0'], allowAllAddresses: true },
        onChange() {}, allAddressesConfirmed: true, onAllAddressesConfirmedChange() {},
      }));
      assert.equal(markup.includes('Use the IP allowlist as defense in depth'), tcpAllowlist);
      assert.equal(markup.includes('does not enforce source-address filtering'), !tcpAllowlist);
      assert.equal(markup.includes('all-address allowlist with explicit acknowledgement'), !tcpAllowlist);
    }
  }
});
