// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { detailProvider } from '../../../engines/index.ts';
import { detailTabState } from './detail-tab-state.ts';

const rabbitmq = { ...detailProvider('rabbitmq', undefined)!, capabilities: ['backups', 'restores', 'systemUsers'] };
const gatedTabs = ['backups', 'restores', 'users'];

test('cold RabbitMQ tab navigation waits for provider metadata instead of redirecting', () => {
  const pendingProvider = detailProvider('rabbitmq', undefined);
  for (const tab of gatedTabs) {
    assert.equal(detailTabState(pendingProvider, tab, false), 'loading', tab);
    assert.equal(detailTabState(detailProvider('rabbitmq', rabbitmq), tab, true), 'available', tab);
  }
});

test('unsupported tabs redirect only after provider metadata has resolved', () => {
  const unsupported = { ...rabbitmq, capabilities: [] };
  for (const tab of gatedTabs) {
    assert.equal(detailTabState(unsupported, tab, false), 'loading', tab);
    assert.equal(detailTabState(unsupported, tab, true), 'unsupported', tab);
  }
});

test('failed catalogs never redirect capability-gated deep links', () => {
  for (const engine of ['rabbitmq', 'elasticsearch']) {
    const failedCatalog = { isPending: false, isSuccess: false };
    const provider = detailProvider(engine, undefined);
    for (const tab of gatedTabs) {
      assert.equal(detailTabState(provider, tab, failedCatalog.isSuccess), 'loading', `${engine}/${tab}`);
    }
  }
});

test('ungated RabbitMQ tabs remain available while provider metadata loads', () => {
  for (const tab of ['', 'topology', 'events', 'operations']) {
    assert.equal(detailTabState(detailProvider('rabbitmq', undefined), tab, false), 'available', tab);
  }
});

test('Mongo capability fallback remains available regardless of catalog success', () => {
  for (const success of [true, false]) {
    for (const tab of gatedTabs) {
      assert.equal(detailTabState(detailProvider('psmdb', undefined), tab, success), 'available', tab);
    }
  }
});
