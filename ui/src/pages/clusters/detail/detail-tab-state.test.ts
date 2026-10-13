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
    assert.equal(detailTabState(pendingProvider, tab, true), 'loading', tab);
    assert.equal(detailTabState(detailProvider('rabbitmq', rabbitmq), tab, false), 'available', tab);
  }
});

test('unsupported tabs redirect only after provider metadata has resolved', () => {
  const unsupported = { ...rabbitmq, capabilities: [] };
  for (const tab of gatedTabs) {
    assert.equal(detailTabState(unsupported, tab, true), 'loading', tab);
    assert.equal(detailTabState(unsupported, tab, false), 'unsupported', tab);
  }
});

test('settled catalog failures retain the existing non-Mongo unsupported fallback', () => {
  for (const tab of gatedTabs) {
    assert.equal(detailTabState(detailProvider('rabbitmq', undefined), tab, false), 'unsupported', tab);
  }
});

test('ungated RabbitMQ tabs remain available while provider metadata loads', () => {
  for (const tab of ['', 'topology', 'events', 'operations']) {
    assert.equal(detailTabState(detailProvider('rabbitmq', undefined), tab, true), 'available', tab);
  }
});

test('Mongo capability fallback remains available regardless of catalog loading', () => {
  for (const pending of [true, false]) {
    for (const tab of gatedTabs) {
      assert.equal(detailTabState(detailProvider('psmdb', undefined), tab, pending), 'available', tab);
    }
  }
});
