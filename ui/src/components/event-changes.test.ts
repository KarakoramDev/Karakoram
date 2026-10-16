// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import { test } from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { components } from '../api/schema';
import { eventChangeValue } from './event-changes.ts';

register('../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before registration; load TSX only after its test loader.
const { EventChanges } = await import('./EventChanges.tsx');
const { EventTable } = await import('../pages/clusters/detail/EventsTab.tsx');
type Details = components['schemas']['Event']['details'];

function render(details?: Details) {
  return renderToStaticMarkup(createElement(EventChanges, { details }));
}

test('event change values preserve absent, null, false, zero and unquoted strings', () => {
  assert.equal(eventChangeValue(undefined), '(unset)');
  assert.equal(eventChangeValue(null), 'null');
  assert.equal(eventChangeValue(false), 'false');
  assert.equal(eventChangeValue(0), '0');
  assert.equal(eventChangeValue(''), '');
  assert.equal(eventChangeValue('nightly'), 'nightly');
});

test('event change values render arrays and objects as compact JSON', () => {
  assert.equal(eventChangeValue({ enabled: true, replicas: 2 }), '{"enabled":true,"replicas":2}');
  assert.equal(eventChangeValue(['nightly', { hour: 2 }]), '["nightly",{"hour":2}]');
});

test('event change values preserve every sensitive marker verbatim', () => {
  for (const marker of ['(changed)', '(set)', '(unset)']) {
    assert.equal(eventChangeValue(marker), marker);
  }
});

test('event changes omit the disclosure when details are absent or empty', () => {
  assert.equal(render(), '');
  assert.equal(render({ changes: [] }), '');
});

test('event changes render an accessible collapsed field comparison with sensitive markers', () => {
  const markup = render({ changes: [
    { path: 'components.engine.resources.maxCpu', before: 1, after: 2 },
    { path: 'backup.schedules[nightly].enabled', after: false },
    { path: 'backup.storage', before: 'archive' },
    { path: 'tls.privateKey', before: '(changed)', after: '(changed)', sensitive: true },
    { path: 'credentials.password', after: '(set)', sensitive: true },
    { path: 'credentials.oldPassword', before: '(unset)', sensitive: true },
  ] });
  assert.match(markup, /<button[^>]*type="button"[^>]*aria-expanded="false"[^>]*aria-controls="([^"]+)"/);
  const contentId = markup.match(/aria-controls="([^"]+)"/)![1];
  assert.ok(markup.includes(`<div id="${contentId}" hidden="">`));
  assert.match(markup, /Changed fields \(6\)/);
  for (const header of ['Path', 'Before', 'After']) assert.ok(markup.includes(`<th scope="col">${header}</th>`));
  assert.match(markup, /components\.engine\.resources\.maxCpu/);
  assert.match(markup, /backup\.schedules\[nightly\]\.enabled/);
  for (const value of ['1', '2', 'false', 'archive', '(changed)', '(set)', '(unset)']) {
    assert.ok(markup.includes(`>${value}</td>`), `Missing rendered value: ${value}`);
  }
  assert.equal((markup.match(/>Sensitive<\/span>/g) ?? []).length, 3);
});

test('event change paths and string values are escaped rather than treated as HTML', () => {
  const markup = render({ changes: [{ path: '<script>path</script>', after: '<img src=x>' }] });
  assert.match(markup, /&lt;script&gt;path&lt;\/script&gt;/);
  assert.match(markup, /&lt;img src=x&gt;/);
  assert.doesNotMatch(markup, /<script>|<img /);
});

test('the shared cluster and global event table keeps the message beside structured changes', () => {
  const event: components['schemas']['Event'] = {
    id: 1,
    type: 'Audit',
    createdAt: '2026-01-01T00:00:00Z',
    reason: 'ClusterSpecChangeRequested',
    message: 'Cluster spec changed for demo (generation 3): components.engine.resources.maxCpu: 1 → 2',
    actor: 'operator',
    details: { changes: [{ path: 'components.engine.resources.maxCpu', before: 1, after: 2 }] },
  };
  const markup = renderToStaticMarkup(createElement(EventTable, { items: [event] }));
  assert.ok(markup.includes(event.message));
  assert.match(markup, /\(ClusterSpecChangeRequested\)/);
  assert.match(markup, /Changed fields \(1\)/);
  assert.match(markup, />operator<\/td>/);
  const historical = renderToStaticMarkup(createElement(EventTable, { items: [{ ...event, details: undefined }] }));
  assert.ok(historical.includes(event.message));
  assert.doesNotMatch(historical, /Changed fields/);
});

test('truncated event values distinguish omitted content from genuinely unset fields', () => {
  assert.equal(eventChangeValue(undefined, true), '(omitted)');
  assert.equal(eventChangeValue(undefined, false), '(unset)');
  assert.equal(eventChangeValue(null, true), 'null');
  assert.equal(eventChangeValue('(4096 bytes)', true), '(4096 bytes)');
  assert.equal(eventChangeValue('(changed)', true), '(changed)');
});

test('truncated event details disclose omitted values and an incomplete comparison', () => {
  const markup = render({ truncated: true, changes: [
    { path: 'engineConfig', truncated: true },
    { path: 'components.pipeline', after: '(4096 bytes)', truncated: true },
    { path: 'backup.storage', after: 'archive' },
  ] });
  assert.match(markup, /Changed fields \(3, truncated\)/);
  assert.match(markup, /some values or fields were omitted/);
  assert.match(markup, /not a complete comparison/);
  assert.equal((markup.match(/>\(omitted\)<\/td>/g) ?? []).length, 3);
  assert.equal((markup.match(/>Truncated<\/span>/g) ?? []).length, 2);
  assert.match(markup, />\(4096 bytes\)<\/td>/);
  assert.match(markup, />\(unset\)<\/td>/);
});

test('truncation remains visible even when no change paths fit the payload limit', () => {
  for (const details of [{ truncated: true, changes: [] }, { truncated: true }]) {
    const markup = render(details);
    assert.match(markup, /Changed fields \(0, truncated\)/);
    assert.match(markup, /some values or fields were omitted/);
  }
});
