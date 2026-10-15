// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { chartTicks, chartTimeTicks, elapsedSeconds, errorDetails, exactUtc, formatBuildVersion, metricCategories, monitoringRange, namespaceMissing, scopedPath, storageTransport } from './presentation.ts';
import { beginSubmission, backgroundDataStale, copyText } from './submission.ts';
import { applyEventFilters, eventFilters } from '../event-filters.ts';

test('timestamps preserve exact UTC seconds and offsets are normalized', () => {
  assert.equal(exactUtc('2026-01-02T04:05:06.123+02:00'), '2026-01-02T02:05:06.123Z');
  assert.equal(exactUtc('2026-01-02T02:05:06.123456789Z'), '2026-01-02T02:05:06.123456789Z');
  assert.equal(exactUtc('2026-01-02T04:05:06.123456789+02:00'), '2026-01-02T02:05:06.123456789Z');
  assert.equal(exactUtc(undefined), '—');
  assert.equal(exactUtc('invalid'), 'invalid');
});

test('operation duration uses completion rather than current time', () => {
  assert.equal(elapsedSeconds('2026-01-02T00:00:00Z', '2026-01-02T00:01:30Z'), 90);
  assert.equal(elapsedSeconds('2026-01-02T00:00:00Z', undefined, Date.parse('2026-01-02T00:00:03Z')), 3);
  assert.equal(elapsedSeconds(undefined, undefined), undefined);
  assert.equal(elapsedSeconds('bad', undefined), undefined);
  assert.equal(elapsedSeconds('2026-01-02T00:01:00Z', '2026-01-02T00:00:00Z'), undefined);
});

test('event filters round-trip across refresh and retain namespace scope', () => {
  const url = applyEventFilters(new URLSearchParams('namespace=default&range=24h'), { actor: 'operator', resource: 'auth', type: 'Warning', cluster: 'sample' });
  assert.deepEqual(eventFilters(new URLSearchParams(url.toString())), { actor: 'operator', resource: 'auth', type: 'Warning', cluster: 'sample' });
  assert.equal(url.get('namespace'), 'default');
  assert.equal(url.get('range'), '24h');
  const cleared = applyEventFilters(url, { actor: '', resource: ' ' });
  assert.equal(cleared.has('actor'), false);
  assert.equal(cleared.has('resource'), false);
  assert.equal(cleared.get('type'), 'Warning');
});

test('namespace navigation explicitly preserves a named or all-namespace choice', () => {
  assert.equal(scopedPath('/events', 'default'), '/events?namespace=default');
  assert.equal(scopedPath('/operations', undefined), '/operations?namespace=');
});

test('monitoring ranges round-trip and unsupported ranges never reach the API', () => {
  assert.equal(monitoringRange(new URLSearchParams('range=24h').get('range')), '24h');
  assert.equal(monitoringRange('7d'), '1h');
  assert.equal(monitoringRange(null), '1h');
});

test('integer and categorical chart ticks never repeat rounded labels', () => {
  assert.deepEqual(chartTicks(1, (v) => v === 0 ? 'Clear' : 'Active'), [0, 1]);
  assert.deepEqual(chartTicks(2, (v) => ['Green', 'Yellow', 'Red'][Math.round(v)]!), [0, 1, 2]);
  const states = metricCategories('repl.memberState')!;
  assert.equal(states[1], 'Primary');
  assert.equal(states[2], 'Secondary');
  const ticks = chartTicks(10, String, states);
  assert.equal(new Set(ticks.map((tick) => states[tick])).size, ticks.length);
  assert.equal(metricCategories('connections.current'), undefined);
});

test('monitoring time ticks are rounded, evenly spaced and distinct for every range', () => {
  for (const minutes of [15, 60, 360, 1440]) {
    const start = Date.parse('2026-01-02T12:47:01Z');
    const end = start + minutes * 60_000;
    const ticks = chartTimeTicks(start, end, 'en-US', 'UTC');
    assert.ok(ticks.length >= 3 && ticks.length <= 5);
    assert.equal(new Set(ticks.map((tick) => tick.label)).size, ticks.length);
    const spacing = ticks[1]!.time - ticks[0]!.time;
    for (let index = 0; index < ticks.length; index++) {
      assert.ok(ticks[index]!.time >= start && ticks[index]!.time <= end);
      assert.equal(ticks[index]!.time % 60_000, 0);
      if (index > 0) assert.equal(ticks[index]!.time - ticks[index - 1]!.time, spacing);
    }
    if (minutes === 1440) {
      assert.match(ticks[0]!.label, /Jan 2/);
      assert.match(ticks.at(-1)!.label, /Jan 3/);
    }
  }
});

test('short monitoring windows crossing midnight include dates and daylight-saving ticks stay distinct', () => {
  const midnight = chartTimeTicks(Date.parse('2026-01-02T23:52:00Z'), Date.parse('2026-01-03T00:07:00Z'), 'en-US', 'UTC');
  assert.match(midnight[0]!.label, /Jan 2/);
  assert.match(midnight.at(-1)!.label, /Jan 3/);
  const fallback = chartTimeTicks(Date.parse('2026-11-01T05:00:00Z'), Date.parse('2026-11-01T07:00:00Z'), 'en-US', 'America/New_York');
  assert.equal(new Set(fallback.map((tick) => tick.label)).size, fallback.length);
  assert.ok(fallback.some((tick) => tick.label.includes('GMT-4')));
  assert.ok(fallback.some((tick) => tick.label.includes('GMT-5')));
});

test('a single timestamp produces one meaningful time label rather than repeated minute labels', () => {
  const time = Date.parse('2026-01-02T12:37:12Z');
  const ticks = chartTimeTicks(time, time, 'en-US', 'UTC');
  assert.deepEqual(ticks, [{ time, label: '12:37:12 PM' }]);
});

test('TLS settings describe configuration, never a verification result', () => {
  assert.equal(storageTransport('http://storage.example', true), 'HTTP (no TLS)');
  assert.equal(storageTransport('https://storage.example', true), 'HTTPS (certificate verification on)');
  assert.equal(storageTransport('https://storage.example', false), 'HTTPS (certificate verification off)');
});

test('submission latch rejects double clicks synchronously and can unlock after failure', () => {
  const latch = { locked: false };
  assert.equal(beginSubmission(latch, false), true);
  assert.equal(beginSubmission(latch, false), false);
  latch.locked = false;
  assert.equal(beginSubmission(latch, true), false);
  assert.equal(beginSubmission(latch, false), true);
});

test('background connection errors override cached live data but permission errors do not', () => {
  assert.equal(backgroundDataStale({ data: {}, error: new TypeError('offline'), fetchFailureCount: 0 }), true);
  assert.equal(backgroundDataStale({ data: {}, error: null, fetchFailureCount: 1, fetchFailureReason: { status: 502 } }), true);
  assert.equal(backgroundDataStale({ data: {}, error: null, fetchFailureCount: 0 }), false);
  assert.equal(backgroundDataStale({ data: {}, error: { status: 403 }, fetchFailureCount: 1 }), false);
});

test('clipboard rejection yields visible manual-copy guidance without rejecting', async () => {
  assert.match((await copyText('text', async () => { throw new Error('denied'); }))!, /copy it manually/);
  let copied = '';
  assert.equal(await copyText('text', async (value) => { copied = value; }), undefined);
  assert.equal(copied, 'text');
});

test('server field and resource details accompany errors', () => {
  assert.deepEqual(errorDetails({ details: { field: 'name', resource: 'namespace', reason: 'not_found' } }), ['field: name', 'resource: namespace', 'reason: not_found']);
  assert.deepEqual(errorDetails(new Error('offline')), []);
});

test('namespace not-found errors are distinct from missing clusters', () => {
  assert.equal(namespaceMissing({ code: 'not_found', details: { resource: 'namespace', name: 'missing' } }), 'missing');
  assert.equal(namespaceMissing({ code: 'not_found', details: { resource: 'cluster', name: 'missing' } }), undefined);
});

test('only numeric release versions receive the v prefix', () => {
  assert.equal(formatBuildVersion('1.2.3'), 'v1.2.3');
  assert.equal(formatBuildVersion('dev'), 'dev');
  assert.equal(formatBuildVersion('unknown (built without git metadata)'), 'unknown (built without git metadata)');
});
