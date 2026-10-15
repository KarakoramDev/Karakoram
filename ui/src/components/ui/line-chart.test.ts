// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToString } from 'react-dom/server';
import { chartTimeTicks } from './presentation.ts';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports execute before registration and cannot load TSX in Node.
const { LineChart } = await import('./LineChart.tsx');

test('a sparse 24-hour query keeps the full requested time axis with distinct dated ticks', () => {
  const start = Date.parse('2026-01-02T12:47:01Z');
  const end = start + 24 * 60 * 60_000;
  const markup = renderToString(createElement(LineChart, {
    title: 'CPU',
    timeRange: [start, end],
    series: [{ name: 'database0', points: [[end - 10 * 60_000, 12]] }],
  }));
  const labels = [...markup.matchAll(/<text[^>]*y="155"[^>]*>([^<]+)<\/text>/g)].map((match) => match[1]!);
  assert.equal(labels.length, 4);
  assert.equal(new Set(labels).size, labels.length);
  assert.match(markup, /CPU/);
  assert.deepEqual(labels, chartTimeTicks(start, end).map((tick) => tick.label));
});

test('a standalone chart with one sample renders only one time tick', () => {
  const markup = renderToString(createElement(LineChart, {
    title: 'Connections',
    series: [{ name: 'database0', points: [[Date.parse('2026-01-02T12:37:00Z'), 12]] }],
  }));
  assert.equal([...markup.matchAll(/<text[^>]*y="155"/g)].length, 1);
});
