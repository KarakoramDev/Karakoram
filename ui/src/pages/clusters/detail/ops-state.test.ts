// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { agentBuildState, drillPolicyErrors, durationSeconds, maintenanceErrors, placementCopy, plannedPlacementCopy } from './ops-state.ts';

test('agent rollout compares artifact identity, not release version or connectivity', () => {
  assert.equal(agentBuildState(undefined, 'target'), 'Unknown');
  assert.equal(agentBuildState({ connected: true, version: '1.0' }, 'target'), 'Unknown');
  assert.equal(agentBuildState({ connected: true, build: 'old', version: '1.0' }, 'target'), 'Outdated');
  assert.equal(agentBuildState({ connected: false, build: 'target', version: '1.0' }, 'target'), 'Current');
  assert.equal(agentBuildState({ connected: true, build: 'target' }), 'Unknown');
});

test('placement copy distinguishes backend authority, best effort, missing observations and co-location', () => {
  const groups = [{ name: 'a', members: ['m1'] }, { name: 'b', members: ['m2'] }];
  assert.match(placementCopy({ groups, source: 'backend', guaranteedAntiAffinity: true, warning: '' }).message, /backend enforces/);
  assert.match(placementCopy({ groups, source: 'backend', guaranteedAntiAffinity: false, warning: '' }).title, /not yet confirmed/);
  assert.match(placementCopy({ groups, source: 'best-effort', guaranteedAntiAffinity: false, warning: '' }).message, /do not guarantee/);
  assert.match(placementCopy({ groups: [], source: 'unavailable', guaranteedAntiAffinity: false, warning: '' }).title, /unavailable/);
  assert.match(placementCopy().message, /cannot be confirmed/);
  assert.match(placementCopy({ groups: [{ name: 'a', members: ['m1', 'm2'] }], source: 'backend', guaranteedAntiAffinity: false, warning: '' }).title, /share a placement group/);
  assert.equal(placementCopy({ groups, source: 'best-effort', guaranteedAntiAffinity: false, warning: 'Provider observation unavailable' }).message, 'Provider observation unavailable');
  assert.equal(placementCopy({ groups: [{ name: 'unknown', members: ['m1', 'm2'] }], source: 'best-effort', guaranteedAntiAffinity: false, warning: '' }).title, 'Placement unavailable');
});

test('wizard placement guidance follows namespace capabilities without assuming a backend', () => {
  assert.match(plannedPlacementCopy(['placement']).message, /authoritative member groups/);
  assert.match(plannedPlacementCopy([]).message, /does not guarantee/);
  assert.match(plannedPlacementCopy().message, /not available to this account/);
});

test('duration forms retain Go duration units and decimal syntax', () => {
  for (const [text, seconds] of [['15m', 900], ['1h30m', 5400], ['.5h', 1800], ['1.h', 3600], ['900000ms', 900], ['1000us', 0.001], ['1µs', 0.000001], ['1μs', 0.000001], ['1ns', 0.000000001]] as const) assert.equal(durationSeconds(text), seconds);
  for (const text of ['', '2 days', '-1h', '3hfoo', '10']) assert.equal(durationSeconds(text), undefined);
});

function assertFirstError(errors: string[], expected: RegExp) {
  const first = errors[0];
  assert.ok(first, 'Expected a validation error');
  assert.match(first, expected);
}

test('maintenance policy enforces window bounds and permits an always-open policy', () => {
  assert.deepEqual(maintenanceErrors({ timeZone: 'UTC', windows: [] }), []);
  assert.deepEqual(maintenanceErrors({ timeZone: 'Europe/Prague', windows: [{ schedule: '0 2 * * 0', duration: '15m' }, { schedule: '0 0 * * *', duration: '168h' }] }), []);
  assertFirstError(maintenanceErrors({ timeZone: 'Not/AZone' }), /time zone/);
  assert.match(maintenanceErrors({ timeZone: 'UTC', windows: [{ schedule: '* * * *', duration: '14m' }] }).join(' '), /five-field.*15m/);
  assertFirstError(maintenanceErrors({ timeZone: 'UTC', windows: [{ schedule: '0 2 * * *', duration: '169h' }] }), /168h/);
  assertFirstError(maintenanceErrors({ timeZone: 'UTC', windows: Array.from({ length: 17 }, () => ({ schedule: '0 2 * * *', duration: '1h' })) }), /16 windows/);
});

test('drill policy validates the full timeout range and five-field schedule', () => {
  for (const timeout of ['30m', '6h', '48h', '.5h']) assert.deepEqual(drillPolicyErrors({ enabled: true, schedule: '0 4 * * 0', timeout }), []);
  for (const timeout of ['29m', '49h', 'invalid']) assertFirstError(drillPolicyErrors({ enabled: true, schedule: '0 4 * * 0', timeout }), /timeout/);
  assertFirstError(drillPolicyErrors({ enabled: false, schedule: '', timeout: '6h' }), /five-field/);
});
