// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { Schemas } from '@/api/client';
import { memberHealthCounts, rememberMemberRoles } from './member-status.ts';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports execute before registration and cannot load TSX in Node.
const { HealthSummary, MemberHealthBadge, MemberRole } = await import('./cluster-utils.tsx');

const primary: Schemas['Member'] = { name: 'database0', set: 'rs0', role: 'primary', state: 'PRIMARY', health: true };
const secondary: Schemas['Member'] = { name: 'database1', set: 'rs0', role: 'secondary', state: 'SECONDARY', health: true };
const leaving: Schemas['Member'] = { ...secondary, role: 'unknown', state: 'LEAVING', health: false };

function summary(members: Schemas['Member'][]) {
  // HealthSummary reads only the member inventory, not desired configuration.
  const cluster = { status: { members } } as Schemas['Cluster'];
  return renderToStaticMarkup(createElement(HealthSummary, { cluster }));
}

test('leaving members are in progress rather than unhealthy in tables and summaries', () => {
  assert.deepEqual(memberHealthCounts([primary, leaving]), { healthy: 1, total: 1, leaving: 1 });
  const health = renderToStaticMarkup(createElement(MemberHealthBadge, { member: leaving }));
  assert.match(health, /badge-progress[^>]*>Leaving/);
  assert.doesNotMatch(health, /unhealthy|badge-danger/);
  const markup = summary([primary, leaving]);
  assert.match(markup, /1\/1 healthy/);
  assert.match(markup, /1 Leaving/);
  assert.doesNotMatch(markup, /badge-warn|badge-danger/);
  assert.deepEqual(memberHealthCounts([{ ...leaving, health: true }]), { healthy: 0, total: 0, leaving: 1 });
  assert.doesNotMatch(summary([leaving]), /healthy|badge-danger/);
});

test('a leaving member retains its last observed role without inventing an unknown role', () => {
  const before = rememberMemberRoles([primary, secondary]);
  const roles = rememberMemberRoles([primary, leaving], before);
  assert.equal(roles.get(leaving.name), 'secondary');
  const role = renderToStaticMarkup(createElement(MemberRole, { member: leaving, roles }));
  assert.match(role, /badge-plain[^>]*>secondary/);
  assert.doesNotMatch(role, /unknown|badge-warn|badge-danger/);
  const fresh = renderToStaticMarkup(createElement(MemberRole, { member: leaving }));
  assert.match(fresh, /badge-plain[^>]*>Leaving/);
  assert.doesNotMatch(fresh, /unknown|badge-warn|badge-danger/);
  assert.equal(rememberMemberRoles([primary], roles).has(leaving.name), false);
  assert.equal(rememberMemberRoles([leaving]).has(leaving.name), false);
  assert.equal(rememberMemberRoles([{ ...leaving, state: 'DOWN' }], roles).has(leaving.name), false);
});

test('genuinely unhealthy members still count as unhealthy and suspended health stays historical', () => {
  const down = { ...secondary, state: 'DOWN', health: false };
  assert.deepEqual(memberHealthCounts([primary, down, leaving]), { healthy: 1, total: 2, leaving: 1 });
  assert.match(renderToStaticMarkup(createElement(MemberHealthBadge, { member: down })), /badge-danger[^>]*>unhealthy/);
  assert.match(summary([primary, down, leaving]), /badge-warn[^>]*>1\/2 healthy/);
  const suspended = renderToStaticMarkup(createElement(MemberHealthBadge, { member: leaving, suspended: true }));
  assert.match(suspended, />historical</);
  assert.doesNotMatch(suspended, /Leaving|healthy|badge-(?:danger|warn|ok|progress)/);
});
