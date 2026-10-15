// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

type Member = Schemas['Member'];
export type MemberRoles = ReadonlyMap<string, Member['role']>;

/** Keep observed roles only while those members remain present. */
export function rememberMemberRoles(members: Member[], previous: MemberRoles = new Map()): MemberRoles {
  const roles = new Map<string, Member['role']>();
  for (const member of members) {
    const role = member.role !== 'unknown' ? member.role : member.state === 'LEAVING' ? previous.get(member.name) : undefined;
    if (role) roles.set(member.name, role);
  }
  return roles;
}

export function memberHealthCounts(members: Member[]): { healthy: number; total: number; leaving: number } {
  let healthy = 0;
  let total = 0;
  let leaving = 0;
  for (const member of members) {
    if (member.state === 'LEAVING') leaving++;
    else {
      total++;
      if (member.health) healthy++;
    }
  }
  return { healthy, total, leaving };
}
