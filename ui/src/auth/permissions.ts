// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

export type Resource =
  | 'clusters'
  | 'cluster-credentials'
  | 'database-users'
  | 'backups'
  | 'restores'
  | 'backup-storages'
  | 'operations'
  | 'events'
  | 'audit'
  | 'monitoring'
  | 'accounts'
  | 'api-tokens'
  | 'rbac'
  | 'settings'
  | 'providers';

export type Action = 'create' | 'read' | 'update' | 'delete';

/** Casbin-style glob: `*` matches any run of characters, everything else literal. */
export function globMatch(pattern: string, value: string): boolean {
  if (pattern === '*') return true;
  if (!pattern.includes('*')) return pattern === value;
  const re = new RegExp('^' + pattern.split('*').map((s) => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$');
  return re.test(value);
}

/**
 * Mirrors the server policy effect: allowed when at least one matching allow
 * rule exists and no matching deny rule exists.
 */
export function evaluate(perms: Schemas['Permission'][], resource: Resource, action: Action, object = '*'): boolean {
  let allowed = false;
  for (const p of perms) {
    if (!globMatch(p.resource, resource) || !globMatch(p.action, action)) continue;
    // Installation audit is a global object, not an entry point for scoped resources.
    if ((object !== '*' || resource === 'audit') && !globMatch(p.object, object)) continue;
    if (object === '*' && p.object !== '*' && !p.object.includes('*')) {
      // A rule scoped to a specific object does not grant the global capability,
      // but it does mean the caller can act on *something*; treat as allow so
      // "create cluster" style entry points stay reachable and the server
      // remains the authority for the concrete object.
      if (p.effect === 'deny') continue;
    }
    if (p.effect === 'deny') return false;
    allowed = true;
  }
  return allowed;
}
