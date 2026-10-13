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
  | 'monitoring'
  | 'namespaces'
  | 'accounts'
  | 'api-tokens'
  | 'rbac'
  | 'settings'
  | 'providers';

export type Action = 'create' | 'read' | 'update' | 'delete' | 'export';

export const DEFAULT_NAMESPACE = 'default';

/** RBAC object of a cluster-scoped resource: `<namespace>/<cluster>`. */
export function clusterObject(namespace: string, cluster: string): string {
  return `${namespace || DEFAULT_NAMESPACE}/${cluster}`;
}

/**
 * Mirrors the server's policy-object normalization: a bare `*` means
 * everything, and an object without a namespace segment refers to the
 * default namespace. Namespace-scoped resources (namespaces) and global ones
 * (settings, accounts, …) keep their object unchanged.
 */
function normalizeObject(pattern: string): string {
  if (pattern === '*') return '**';
  if (pattern.startsWith('**') || pattern.includes('/')) return pattern;
  return `${DEFAULT_NAMESPACE}/${pattern}`;
}

/**
 * Doublestar-style glob matched against a `/`-separated object: `*` matches
 * within one segment, `**` spans segments and `x/**` also matches `x` itself.
 * Simple resources (resource and action names) have no separator, so the
 * same matcher serves them.
 */
export function globMatch(pattern: string, value: string): boolean {
  if (pattern === '*' || pattern === '**') return true;
  if (!pattern.includes('*')) return pattern === value;
  let re = '';
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i]!;
    if (ch === '*') {
      if (pattern[i + 1] === '*') {
        i++;
        if (pattern[i + 1] === '/') {
          i++;
          re += '(?:.*/)?';
        } else {
          re += '.*';
        }
      } else {
        re += '[^/]*';
      }
    } else if (ch === '/' && pattern.slice(i + 1) === '**') {
      re += '(?:/.*)?';
      break;
    } else {
      re += ch.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
    }
  }
  return new RegExp(`^${re}$`).test(value);
}

/** Resources whose RBAC object is a cluster or storage inside a namespace. */
const NAMESPACED: Partial<Record<Resource, true>> = { clusters: true, 'cluster-credentials': true, 'database-users': true, backups: true, restores: true, 'backup-storages': true, operations: true, events: true, monitoring: true };

/**
 * Mirrors the server policy effect: allowed when at least one matching allow
 * rule exists and no matching deny rule exists.
 */
export function evaluate(perms: Schemas['Permission'][], resource: Resource, action: Action, object = '*'): boolean {
  let allowed = false;
  for (const p of perms) {
    if (!globMatch(p.resource, resource) || !globMatch(p.action, action)) continue;
    const obj = NAMESPACED[resource] ? normalizeObject(p.object) : p.object;
    if (object !== '*' && !globMatch(obj, object)) continue;
    if (object === '*' && obj !== '*' && !obj.includes('*')) {
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
