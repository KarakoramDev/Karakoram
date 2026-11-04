// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import type { Schemas } from '../../api/client';
export const BUILTIN_ROLES = ['root', 'admin', 'viewer', 'read-only', 'operator_metadata_reader'];
export function protectedUser(user: Schemas['WeaviateUser']): boolean {
  return ['karakoram-admin', 'karakoram-cluster'].includes(user.name) || user.userType === 'db_env_user';
}
export function privilegedRoles(roles: string[]): boolean {
  return roles.some((role) => !['viewer', 'read-only'].includes(role));
}
export function parsePermissions(text: string): Schemas['WeaviatePermission'][] {
  const value: unknown = JSON.parse(text);
  if (!Array.isArray(value) || value.some((p) => !p || typeof p !== 'object' || Array.isArray(p) || typeof p.action !== 'string' || !p.action.trim())) throw new Error('Permissions must be a JSON array of objects with a non-empty action string.');
  return value as Schemas['WeaviatePermission'][];
}
// Ephemeral reveal only: never put this value in query/mutation caches, storage or URLs.
export function secretState(_current: string | null, event: { type: 'reveal'; key: string } | { type: 'clear' }): string | null {
  return event.type === 'reveal' ? event.key : null;
}
