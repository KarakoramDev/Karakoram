// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../api/client';
import type { EngineChangeReview } from './types';

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** RFC 7386: objects merge recursively, null deletes a key, and all other values replace. */
export function applyMergePatch(current: unknown, patch: unknown): unknown {
  if (!isObject(patch)) return patch;
  const result = isObject(current) ? { ...current } : {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete result[key];
    else Object.defineProperty(result, key, {
      value: applyMergePatch(Object.hasOwn(result, key) ? result[key] : undefined, value),
      enumerable: true, configurable: true, writable: true,
    });
  }
  return result;
}

/** Use the same patch for the request and the engine's consequence review. */
export function reviewChanges(current: Schemas['ClusterSpec'], draft: Schemas['ClusterSpec'], engine: EngineChangeReview): { patch: Record<string, unknown>; consequences: string[] } {
  const patch = engine.buildPatch(current, draft);
  const applied = applyMergePatch(current, patch) as Schemas['ClusterSpec'];
  return { patch, consequences: engine.disruptions(current, applied) };
}
