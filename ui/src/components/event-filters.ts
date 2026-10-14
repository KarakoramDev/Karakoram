// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export function eventFilters(params: URLSearchParams) {
  return { actor: params.get('actor') || undefined, resource: params.get('resource') || undefined, type: params.get('type') || undefined, cluster: params.get('cluster') || undefined };
}

/** Preserve the namespace and unrelated page state while applying filters. */
export function applyEventFilters(previous: URLSearchParams, values: Record<string, string>): URLSearchParams {
  const next = new URLSearchParams(previous);
  for (const [field, raw] of Object.entries(values)) { const value = raw.trim(); if (value) next.set(field, value); else next.delete(field); }
  return next;
}
