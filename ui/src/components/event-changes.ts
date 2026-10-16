// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** Distinguish absent fields from omitted audit values; preserve server masking. */
export function eventChangeValue(value: unknown, truncated = false): string {
  if (value === undefined) return truncated ? '(omitted)' : '(unset)';
  if (typeof value === 'string') return value;
  return JSON.stringify(value) ?? '(unset)';
}
