// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

export function validatePitrPoint(value: string | undefined, window: Schemas['PITRWindow'] | null): string[] {
  if (!window) return ['Wait for the source PITR coverage to load.'];
  if (!window.available || !window.start || !window.end) return [window.message || 'No continuous PITR window is available for the source cluster.'];
  if (!value) return ['Choose a point in the restorable window.'];
  const instant = Date.parse(value);
  if (!Number.isFinite(instant) || instant < Date.parse(window.start) || instant > Date.parse(window.end)) return ['Outside the restorable window.'];
  return [];
}
