// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

export function pitrIntervals(window: Schemas['PITRWindow'] | null | undefined): Schemas['PITRInterval'][] {
  if (!window?.available) return [];
  if (window.intervals?.length) return window.intervals;
  return window.start && window.end ? [{ start: window.start, end: window.end }] : [];
}

export function validatePitrPoint(value: string | undefined, window: Schemas['PITRWindow'] | null): string[] {
  if (!window) return ['Wait for the source PITR coverage to load.'];
  const intervals = pitrIntervals(window);
  if (!intervals.length) return [window.message || 'No continuous PITR window is available for the source cluster.'];
  if (!value) return ['Choose a point in the restorable window.'];
  const instant = Date.parse(value);
  if (!Number.isFinite(instant) || !intervals.some((range) => instant >= Date.parse(range.start) && instant <= Date.parse(range.end))) return ['Outside the restorable window.'];
  return [];
}
