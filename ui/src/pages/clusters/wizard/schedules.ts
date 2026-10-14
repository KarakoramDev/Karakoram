// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

const CRON_FIELD = /^(\*|[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*)(\/[0-9]+)?$/;

export function validateCron(expr: string): boolean {
  const parts = expr.trim().split(/\s+/);
  return parts.length === 5 && parts.every((p) => CRON_FIELD.test(p));
}

export function scheduleErrors(s: Schemas['BackupSchedule'], items: Schemas['BackupSchedule'][]): Partial<Record<keyof Schemas['BackupSchedule'], string>> {
  const errors: Partial<Record<keyof Schemas['BackupSchedule'], string>> = {};
  if (!/^[a-z][a-z0-9-]{0,30}$/.test(s.name)) errors.name = 'Use 1–31 lowercase letters, digits and dashes, starting with a letter.';
  else if (items.filter((item) => item.name === s.name).length > 1) errors.name = 'Schedule names must be unique.';
  if (!validateCron(s.cron)) errors.cron = 'Five fields: minute hour day month weekday.';
  if (!s.storage) errors.storage = 'Choose a storage.';
  if (!Number.isInteger(s.retainCount) || s.retainCount < 1) errors.retainCount = 'Retain count must be a whole number of at least 1.';
  return errors;
}

export function validateSchedules(items: Schemas['BackupSchedule'][]): string[] {
  return items.flatMap((s, i) => Object.values(scheduleErrors(s, items)).map((e) => `Schedule ${s.name || `#${i + 1}`}: ${e}`));
}
