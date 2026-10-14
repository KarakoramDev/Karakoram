// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export function exactUtc(iso: string | undefined | null): string {
  if (!iso) return '—';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  if (iso.endsWith('Z')) return iso;
  const fraction = iso.match(/\.(\d+)(?:[+-]\d{2}:\d{2})$/)?.[1];
  return date.toISOString().replace(/\.\d{3}Z$/, fraction ? `.${fraction}Z` : 'Z');
}

export function elapsedSeconds(start: string | undefined, end: string | undefined, now = Date.now()): number | undefined {
  if (!start) return undefined;
  const a = Date.parse(start);
  const b = end ? Date.parse(end) : now;
  return Number.isFinite(a) && Number.isFinite(b) && b >= a ? (b - a) / 1000 : undefined;
}

export function errorDetails(error: unknown): string[] {
  if (!error || typeof error !== 'object' || !('details' in error) || !error.details || typeof error.details !== 'object') return [];
  return Object.entries(error.details).map(([key, value]) => `${key}: ${typeof value === 'string' ? value : JSON.stringify(value)}`);
}

export function namespaceMissing(error: unknown): string | undefined {
  if (!error || typeof error !== 'object' || !('code' in error) || error.code !== 'not_found' || !('details' in error) || !error.details || typeof error.details !== 'object') return undefined;
  const details = error.details as Record<string, unknown>;
  return details.resource === 'namespace' && typeof details.name === 'string' ? details.name : undefined;
}

export function scopedPath(path: string, namespace: string | undefined): string {
  return `${path}?${new URLSearchParams({ namespace: namespace ?? '' })}`;
}

export function storageTransport(endpoint: string, verifyTLS: boolean): string {
  return /^https:\/\//i.test(endpoint) ? `HTTPS (certificate verification ${verifyTLS ? 'on' : 'off'})` : 'HTTP (no TLS)';
}

export function chartTicks(top: number, format: (value: number) => string, categories?: Record<number, string>): number[] {
  if (categories) return Object.keys(categories).map(Number).filter((v) => v >= 0 && v <= top).sort((a, b) => a - b);
  const values = Number.isInteger(top) && top <= 4 ? Array.from({ length: top + 1 }, (_, i) => i) : [0, .25, .5, .75, 1].map((v) => v * top);
  const seen = new Set<string>();
  return values.filter((v) => { const label = format(v); if (seen.has(label)) return false; seen.add(label); return true; });
}

export const MONITORING_RANGES = ['15m', '1h', '6h', '24h'] as const;
export type MonitoringRange = typeof MONITORING_RANGES[number];
export function monitoringRange(value: string | null): MonitoringRange {
  return MONITORING_RANGES.find((v) => v === value) ?? '1h';
}

export function metricCategories(name: string): Record<number, string> | undefined {
  if (name === 'repl.memberState') return { 0: 'Startup', 1: 'Primary', 2: 'Secondary', 3: 'Recovering', 5: 'Startup 2', 6: 'Unknown', 7: 'Arbiter', 8: 'Down', 9: 'Rollback', 10: 'Removed' };
  if (/alarm/i.test(name)) return { 0: 'Clear', 1: 'Active' };
  if (name === 'elasticsearch.cluster.status') return { 0: 'Green', 1: 'Yellow', 2: 'Red' };
  if (name === 'kibana.status.level') return { 0: 'Level 0', 1: 'Level 1', 2: 'Level 2', 3: 'Level 3' };
  if (name === 'up' || name.endsWith('.up')) return { 0: 'Down', 1: 'Up' };
  return undefined;
}

export function formatBuildVersion(version: string): string {
  return /^[0-9]/.test(version) ? `v${version}` : version;
}
