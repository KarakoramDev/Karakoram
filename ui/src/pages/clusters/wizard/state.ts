// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { normalizeOptions } from '../../../engines/elasticsearch/spec.ts';
import { normalizeSpec } from '../../../engines/postgres/spec.ts';
import { normalizeOptions as normalizeWeaviateOptions } from '../../../engines/weaviate/helpers.ts';

export const STEPS = [
  { id: 'basics', label: 'Basics' },
  { id: 'topology', label: 'Topology' },
  { id: 'resources', label: 'Resources' },
  { id: 'backups', label: 'Backups' },
  { id: 'access', label: 'Access & TLS' },
  { id: 'review', label: 'Review' },
] as const;
export type StepId = (typeof STEPS)[number]['id'];
export type Validation = Record<Exclude<StepId, 'review'>, string[]>;

export function nextStep(current: StepId, validation: Validation): StepId {
  if (current === 'review' || validation[current].length) return current;
  return STEPS[STEPS.findIndex((step) => step.id === current) + 1]!.id;
}

/** Ignore stale handlers and the repeated clicks in a browser multi-click gesture. */
export function navigateStep(current: StepId, rendered: StepId, target: StepId, clickCount: number): StepId {
  return current === rendered && clickCount <= 1 ? target : current;
}

export function firstInvalidStep(validation: Validation): Exclude<StepId, 'review'> | undefined {
  for (const step of STEPS) if (step.id !== 'review' && validation[step.id].length) return step.id;
}

/** Clear checked flags when their dependency disappears; use for optional component public surfaces too. */
export function clearDisabledFlags<T extends object>(settings: T, dependencies: Partial<Record<keyof T, boolean>>): T {
  let result = settings;
  for (const key of Object.keys(dependencies) as (keyof T)[]) {
    if (dependencies[key] === false && settings[key] === true) result = { ...result, [key]: false };
  }
  return result;
}

export function normalizeWizardSpec(spec: Schemas['ClusterSpec']): Schemas['ClusterSpec'] {
  if (spec.provider === 'elasticsearch') spec = normalizeOptions(spec);
  spec = normalizeSpec(spec);
  spec = normalizeWeaviateOptions(spec);
  const pitr = spec.backup?.pitr;
  if (!pitr) return spec;
  const normalized = clearDisabledFlags(pitr, { enabled: Boolean(spec.backup?.storage) });
  return normalized === pitr ? spec : { ...spec, backup: { ...spec.backup, pitr: normalized } };
}

/** Claim the request synchronously, before a pending React render can disable the control. */
export function claimSubmission(lock: { current: boolean }): boolean {
  if (lock.current) return false;
  lock.current = true;
  return true;
}

/** Creation has exactly two outcomes: reveal the owning invalid step, or claim one request. */
export function submitWizard(validation: Validation, lock: { current: boolean }, onInvalid: (step: Exclude<StepId, 'review'>) => void, create: () => void): void {
  if (lock.current) return;
  const invalid = firstInvalidStep(validation);
  if (invalid) {
    onInvalid(invalid);
    return;
  }
  if (claimSubmission(lock)) create();
}

/** An edited failed form is a new attempt; an unchanged retry keeps the original key. */
export function resetEditedAttempt(submitted: { current: boolean }, pending: boolean): boolean {
  if (!submitted.current || pending) return false;
  submitted.current = false;
  return true;
}
