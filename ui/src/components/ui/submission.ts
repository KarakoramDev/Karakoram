// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** A mutable latch closes before React can commit a pending-state render. */
export function beginSubmission(latch: { locked: boolean }, busy: boolean): boolean {
  if (busy || latch.locked) return false;
  latch.locked = true;
  return true;
}

export async function copyText(value: string, write: (value: string) => Promise<void>): Promise<string | undefined> {
  try { await write(value); return undefined; } catch {
    return 'Clipboard access failed. Select the text below and copy it manually.';
  }
}

export function backgroundDataStale(state: { data: unknown; error: unknown; fetchFailureCount: number; fetchFailureReason?: unknown }): boolean {
  if (state.data === undefined || (!state.error && state.fetchFailureCount === 0)) return false;
  const failure = state.error ?? state.fetchFailureReason;
  return !(failure && typeof failure === 'object' && 'status' in failure && typeof failure.status === 'number' && failure.status < 500);
}
