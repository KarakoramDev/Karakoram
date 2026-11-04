// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Client } from 'openapi-fetch';
import type { paths } from '../../api/schema';
import type { Schemas } from '../../api/client';

export type UnsealKeys = Schemas['OpenBaoUnsealKeys'];

/**
 * Revealed material lives only in the reducer state of the panel that requested
 * it: never in React Query, never derived into other components. Hiding or
 * unmounting the panel drops it.
 */
export interface RevealState {
  keys: UnsealKeys | null;
  busy: boolean;
  error: string | null;
}

export type RevealAction = { type: 'request' } | { type: 'resolved'; keys: UnsealKeys } | { type: 'failed'; error: string } | { type: 'hide' };

export const INITIAL_REVEAL: RevealState = { keys: null, busy: false, error: null };

export function revealReducer(_state: RevealState, action: RevealAction): RevealState {
  switch (action.type) {
    case 'request':
      return { keys: null, busy: true, error: null };
    case 'resolved':
      return { keys: action.keys, busy: false, error: null };
    case 'failed':
      return { keys: null, busy: false, error: action.error };
    case 'hide':
      return INITIAL_REVEAL;
  }
}

/** One audited POST per click; the caller unwraps the result and keeps it out of every cache. */
export function revealUnsealKeys(client: Client<paths>, namespace: string, cluster: string, confirm: string, signal?: AbortSignal) {
  return client.POST('/clusters/{cluster}/openbao/unseal-keys/reveal', { params: { path: { cluster }, query: { namespace } }, body: { confirm }, signal });
}

export function shareLabel(keys: Pick<UnsealKeys, 'recoveryShares'>, index: number): string {
  return `${keys.recoveryShares ? 'Recovery' : 'Unseal'} share ${index + 1}`;
}

/** Plain-text export for an offline password manager or printed envelope. */
export function unsealKeysExport(cluster: string, keys: UnsealKeys): string {
  const kind = keys.recoveryShares ? 'recovery' : 'unseal';
  return [
    `# OpenBao cluster ${cluster} — ${kind} shares (${keys.threshold} of ${keys.shares.length} required), seal ${keys.sealType}, initialized ${keys.initializedAt}`,
    ...keys.shares.map((share, index) => `${kind}-share-${index + 1}: ${share}`),
    `initial-root-token: ${keys.initialRootToken}`,
  ].join('\n');
}
