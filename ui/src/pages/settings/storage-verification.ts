// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

type FailureReason = NonNullable<Schemas['StorageVerification']['failureReason']>;

const FAILURE_LABELS: Record<FailureReason, string> = {
  access_denied: 'Access denied',
  bucket_not_found: 'Bucket not found',
  endpoint_not_allowed: 'Endpoint not allowed',
  tls_error: 'TLS verification failed',
  unreachable: 'Endpoint unreachable',
  timeout: 'Verification timed out',
  unknown: 'Unknown verification failure',
};

export function storageFailureLabel(reason: Schemas['StorageVerification']['failureReason']): string | undefined {
  return reason ? FAILURE_LABELS[reason] : undefined;
}

// Replace, rather than merge, so an omitted verification clears a previous check
// even when a credential-only edit leaves every public connection field unchanged.
export function replaceStorageInList(current: Schemas['BackupStorageList'] | undefined, saved: Schemas['BackupStorage']): Schemas['BackupStorageList'] | undefined {
  if (!current) return current;
  return {
    ...current,
    items: current.items.map((storage) => (storage.namespace || 'default') === (saved.namespace || 'default') && storage.name === saved.name ? saved : storage),
  };
}
