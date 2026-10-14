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
