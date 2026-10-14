// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';

export function idempotencyHeaders(key: string): { 'Idempotency-Key': string } {
  return { 'Idempotency-Key': key };
}

/** Retain the key on failure so retrying cannot duplicate accepted work. */
export function useIdempotencyKey(): { key: string; reset(): void } {
  const [key, setKey] = useState(() => crypto.randomUUID());
  return { key, reset: () => setKey(crypto.randomUUID()) };
}
