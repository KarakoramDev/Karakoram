// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useLayoutEffect, useRef } from 'react';
import { useIdempotencyKey } from '@/api/idempotency';
import { refreshSubmissionBody } from './detail-state';

/** Editing a request body starts a new attempt; unchanged retries retain their key. */
export function useSubmissionKey(body: string): { key: string; reset(): void } {
  const submission = useIdempotencyKey();
  const attempt = useRef({ body });
  useLayoutEffect(() => {
    refreshSubmissionBody(attempt.current, body, submission.reset);
  }, [body, submission]);
  return submission;
}
