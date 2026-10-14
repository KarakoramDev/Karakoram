// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export function tokenNameError(name: string): string | undefined {
  const length = [...name.trim()].length;
  if (length === 0) return 'Name is required and must not be blank.';
  if (length > 128) return 'Name must contain 1–128 characters after trimming whitespace.';
  return undefined;
}

export function tokenExpiry(input: string, now = Date.now()): { expiresAt?: string; error?: string } {
  if (!input) return {};
  const date = new Date(input);
  if (!Number.isFinite(date.getTime())) return { error: 'Expiry must be a valid date and time.' };
  if (date.getTime() <= now) return { error: 'Expiry must be in the future.' };
  return { expiresAt: date.toISOString() };
}

/** Preserve the request body and key together across lost-response retries. */
export function tokenSubmission(uuid: () => string = () => crypto.randomUUID()) {
  let busy = false;
  let fingerprint: string | undefined;
  let key: string | undefined;
  return {
    begin(name: string, expiresAt?: string) {
      if (busy) return null;
      const next = JSON.stringify({ name: name.trim(), expiresAt });
      if (fingerprint !== next) { fingerprint = next; key = uuid(); }
      busy = true;
      return { name: name.trim(), expiresAt, key: key! };
    },
    release() { busy = false; },
  };
}
