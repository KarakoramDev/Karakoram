// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export const PASSWORD_CHANGED_MESSAGE = 'Password changed. Sign in with your new password.';
export const ACCOUNT_NAME_RULE = 'Use 1–64 characters: start with a letter; then letters, digits, _, ., @ or -.';
export function accountNameError(name: string): string | undefined {
  return /^[a-zA-Z][a-zA-Z0-9_.@-]{0,63}$/.test(name) ? undefined : ACCOUNT_NAME_RULE;
}
export function passwordError(password: string, current?: string): string | undefined {
  if ([...password].length < 12 || new TextEncoder().encode(password).length > 1024) return 'Use at least 12 characters and at most 1024 bytes.';
  if (current !== undefined && password === current) return 'New password must differ from the current password.';
  return undefined;
}
export function loginFailureMessage(message: string, details: unknown): string {
  if (details && typeof details === 'object' && 'retryAfterSeconds' in details && typeof details.retryAfterSeconds === 'number') {
    return `${message}. Try again in ${details.retryAfterSeconds} seconds.`;
  }
  return message;
}

/** A warning, not authorization: the server validates effective rights and denies. */
export function hasAdminAssignment(csv: string): boolean {
  const bindings = new Map<string, string[]>();
  for (const raw of csv.split('\n')) {
    const row = raw.trim().split(',').map((value) => value.trim());
    if (row[0] === 'g' && row.length === 3) {
      const targets = bindings.get(row[1]!) ?? [];
      targets.push(row[2]!);
      bindings.set(row[1]!, targets);
    }
  }
  const reachesAdmin = (subject: string, visited = new Set<string>()): boolean => {
    if (subject === 'role:admin') return true;
    if (visited.has(subject)) return false;
    visited.add(subject);
    return (bindings.get(subject) ?? []).some((target) => reachesAdmin(target, visited));
  };
  return [...bindings.keys()].some((subject) => /^(user|group):.+/.test(subject) && reachesAdmin(subject));
}
