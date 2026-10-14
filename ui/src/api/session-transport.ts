// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export const REQUEST_TIMEOUT_MS = 30_000;
export const MIN_REFRESH_INTERVAL_MS = 10_000;
export const TEMPORARY_UNAVAILABILITY = 'Karakoram is temporarily unavailable. Your session has been kept; please retry.';

export class SessionUnavailableError extends Error {
  constructor() {
    super(TEMPORARY_UNAVAILABILITY);
    this.name = 'SessionUnavailableError';
  }
}

interface SessionTransportOptions {
  base: string;
  fetch: typeof fetch;
  csrf: { get(): string | null; set(token: string): void };
  unauthorized(): void;
  now?: () => number;
  timeoutMs?: number;
}

/** One rotation per recovery window, shared by all ordinary API requests. */
export function createSessionTransport(options: SessionTransportOptions) {
  const now = options.now ?? Date.now;
  const timeoutMs = options.timeoutMs ?? REQUEST_TIMEOUT_MS;
  let refreshing: Promise<boolean> | null = null;
  let lastSuccess = -Infinity;
  let rejected = false;

  const boundedFetch: typeof fetch = (input, init) => {
    const incoming = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    const timeout = AbortSignal.timeout(timeoutMs);
    const signal = incoming ? AbortSignal.any([incoming, timeout]) : timeout;
    return options.fetch(input, { ...init, signal });
  };

  const reject = () => {
    rejected = true;
    options.unauthorized();
  };

  async function refresh(): Promise<boolean> {
    if (rejected) return false;
    if (refreshing) return refreshing;
    if (now() - lastSuccess < MIN_REFRESH_INTERVAL_MS) {
      reject();
      return false;
    }
    const token = options.csrf.get();
    if (!token) return false;
    // Assign before starting work; even an immediate failure releases single-flight.
    refreshing = Promise.resolve().then(async () => {
      let response: Response;
      try {
        response = await boundedFetch(`${options.base}/auth/refresh`, {
          method: 'POST', credentials: 'same-origin',
          headers: { 'X-Karakoram-CSRF': token, Accept: 'application/json' },
        });
      } catch {
        throw new SessionUnavailableError();
      }
      if (response.status === 401 || response.status === 403) return false;
      if (!response.ok) throw new SessionUnavailableError();
      try {
        const session = await response.json() as { csrfToken: string };
        options.csrf.set(session.csrfToken);
      } catch {
        throw new SessionUnavailableError();
      }
      lastSuccess = now();
      return true;
    }).finally(() => { refreshing = null; });
    return refreshing;
  }

  const fetchWithSession: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    const path = new URL(request.url).pathname;
    const recover = !path.startsWith(`${options.base}/auth/`) || path === `${options.base}/auth/me`;
    const started = now();
    const replay = recover ? request.clone() : null;
    const response = await boundedFetch(request);
    if (response.status !== 401 || !recover) return response;
    // Requests already in flight when rotation completed may retry the new cookie
    // without rotating again. A new 401 immediately after rotation ends recovery.
    const alreadyRefreshed = !rejected && lastSuccess >= started;
    if (!alreadyRefreshed && !await refresh()) {
      reject();
      return response;
    }
    const token = options.csrf.get();
    if (token && replay && !['GET', 'HEAD', 'OPTIONS'].includes(replay.method)) replay.headers.set('X-Karakoram-CSRF', token);
    const retry = await boundedFetch(replay!);
    if (retry.status === 401) reject();
    return retry;
  };

  return {
    fetch: fetchWithSession,
    reset() { rejected = false; lastSuccess = -Infinity; },
  };
}
