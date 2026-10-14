// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './schema';
import { createSessionTransport, TEMPORARY_UNAVAILABILITY } from './session-transport';

export type Schemas = components['schemas'];

export const API_BASE = '/api/v1';

const CSRF_KEY = 'karakoram.csrf';
const CSRF_HEADER = 'X-Karakoram-CSRF';
const SAFE_METHODS: Record<string, true> = { GET: true, HEAD: true, OPTIONS: true };

/**
 * The CSRF token is a double-submit secret bound to the HttpOnly session cookie.
 * It carries no authority by itself, so it can live in localStorage to survive
 * reloads and new tabs.
 */
export const csrf = {
  get(): string | null {
    const stored = window.localStorage.getItem(CSRF_KEY);
    if (stored) return stored;
    const cookie = document.cookie.split('; ').find((part) => part.startsWith('karakoram_csrf='));
    return cookie ? decodeURIComponent(cookie.slice('karakoram_csrf='.length)) : null;
  },
  set(token: string) {
    window.localStorage.setItem(CSRF_KEY, token);
  },
  clear() {
    window.localStorage.removeItem(CSRF_KEY);
  },
};

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: unknown;

  constructor(status: number, body: Schemas['Error'] | undefined, fallback: string) {
    super(body?.message ?? fallback);
    this.name = 'ApiError';
    this.status = status;
    this.code = body?.code ?? `http_${status}`;
    this.details = body?.details;
  }
}

/** Fired on window when the API rejects the session and no refresh is possible. */
export const UNAUTHORIZED_EVENT = 'karakoram:unauthorized';

const transport = createSessionTransport({
  base: API_BASE,
  fetch: (input, init) => fetch(input, init),
  csrf,
  unauthorized: () => window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT)),
});

/** Reset recovery after an explicit new login, not after each successful read. */
export function resetSessionRecovery() { transport.reset(); }

const sessionMiddleware: Middleware = {
  onRequest({ request }) {
    if (!SAFE_METHODS[request.method]) {
      const token = csrf.get();
      if (token && !request.headers.has(CSRF_HEADER)) request.headers.set(CSRF_HEADER, token);
    }
    return request;
  },
};

export const api = createClient<paths>({
  baseUrl: API_BASE,
  credentials: 'same-origin',
  headers: { Accept: 'application/json' },
  fetch: transport.fetch,
});
api.use(sessionMiddleware);

interface FetchResult<T> {
  data?: T;
  error?: unknown;
  response: Response;
}

/** Resolve an openapi-fetch result to its data or throw a typed ApiError. */
export async function unwrap<T>(p: Promise<FetchResult<T>>): Promise<T> {
  const { data, error, response } = await p;
  if (error !== undefined || !response.ok) {
    throw new ApiError(response.status, error as Schemas['Error'] | undefined, `${response.status} ${response.statusText}`);
  }
  return data as T;
}

/** Narrow an unknown thrown value to a human-readable message. */
export function errorMessage(err: unknown): string {
  if (err instanceof ApiError) return err.status >= 500 ? `${TEMPORARY_UNAVAILABILITY} ${err.message}` : err.message;
  if (err instanceof TypeError || (err instanceof Error && err.name === 'TimeoutError')) return TEMPORARY_UNAVAILABILITY;
  if (err instanceof Error) return err.message;
  return String(err);
}
