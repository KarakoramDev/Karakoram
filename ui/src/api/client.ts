// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './schema';

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
    return window.localStorage.getItem(CSRF_KEY);
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

// Bodies of unsafe requests are kept as clones so a request can be replayed
// once after a successful session refresh.
const replayable = new WeakMap<Request, Request>();
let refreshing: Promise<boolean> | null = null;

async function refreshSession(): Promise<boolean> {
  if (!refreshing) {
    refreshing = (async () => {
      const token = csrf.get();
      if (!token) return false;
      try {
        const res = await fetch(`${API_BASE}/auth/refresh`, {
          method: 'POST',
          credentials: 'same-origin',
          headers: { [CSRF_HEADER]: token, Accept: 'application/json' },
        });
        if (!res.ok) return false;
        const session = (await res.json()) as Schemas['Session'];
        csrf.set(session.csrfToken);
        return true;
      } catch {
        return false;
      } finally {
        refreshing = null;
      }
    })();
  }
  return refreshing;
}

const sessionMiddleware: Middleware = {
  onRequest({ request }) {
    if (!SAFE_METHODS[request.method]) {
      const token = csrf.get();
      if (token && !request.headers.has(CSRF_HEADER)) request.headers.set(CSRF_HEADER, token);
      replayable.set(request, request.clone());
    }
    return request;
  },
  async onResponse({ request, response }) {
    if (response.status !== 401) return response;
    const path = new URL(request.url).pathname;
    // Auth endpoints answer 401 for their own reasons (bad password, dead refresh).
    if (path.startsWith(`${API_BASE}/auth/`)) return response;
    if (await refreshSession()) {
      const retry = replayable.get(request) ?? request.clone();
      const token = csrf.get();
      if (token && !SAFE_METHODS[retry.method]) retry.headers.set(CSRF_HEADER, token);
      return fetch(retry);
    }
    window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT));
    return response;
  },
};

export const api = createClient<paths>({
  baseUrl: API_BASE,
  credentials: 'same-origin',
  headers: { Accept: 'application/json' },
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
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  return String(err);
}
