// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { createSessionTransport, MIN_REFRESH_INTERVAL_MS, SessionUnavailableError } from './session-transport.ts';

const origin = 'https://karakoram.example';
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } });
const pathOf = (input: RequestInfo | URL) => new URL(input instanceof Request ? input.url : String(input), origin).pathname;

function setup(fetcher: typeof fetch, timeoutMs?: number) {
  let token = 'csrf-before';
  let signedOut = 0;
  let now = 1_000;
  const transport = createSessionTransport({
    base: '/api/v1', fetch: fetcher, timeoutMs,
    csrf: { get: () => token, set: (value) => { token = value; } },
    unauthorized: () => { signedOut++; }, now: () => now,
  });
  return { transport, token: () => token, signedOut: () => signedOut, advance: (ms: number) => { now += ms; } };
}

test('reload without access cookie restores identity with refresh and new CSRF', async () => {
  let restored = false;
  let refreshes = 0;
  const s = setup(async (input, init) => {
    if (pathOf(input) === '/api/v1/auth/refresh') {
      refreshes++;
      assert.equal(new Headers(init?.headers).get('X-Karakoram-CSRF'), 'csrf-before');
      restored = true;
      return json({ csrfToken: 'csrf-after' });
    }
    return restored ? json({ name: 'reader' }) : json({}, 401);
  });
  const response = await s.transport.fetch(`${origin}/api/v1/auth/me`);
  assert.equal(response.status, 200);
  assert.equal(refreshes, 1);
  assert.equal(s.token(), 'csrf-after');
  assert.equal(s.signedOut(), 0);
});

test('concurrent stale requests use one refresh', async () => {
  let restored = false;
  let refreshes = 0;
  const refreshStarted = Promise.withResolvers<void>();
  const refreshFinished = Promise.withResolvers<void>();
  const s = setup(async (input) => {
    if (pathOf(input) === '/api/v1/auth/refresh') {
      refreshes++;
      refreshStarted.resolve();
      await refreshFinished.promise;
      restored = true;
      return json({ csrfToken: 'csrf-after' });
    }
    return json({}, restored ? 200 : 401);
  });
  const pending = Promise.all(Array.from({ length: 50 }, () => s.transport.fetch(`${origin}/api/v1/events`)));
  await refreshStarted.promise;
  refreshFinished.resolve();
  const responses = await pending;
  assert.ok(responses.every((response) => response.ok));
  assert.equal(refreshes, 1);
  assert.equal(s.signedOut(), 0);
});

test('persistent 401 after successful rotation ends recovery instead of a refresh storm', async () => {
  let refreshes = 0;
  const s = setup(async (input) => {
    if (pathOf(input) === '/api/v1/auth/refresh') {
      refreshes++;
      return json({ csrfToken: 'csrf-after' });
    }
    return json({}, 401);
  });
  for (let index = 0; index < 149; index++) {
    s.advance(50);
    assert.equal((await s.transport.fetch(`${origin}/api/v1/events`)).status, 401);
  }
  assert.equal(refreshes, 1);
  assert.ok(s.signedOut() > 0);
});

test('new 401 right after a successful refreshed read does not rotate again', async () => {
  let refreshes = 0;
  let reads = 0;
  const s = setup(async (input) => {
    if (pathOf(input) === '/api/v1/auth/refresh') { refreshes++; return json({ csrfToken: 'csrf-after' }); }
    reads++;
    return json({}, reads === 2 ? 200 : 401);
  });
  assert.equal((await s.transport.fetch(`${origin}/api/v1/events`)).status, 200);
  s.advance(1);
  assert.equal((await s.transport.fetch(`${origin}/api/v1/events`)).status, 401);
  assert.equal(refreshes, 1);
  assert.ok(s.signedOut() > 0);
  s.transport.reset();
  s.advance(MIN_REFRESH_INTERVAL_MS);
  await s.transport.fetch(`${origin}/api/v1/events`);
  assert.equal(refreshes, 2, 'explicit login resets rejection guard');
});

test('refresh 502 and network failures preserve the session and allow retry', async () => {
  for (const network of [false, true]) {
    let available = false;
    let restored = false;
    const s = setup(async (input) => {
      if (pathOf(input) === '/api/v1/auth/refresh') {
        if (!available) {
          if (network) throw new TypeError('fetch failed');
          return json({}, 502);
        }
        restored = true;
        return json({ csrfToken: 'csrf-after' });
      }
      return json({}, restored ? 200 : 401);
    });
    await assert.rejects(s.transport.fetch(`${origin}/api/v1/auth/me`), SessionUnavailableError);
    assert.equal(s.signedOut(), 0);
    available = true;
    assert.equal((await s.transport.fetch(`${origin}/api/v1/auth/me`)).status, 200);
    assert.equal(s.signedOut(), 0);
  }
});

test('/auth/me 502 never dispatches sign-out; login 401 does not try refresh', async () => {
  let calls = 0;
  const s = setup(async (input) => {
    calls++;
    return json({}, pathOf(input) === '/api/v1/auth/me' ? 502 : 401);
  });
  assert.equal((await s.transport.fetch(`${origin}/api/v1/auth/me`)).status, 502);
  assert.equal((await s.transport.fetch(`${origin}/api/v1/auth/login`, { method: 'POST' })).status, 401);
  assert.equal(calls, 2);
  assert.equal(s.signedOut(), 0);
});

test('ordinary requests are time bounded and caller cancellation is preserved', async (t) => {
  let timeout = new AbortController();
  t.mock.method(AbortSignal, 'timeout', (milliseconds: number) => {
    assert.equal(milliseconds, 10);
    return timeout.signal;
  });
  const s = setup((_input, init) => {
    const pending = Promise.withResolvers<Response>();
    init!.signal!.addEventListener('abort', () => pending.reject(init!.signal!.reason), { once: true });
    return pending.promise;
  }, 10);
  const timed = s.transport.fetch(`${origin}/api/v1/auth/me`);
  timeout.abort(new DOMException('Request timed out', 'TimeoutError'));
  await assert.rejects(timed, { name: 'TimeoutError' });
  timeout = new AbortController();
  const controller = new AbortController();
  const pending = s.transport.fetch(`${origin}/api/v1/events`, { signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(s.signedOut(), 0);
});

test('unsafe replay retains body and replaces CSRF header after refresh', async () => {
  let reads = 0;
  const s = setup(async (input) => {
    if (pathOf(input) === '/api/v1/auth/refresh') return json({ csrfToken: 'csrf-after' });
    const request = input as Request;
    assert.equal(await request.text(), '{"name":"automation"}');
    reads++;
    if (reads === 2) assert.equal(request.headers.get('X-Karakoram-CSRF'), 'csrf-after');
    return json({}, reads === 1 ? 401 : 201);
  });
  assert.equal((await s.transport.fetch(`${origin}/api/v1/api-tokens`, {
    method: 'POST', headers: { 'X-Karakoram-CSRF': 'csrf-before' }, body: '{"name":"automation"}',
  })).status, 201);
});
