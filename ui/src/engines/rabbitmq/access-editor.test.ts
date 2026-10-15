// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { AccessEdit } from './AccessEditor';

register('../../../test/tsx-loader.mjs', import.meta.url);
// These tests cross the JSX/alias loader boundary: static imports would execute
// before registration and Node cannot load the editor or its aliased dependencies.
const { AccessEditor } = await import('./AccessEditor.tsx');
const { api } = await import('../../api/client.ts');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');

function editorSubmission(edit: AccessEdit) {
  const client = new QueryClient();
  const defaultMutationOptions = client.defaultMutationOptions.bind(client);
  let submit: (() => Promise<unknown>) | undefined;
  // Capture the real mutation configured by the rendered editor, without a DOM
  // or replacing its hooks. QueryClient applies these options to every observer.
  client.defaultMutationOptions = (options) => {
    if (options?.mutationFn) submit = options.mutationFn as () => Promise<unknown>;
    return defaultMutationOptions(options);
  };
  try {
    renderToStaticMarkup(createElement(QueryClientProvider, { client },
      createElement(ToastProvider, null,
        createElement(AccessEditor, { namespace: 'default', cluster: 'brokers', edit, vhosts: ['/'], users: ['app'], onClose() {}, onSaved() {} }))));
    assert.ok(submit, 'editor did not configure its save mutation');
    return submit;
  } finally {
    client.clear();
  }
}

const edits: { edit: AccessEdit; method: 'POST' | 'PUT'; resource: string }[] = [
  { edit: { kind: 'permission' }, method: 'POST', resource: 'permissions' },
  { edit: { kind: 'vhost', existing: { name: '/', tags: [] } }, method: 'PUT', resource: 'vhosts/{vhost}' },
  { edit: { kind: 'user', existing: { name: 'app', tags: [] } }, method: 'PUT', resource: 'users/{user}' },
  { edit: { kind: 'permission', existing: { vhost: '/', user: 'app', configure: '', write: '', read: '' } }, method: 'PUT', resource: 'permissions/{vhost}/{user}' },
  { edit: { kind: 'topic', existing: { vhost: '/', user: 'app', exchange: 'events', write: '', read: '' } }, method: 'PUT', resource: 'topic-permissions/{vhost}/{user}' },
  { edit: { kind: 'policy', existing: { vhost: '/', name: 'limits', pattern: '.*', applyTo: 'queues', priority: 0, definition: { 'message-ttl': 100 } } }, method: 'PUT', resource: 'policies/{vhost}/{name}' },
  { edit: { kind: 'operatorPolicy', existing: { vhost: '/', name: 'limits', pattern: '.*', applyTo: 'queues', priority: 0, definition: { 'message-ttl': 100 } } }, method: 'PUT', resource: 'operator-policies/{vhost}/{name}' },
];

for (const { edit, method, resource } of edits) {
  test(`Access editor ${method} ${resource} retains its key after a failed request`, async (t) => {
    const calls: { route: unknown; options: { headers?: Record<string, string>; body: unknown } }[] = [];
    t.mock.method(api, method, async (route: unknown, options: { headers?: Record<string, string>; body: unknown }) => {
      calls.push({ route, options });
      if (calls.length === 1) throw new TypeError('Connection lost after the request was sent');
      return { data: {}, response: new Response('{}', { status: 200 }) };
    });
    const submit = editorSubmission(edit);
    await assert.rejects(submit(), /Connection lost/);
    await submit();
    assert.equal(calls.length, 2);
    assert.equal(calls[0]!.route, '/clusters/{cluster}/rabbitmq/' + resource);
    const key = calls[0]!.options.headers?.['Idempotency-Key'];
    assert.ok(key, 'native access mutation omitted Idempotency-Key');
    assert.equal(calls[1]!.options.headers?.['Idempotency-Key'], key);
    assert.deepEqual(calls[1]!.options.body, calls[0]!.options.body);
    await editorSubmission(edit)();
    assert.notEqual(calls[2]!.options.headers?.['Idempotency-Key'], key, 'a new editor reused an earlier submission key');
  });
}
