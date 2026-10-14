// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement } from 'react';
import type { ReactElement } from 'react';
import { renderToString } from 'react-dom/server';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';

register('../test/tsx-loader.mjs', import.meta.url);
// Register the TSX loader before loading source modules; static imports run first.

const { appRoutes } = await import('./App.tsx');
const { OperationNotice } = await import('./components/OperationNotice.tsx');
const { ToastContext, ToastProvider, ToastViewport, useToast } = await import('./components/ui/Toast.tsx');
const { AppErrorBoundary, AppErrorFallback } = await import('./components/AppErrorBoundary.tsx');

const unexpectedToast = () => { assert.fail('Rendering must not enqueue or dismiss a toast'); };

const toastState = {
  items: [{ id: 1, tone: 'ok' as const, message: createElement(OperationNotice, { operationId: 'op/a ?#', children: 'Restart accepted.' }) }],
  toast: unexpectedToast,
  success: unexpectedToast,
  error: unexpectedToast,
  dismiss: unexpectedToast,
};

test('the real app root renders operation toast links inside its router', async () => {
  const root = appRoutes[0];
  assert.ok(root);
  assert.ok(root.index !== true);
  const router = createMemoryRouter([{
    ...root,
    children: [
      { path: '/clusters', element: createElement('h1', null, 'Cluster page') },
      { path: '/operations/:id', element: createElement('h1', null, 'Operation details') },
    ],
  }], { initialEntries: ['/clusters'] });
  try {
    const shell = createElement(ToastContext.Provider, { value: toastState }, createElement(RouterProvider, { router }));
    const markup = renderToString(shell);
    assert.match(markup, /<h1>Cluster page<\/h1>/);
    assert.match(markup, /class="toast toast-ok" role="status"/);
    assert.match(markup, /href="\/operations\/op%2Fa%20%3F%23"[^>]*>View operation<\/a>/);
    assert.doesNotMatch(markup, /The page could not be displayed/);

    await router.navigate('/operations/op%2Fa%20%3F%23');
    assert.equal(router.state.location.pathname, '/operations/op%2Fa%20%3F%23');
    assert.match(renderToString(shell), /<h1>Operation details<\/h1>/);
  } finally {
    router.dispose();
  }
});

test('operation links require a router, while non-route consumers keep toast access', () => {
  assert.throws(() => renderToString(createElement(ToastContext.Provider, { value: toastState }, createElement(ToastViewport))));

  function NonRouteConsumer() {
    const api = useToast();
    assert.equal(typeof api.success, 'function');
    assert.equal(typeof api.error, 'function');
    return createElement('span', null, 'Toast access available');
  }
  const markup = renderToString(createElement(ToastProvider, null, createElement(NonRouteConsumer)));
  assert.match(markup, /Toast access available/);
  assert.doesNotMatch(markup, /class="toasts"/);
});

test('the error boundary renders a plain recoverable fallback after a child fails', () => {
  function BrokenChild(): never {
    throw new Error('private diagnostic detail');
  }
  const boundary = new AppErrorBoundary({ children: createElement(BrokenChild) });
  // Server rendering propagates errors rather than invoking error boundaries.
  // Exercise the same state transition React uses after catching a client error.
  assert.throws(() => renderToString(boundary.render()), /private diagnostic detail/);
  boundary.state = AppErrorBoundary.getDerivedStateFromError();
  const markup = renderToString(boundary.render());
  assert.match(markup, /role="alert"/);
  assert.match(markup, /The page could not be displayed/);
  assert.match(markup, /Reload page<\/button>/);
  assert.doesNotMatch(markup, /private diagnostic detail/);
  assert.equal((appRoutes[0]?.errorElement as ReactElement).type, AppErrorFallback);
});
