// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

register('../../test/tsx-loader.mjs', import.meta.url);
// Static source imports would run before the TSX/alias loader is registered.
const { AppShell } = await import('./AppShell.tsx');
const { AuthProvider } = await import('../auth/session.tsx');
const { ClustersListPage } = await import('../pages/clusters/ClustersList.tsx');
const { keys } = await import('../api/keys.ts');

function renderClusterShell() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(keys.clusterList('default'), { items: [] });
  const router = createMemoryRouter([{
    element: createElement(AppShell),
    children: [{ path: '/clusters', element: createElement(ClustersListPage) }],
  }], { initialEntries: ['/clusters?namespace=default'] });
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client },
      createElement(AuthProvider, null, createElement(RouterProvider, { router }))));
  } finally {
    router.dispose();
    client.clear();
  }
}

test('the cluster filter uses a wrapping card header and bounded input instead of a fixed inline width', () => {
  const markup = renderClusterShell();
  assert.match(markup, /<section class="card cluster-list"><header class="card-head">/);
  const filter = markup.match(/<input[^>]*aria-label="Filter clusters"[^>]*>/)?.[0];
  assert.ok(filter);
  assert.match(filter, /class="input cluster-list-filter"/);
  assert.doesNotMatch(filter, /style=/);

  // SSR cannot measure viewport overflow; protect the sizing contract here.
  // The live mobile smoke checks the resulting 390px document width.
  const css = readFileSync(new URL('../styles/components.css', import.meta.url), 'utf8');
  assert.match(css, /\.cluster-list \.card-head\s*\{\s*flex-wrap: wrap;/);
  assert.match(css, /\.cluster-list \.card-head > \.row\s*\{\s*min-width: 0;\s*max-width: 100%;/);
  assert.match(css, /\.cluster-list-filter\s*\{\s*width: 240px;\s*min-width: 0;\s*max-width: 100%;/);
});

test('change password keeps its icon and accessible name when CSS collapses the rail without React state', () => {
  const markup = renderClusterShell();
  assert.match(markup, /class="shell "/);
  assert.match(markup, /<a[^>]*aria-label="Change password"[^>]*title="Change password"[^>]*href="\/change-password"[^>]*><svg[^>]*aria-hidden="true"[^>]*>.*?<\/svg><span>Change password<\/span><\/a>/);

  const css = readFileSync(new URL('../styles/layout.css', import.meta.url), 'utf8');
  assert.match(css, /\.collapsed \.nav-link span,[^{]*\{\s*display: none;/);
  assert.match(css, /@media \(max-width: 1100px\)\s*\{[\s\S]*?\.nav-link span,[^{]*\{\s*display: none;/);
});
