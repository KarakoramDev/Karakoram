// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { register } from 'node:module';
import { readFile } from 'node:fs/promises';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

register('../../../test/tsx-loader.mjs', import.meta.url);
// The alias/TSX loader must register before loading the hook's source module.
const { useLocalRequest } = await import('./use-local-request.ts');

for (const outcome of ['success', 'failure']) {
  test(`secret-bearing local submissions never populate mutation cache on ${outcome}`, async () => {
    const client = new QueryClient();
    let submit: (() => Promise<void>) | undefined;
    let extras: { openbaoUnsealShares?: { shares: string[]; threshold: number } } = { openbaoUnsealShares: { shares: ['offline-private-share'], threshold: 1 } };
    let calls = 0;
    function Harness() {
      const request = useLocalRequest({
        request: async (body: typeof extras) => {
          calls++;
          assert.equal(body.openbaoUnsealShares?.shares[0], 'offline-private-share');
          assert.equal(client.getMutationCache().getAll().length, 0);
          if (outcome === 'failure') throw new Error('request failed');
          return { name: 'created' };
        },
        onSuccess: () => {},
        onSettled: () => { extras = {}; },
      });
      submit = () => request.run(extras);
      return null;
    }
    renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(Harness)));
    assert.ok(submit);
    await submit();
    assert.equal(calls, 1);
    assert.deepEqual(extras, {});
    assert.deepEqual(client.getMutationCache().getAll(), []);
    client.clear();
  });
}

test('cluster create and restore use local requests and clear write-only material after completion', async () => {
  for (const [file, clear] of [['ClusterWizard.tsx', 'setRestoreExtras({})'], ['detail/RestoresTab.tsx', 'setExtras({})']]) {
    const source = await readFile(new URL(file!, import.meta.url), 'utf8');
    assert.match(source, /const create = useLocalRequest\(/);
    assert.match(source, /create\.run\(/);
    assert.ok(source.includes(clear!));
    assert.doesNotMatch(source, /useMutation|create\.mutate/);
  }
});
