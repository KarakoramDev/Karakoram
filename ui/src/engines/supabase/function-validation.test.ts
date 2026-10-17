// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { validateFunction } from './function-validation.ts';

const source = { 'index.ts': 'Deno.serve(() => new Response("hello"));' };

test('function names match API bounds and reserve main', () => {
  for (const name of ['hello', '0', 'hello_world-1', 'a'.repeat(63)]) assert.deepEqual(validateFunction(name, source), []);
  for (const name of ['', 'Hello', '_hello', '-hello', 'hello/world', 'a'.repeat(64)]) assert.match(validateFunction(name, source).join(' '), /Function name/);
  assert.match(validateFunction('main', source).join(' '), /reserved/);
});

test('deployment requires a non-empty index.ts entry point', () => {
  const missingEntries: Record<string, string>[] = [{}, { 'other.ts': 'source' }, { 'index.ts': '' }];
  for (const files of missingEntries) assert.match(validateFunction('hello', files).join(' '), /non-empty index.ts/);
  assert.deepEqual(validateFunction('hello', source), []);
});

test('function paths stay relative and UTF-8 sources respect the API byte limit', () => {
  for (const path of ['', '/file.ts', '../file.ts', './file.ts', 'a//file.ts', 'a\\file.ts']) assert.match(validateFunction('hello', { ...source, [path]: 'source' }).join(' '), /Source paths/);
  assert.deepEqual(validateFunction('hello', { 'index.ts': 'x'.repeat(1024 * 1024) }), []);
  assert.match(validateFunction('hello', { 'index.ts': 'é'.repeat(512 * 1024 + 1) }).join(' '), /1 MiB/);
});
