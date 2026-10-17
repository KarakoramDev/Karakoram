// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client.ts';

export function validateFunction(name: string, files: Schemas['SupabaseFunction']): string[] {
  const errors: string[] = [];
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(name)) errors.push('Function name must be 1–63 lowercase letters, digits, underscores or hyphens, starting with a letter or digit.');
  if (name === 'main') errors.push('The function name main is reserved for the runtime dispatcher.');
  if (!files['index.ts']) errors.push('Add a non-empty index.ts entry point.');
  let bytes = 0;
  for (const [path, source] of Object.entries(files)) {
    if (!path || path.startsWith('/') || path.includes('\\') || path.split('/').some((part) => !part || part === '.' || part === '..')) errors.push('Source paths must be relative and cannot contain parent components.');
    bytes += new TextEncoder().encode(source).length;
  }
  if (bytes > 1024 * 1024) errors.push('Function sources must not exceed 1 MiB.');
  return errors;
}
