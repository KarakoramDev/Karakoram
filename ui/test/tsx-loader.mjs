// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { statSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const sourceRoot = new URL('../src/', import.meta.url);

// Node's type stripping does not support JSX or Vite's source aliases.
export async function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith('@/') || specifier.startsWith('.')) {
    const url = specifier.startsWith('@/')
      ? new URL(specifier.slice(2), sourceRoot)
      : new URL(specifier, context.parentURL);
    if (url.href.startsWith(sourceRoot.href)) {
      for (const suffix of ['', '.ts', '.tsx', '/index.ts', '/index.tsx']) {
        const candidate = new URL(url.href + suffix);
        if (statSync(fileURLToPath(candidate), { throwIfNoEntry: false })?.isFile()) {
          return nextResolve(candidate.href, context);
        }
      }
    }
  }
  return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
  if (url.startsWith(sourceRoot.href) && /\.tsx?$/.test(url)) {
    const source = await readFile(new URL(url), 'utf8');
    const result = ts.transpileModule(source, {
      fileName: fileURLToPath(url),
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext, jsx: ts.JsxEmit.ReactJSX },
    });
    return { format: 'module', source: result.outputText, shortCircuit: true };
  }
  return nextLoad(url, context);
}
