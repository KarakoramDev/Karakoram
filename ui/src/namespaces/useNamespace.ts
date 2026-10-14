// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useCallback } from 'react';
import { useMatch, useSearchParams } from 'react-router-dom';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';

export const NAMESPACE_PARAM = 'namespace';
const PREFERENCE_KEY = 'karakoram.namespace';
const NAME_RE = /^[a-z][a-z0-9-]{0,30}[a-z0-9]$/;

/** Validates a namespace name the same way the API does. */
export function isNamespaceName(value: string): boolean {
  return NAME_RE.test(value);
}

function readPreference(): string | undefined {
  try {
    const v = localStorage.getItem(PREFERENCE_KEY);
    return v && isNamespaceName(v) ? v : undefined;
  } catch {
    return undefined;
  }
}

/** Remembers an explicit choice (or an URL-supplied namespace) for later views. */
export function rememberNamespace(namespace: string | undefined) {
  try {
    if (namespace) localStorage.setItem(PREFERENCE_KEY, namespace);
    else localStorage.removeItem(PREFERENCE_KEY);
  } catch {
    // Storage may be unavailable (private mode); the URL still carries the choice.
  }
}

/**
 * The namespace scope of collection views. `undefined` means "all namespaces
 * I may read", which is also what the API returns without the parameter.
 *
 * The `?namespace=` search parameter is authoritative for the current view;
 * without it the last explicit choice (a non-secret localStorage preference)
 * applies, so the selection survives navigation through links that do not
 * carry the parameter.
 */
export function useNamespace(): { namespace: string | undefined; fromUrl: boolean; setNamespace: (namespace: string | undefined) => void } {
  const [params, setParams] = useSearchParams();
  const detail = useMatch('/clusters/:namespace/:name/*');
  const raw = params.get(NAMESPACE_PARAM);
  const fromUrl = raw !== null || Boolean(detail);
  // Detail routes are bound to their path namespace. Invalid collection scopes
  // are sent to the API for an explicit error, never broadened to all namespaces.
  const namespace = detail?.params.namespace ?? (raw !== null ? raw || undefined : readPreference());

  const setNamespace = useCallback(
    (next: string | undefined) => {
      rememberNamespace(next);
      setParams(
        (prev) => {
          const out = new URLSearchParams(prev);
          if (next) out.set(NAMESPACE_PARAM, next);
          else out.set(NAMESPACE_PARAM, '');
          return out;
        },
        { replace: true },
      );
    },
    [setParams],
  );

  return { namespace, fromUrl, setNamespace };
}

/** Query parameters that scope a request to the selected namespace. */
export function namespaceQuery(namespace: string | undefined): { namespace?: string } {
  return namespace ? { namespace } : {};
}

/**
 * Whether a collection warrants a Namespace column: true once any item lives
 * outside the default namespace, so a single-project installation keeps its
 * pre-namespace tables.
 */
export function spansNamespaces(items: ReadonlyArray<{ namespace?: string }>): boolean {
  return items.some((item) => Boolean(item.namespace) && item.namespace !== DEFAULT_NAMESPACE);
}
