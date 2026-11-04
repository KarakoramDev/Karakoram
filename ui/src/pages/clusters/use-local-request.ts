// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useRef, useState } from 'react';

// Secret-bearing requests must not enter QueryClient's persistent mutation cache.
// Keep only pending/error state locally, never request arguments or results.
export function useLocalRequest<T, A extends unknown[]>(options: {
  request: (...args: A) => Promise<T>;
  onSuccess: (value: T) => void;
  onError?: () => void;
  onSettled?: () => void;
}) {
  const mounted = useRef(true);
  const busy = useRef(false);
  const [isPending, setPending] = useState(false);
  const [error, setError] = useState<unknown>(null);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  const run = async (...args: A) => {
    if (busy.current) return;
    busy.current = true;
    setPending(true);
    setError(null);
    try {
      const value = await options.request(...args);
      if (mounted.current) options.onSuccess(value);
    } catch (cause) {
      if (mounted.current) {
        setError(cause);
        options.onError?.();
      }
    } finally {
      busy.current = false;
      if (mounted.current) {
        setPending(false);
        options.onSettled?.();
      }
    }
  };
  return { run, isPending, error, reset: () => setError(null) };
}
