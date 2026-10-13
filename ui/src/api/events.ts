// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { API_BASE, type Schemas } from './client';
import { keys } from './keys';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';

export type KEvent = Schemas['Event'];

export interface SSEFrame {
  id?: string;
  event?: string;
  data: string;
}

/**
 * Minimal SSE client over fetch. Unlike EventSource it delivers every named
 * `event:` type to one handler, sends Last-Event-ID on reconnect and shares the
 * cookie session. Returns a stop function.
 */
export function subscribeSSE(url: string, onFrame: (f: SSEFrame) => void, onState?: (connected: boolean) => void): () => void {
  let stopped = false;
  let lastId: string | undefined;
  let attempt = 0;
  let controller: AbortController | null = null;

  const run = async () => {
    while (!stopped) {
      controller = new AbortController();
      try {
        const headers: Record<string, string> = { Accept: 'text/event-stream' };
        if (lastId) headers['Last-Event-ID'] = lastId;
        const res = await fetch(url, { headers, credentials: 'same-origin', signal: controller.signal, cache: 'no-store' });
        if (!res.ok || !res.body) throw new Error(`stream ${res.status}`);
        attempt = 0;
        onState?.(true);
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        let buf = '';
        let frame: SSEFrame = { data: '' };
        let hasData = false;
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += value;
          let nl: number;
          while ((nl = buf.indexOf('\n')) >= 0) {
            const line = buf.slice(0, nl).replace(/\r$/, '');
            buf = buf.slice(nl + 1);
            if (line === '') {
              if (hasData) {
                if (frame.id) lastId = frame.id;
                onFrame({ ...frame, data: frame.data.replace(/\n$/, '') });
              }
              frame = { data: '' };
              hasData = false;
              continue;
            }
            if (line.startsWith(':')) continue; // heartbeat comment
            const colon = line.indexOf(':');
            const field = colon < 0 ? line : line.slice(0, colon);
            const val = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
            if (field === 'data') {
              frame.data += val + '\n';
              hasData = true;
            } else if (field === 'event') frame.event = val;
            else if (field === 'id') frame.id = val;
          }
        }
      } catch {
        // fall through to reconnect
      }
      onState?.(false);
      if (stopped) break;
      attempt++;
      const backoff = Promise.withResolvers<void>();
      setTimeout(backoff.resolve, Math.min(30000, 1000 * 2 ** Math.min(attempt, 5)));
      await backoff.promise;
    }
  };
  void run();

  return () => {
    stopped = true;
    controller?.abort();
  };
}

/**
 * Keeps TanStack Query caches fresh from the control-plane event stream. Any
 * event carrying a cluster name invalidates that cluster's queries and the
 * cluster list; namespace lifecycle events (namespace set, no cluster) refresh
 * the namespace views; global events invalidate the global collections.
 */
export function useLiveUpdates(enabled: boolean, onEvent?: (e: KEvent) => void, onState?: (connected: boolean) => void) {
  const queryClient = useQueryClient();
  useEffect(() => {
    if (!enabled) return;
    const stop = subscribeSSE(`${API_BASE}/events/stream`, (frame) => {
      let ev: KEvent;
      try {
        ev = JSON.parse(frame.data) as KEvent;
      } catch {
        return;
      }
      onEvent?.(ev);
      void queryClient.invalidateQueries({ queryKey: keys.events });
      void queryClient.invalidateQueries({ queryKey: keys.operations });
      const namespace = ev.namespace || DEFAULT_NAMESPACE;
      if (ev.cluster) {
        void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, ev.cluster) });
        void queryClient.invalidateQueries({ queryKey: keys.clusters });
      } else if (ev.namespace) {
        void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      }
      const t = ev.type.toLowerCase();
      const reason = (ev.reason ?? '').toLowerCase();
      if (t.includes('storage')) void queryClient.invalidateQueries({ queryKey: keys.backupStorages });
      if (t.includes('namespace') || t.includes('relay') || reason.includes('namespace') || reason.includes('relay')) void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      if (reason.includes('volumeorphaned') || reason.includes('orphanedvolume')) void queryClient.invalidateQueries({ queryKey: keys.orphanedVolumes });
      if (t.includes('account')) void queryClient.invalidateQueries({ queryKey: keys.accounts });
      if (t.includes('token')) void queryClient.invalidateQueries({ queryKey: keys.apiTokens });
      if (t.includes('rbac') || t.includes('policy')) void queryClient.invalidateQueries({ queryKey: keys.rbacPolicy });
      if (t.includes('setting')) void queryClient.invalidateQueries({ queryKey: keys.settings });
    }, onState);
    return stop;
  }, [enabled, queryClient, onEvent, onState]);
}
