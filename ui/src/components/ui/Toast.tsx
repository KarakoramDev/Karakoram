// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from 'react';

type Tone = 'default' | 'ok' | 'danger';

interface Toast {
  id: number;
  tone: Tone;
  message: ReactNode;
}

interface ToastApi {
  toast: (message: ReactNode, tone?: Tone) => void;
  success: (message: ReactNode) => void;
  error: (message: ReactNode) => void;
}

const ToastContext = createContext<ToastApi | null>(null);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Toast[]>([]);
  const seq = useRef(0);

  const dismiss = useCallback((id: number) => setItems((xs) => xs.filter((t) => t.id !== id)), []);

  const toast = useCallback(
    (message: ReactNode, tone: Tone = 'default') => {
      const id = ++seq.current;
      setItems((xs) => [...xs, { id, tone, message }]);
      window.setTimeout(() => dismiss(id), tone === 'danger' ? 9000 : 5000);
    },
    [dismiss],
  );

  const api = useMemo<ToastApi>(
    () => ({
      toast,
      success: (m) => toast(m, 'ok'),
      error: (m) => toast(m, 'danger'),
    }),
    [toast],
  );

  return (
    <ToastContext.Provider value={api}>
      {children}
      <div className="toasts" aria-live="polite" aria-atomic="false">
        {items.map((t) => (
          <div key={t.id} className={['toast', t.tone !== 'default' ? `toast-${t.tone}` : ''].join(' ')} role={t.tone === 'danger' ? 'alert' : 'status'}>
            <span>{t.message}</span>
            <button type="button" aria-label="Dismiss" onClick={() => dismiss(t.id)}>
              ×
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastApi {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error('useToast must be used within ToastProvider');
  return ctx;
}
