// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useNamespaces, type Namespace } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import { rememberNamespace, useNamespace } from '@/namespaces/useNamespace';

type Health = 'ok' | 'warn' | 'danger';

/** One dot summarises token, relay and phase; details live under Settings → Namespaces. */
export function namespaceHealth(ns: Namespace): { health: Health; label: string } {
  if (ns.phase === 'Error' || !ns.token.valid) return { health: 'danger', label: ns.token.valid ? ns.relay.lastError || 'Error' : ns.token.lastError || 'Token invalid' };
  if (ns.phase === 'Deleting') return { health: 'warn', label: 'Deleting' };
  if (ns.phase === 'Pending') return { health: 'warn', label: 'Pending' };
  if (!ns.default && ns.relay.phase !== 'Ready') return { health: 'warn', label: `Relay ${ns.relay.phase.toLowerCase()}` };
  if (!ns.default && !ns.relay.connected) return { health: 'warn', label: 'Relay disconnected' };
  return { health: 'ok', label: 'Ready' };
}

function Dot({ health, label }: { health: Health | 'all'; label: string }) {
  return <span className={`ns-dot ns-dot-${health}`} title={label} aria-label={label} role="img" />;
}

export function NamespaceSwitcher({ collapsed }: { collapsed: boolean }) {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const { namespace, fromUrl, setNamespace } = useNamespace();
  const namespaces = useNamespaces(auth.status === 'authenticated' && auth.can('namespaces', 'read'));
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // A namespace arriving through a shared link becomes the sticky choice.
  useEffect(() => {
    if (fromUrl) rememberNamespace(namespace);
  }, [fromUrl, namespace]);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (!ref.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDoc);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  const items = namespaces.data?.items ?? [];

  const current = items.find((n) => n.name === namespace);
  const currentHealth = current ? namespaceHealth(current) : null;

  const choose = (next: string | undefined) => {
    setOpen(false);
    // Cluster detail pages are bound to one namespace; switching leaves them
    // for the namespace-scoped cluster list.
    const onDetail = /^\/clusters\/[^/]+\/[^/]+/.test(location.pathname) && !location.pathname.startsWith('/clusters/new');
    if (onDetail) {
      rememberNamespace(next);
      navigate(`/clusters?${new URLSearchParams({ namespace: next ?? '' })}`);
      return;
    }
    setNamespace(next);
  };

  return (
    <div className="ns-switch" ref={ref}>
      <button type="button" className="ns-switch-btn" aria-haspopup="listbox" aria-expanded={open} aria-label={`Namespace: ${namespace ?? 'all namespaces'}`} title={namespace ?? 'All namespaces'} onClick={() => setOpen((o) => !o)}>
        <Dot health={currentHealth?.health ?? (namespace ? 'warn' : 'all')} label={currentHealth?.label ?? (namespace ? 'Not in your namespace list' : 'All namespaces')} />
        {collapsed ? null : (
          <>
            <span className="ns-switch-text">
              <span className="ns-switch-label">Namespace</span>
              <span className="ns-switch-value">{namespace ?? 'All namespaces'}</span>
            </span>
            <span className="ns-switch-caret" aria-hidden>
              ▾
            </span>
          </>
        )}
      </button>
      {open ? (
        <div className="ns-menu" role="listbox" aria-label="Namespaces">
          <button type="button" role="option" aria-selected={!namespace} onClick={() => choose(undefined)}>
            <Dot health="all" label="All namespaces" />
            <span>All namespaces</span>
          </button>
          {items.map((n) => {
            const h = namespaceHealth(n);
            return (
              <button key={n.name} type="button" role="option" aria-selected={n.name === namespace} onClick={() => choose(n.name)}>
                <Dot health={h.health} label={h.label} />
                <span className="mono">{n.name}</span>
                {n.default ? <span className="ns-menu-tag">default</span> : null}
              </button>
            );
          })}
          {namespace && !current ? (
            <button type="button" role="option" aria-selected onClick={() => choose(namespace)}>
              <Dot health="warn" label="Not in your namespace list" />
              <span className="mono">{namespace}</span>
            </button>
          ) : null}
          {namespaces.error ? <div className="ns-menu-note">Namespace list unavailable.</div> : null}
        </div>
      ) : null}
    </div>
  );
}
