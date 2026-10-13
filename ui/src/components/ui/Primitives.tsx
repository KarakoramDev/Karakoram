// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useRef, useState, type ReactNode } from 'react';
import { NavLink } from 'react-router-dom';
import { Button } from './Button';

export type Tone = 'neutral' | 'ok' | 'warn' | 'danger' | 'info' | 'progress' | 'accent';

export function Badge({ tone = 'neutral', plain, children, title }: { tone?: Tone; plain?: boolean; children: ReactNode; title?: string }) {
  return (
    <span className={['badge', tone !== 'neutral' ? `badge-${tone}` : '', plain ? 'badge-plain' : ''].join(' ')} title={title}>
      {children}
    </span>
  );
}

export function Card({ title, actions, children, flush, className }: { title?: ReactNode; actions?: ReactNode; children: ReactNode; flush?: boolean; className?: string }) {
  return (
    <section className={['card', className ?? ''].join(' ')}>
      {title || actions ? (
        <header className="card-head">
          <h3 className="card-title">{title}</h3>
          {actions ? <div className="row">{actions}</div> : null}
        </header>
      ) : null}
      <div className={['card-body', flush ? 'card-body-flush' : ''].join(' ')}>{children}</div>
    </section>
  );
}

export function Callout({ tone = 'info', children, title }: { tone?: 'info' | 'warn' | 'danger' | 'ok'; children: ReactNode; title?: ReactNode }) {
  return (
    <div className={`callout callout-${tone}`} role={tone === 'danger' ? 'alert' : undefined}>
      <span className="callout-icon" aria-hidden>
        {tone === 'ok' ? '✓' : tone === 'info' ? 'ⓘ' : '⚠'}
      </span>
      <div className="stack" style={{ gap: 2 }}>
        {title ? <strong>{title}</strong> : null}
        <div>{children}</div>
      </div>
    </div>
  );
}

export function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="loading-block" role="status">
      <span className="spinner" aria-hidden />
      {label}
    </div>
  );
}

export function ErrorBlock({ error, retry }: { error: unknown; retry?: () => void }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <Callout tone="danger" title="Request failed">
      <div className="row wrap">
        <span>{msg}</span>
        {retry ? (
          <Button size="sm" onClick={retry}>
            Retry
          </Button>
        ) : null}
      </div>
    </Callout>
  );
}

export function Empty({ title, children, action }: { title: ReactNode; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="empty">
      <h3>{title}</h3>
      {children ? <p className="small">{children}</p> : null}
      {action ? <div style={{ marginTop: 8 }}>{action}</div> : null}
    </div>
  );
}

export function KV({ items }: { items: Array<[ReactNode, ReactNode] | null | false | undefined> }) {
  return (
    <dl className="kv">
      {items
        .filter((x): x is [ReactNode, ReactNode] => Boolean(x))
        .map(([k, v], i) => (
          <div key={i} style={{ display: 'contents' }}>
            <dt>{k}</dt>
            <dd>{v ?? <span className="faint">—</span>}</dd>
          </div>
        ))}
    </dl>
  );
}

export function PageHeader({ title, description, actions, crumbs }: { title: ReactNode; description?: ReactNode; actions?: ReactNode; crumbs?: ReactNode }) {
  return (
    <div className="page-head">
      <div className="page-head-text">
        {crumbs ? <nav className="crumbs" aria-label="Breadcrumb">{crumbs}</nav> : null}
        <h1>{title}</h1>
        {description ? <p>{description}</p> : null}
      </div>
      {actions ? <div className="page-actions">{actions}</div> : null}
    </div>
  );
}

export function Tabs({ items }: { items: Array<{ to: string; label: ReactNode; end?: boolean }> }) {
  return (
    <nav className="tabs" aria-label="Sections">
      {items.map((t) => (
        <NavLink key={t.to} to={t.to} end={t.end} className={({ isActive }) => ['tab', isActive ? 'active' : ''].join(' ')}>
          {t.label}
        </NavLink>
      ))}
    </nav>
  );
}

export function CopyButton({ value, label = 'Copy' }: { value: string; label?: string }) {
  const [done, setDone] = useState(false);
  return (
    <Button
      size="sm"
      aria-label={`${label} to clipboard`}
      onClick={async () => {
        await navigator.clipboard.writeText(value);
        setDone(true);
        window.setTimeout(() => setDone(false), 1500);
      }}
    >
      {done ? 'Copied' : label}
    </Button>
  );
}

export function CodeLine({ value }: { value: string }) {
  return (
    <div className="copy-row">
      <code className="code">{value}</code>
      <CopyButton value={value} />
    </div>
  );
}

export function Meter({ value, max, label, unit, compact }: { value: number; max?: number; label?: string; unit?: string; compact?: boolean }) {
  const pct = max && max > 0 ? Math.min(100, (value / max) * 100) : undefined;
  const tone = pct === undefined ? '' : pct >= 90 ? 'meter-danger' : pct >= 75 ? 'meter-warn' : '';
  const text = compact && pct !== undefined ? `${Math.round(pct)}%` : `${formatNum(value)}${max ? `/${formatNum(max)}` : ''}${unit ? ` ${unit}` : ''}`;
  return (
    <span className={['meter', 'nowrap', tone].join(' ')} title={label ?? (pct !== undefined ? `${formatNum(value)} of ${formatNum(max ?? 0)} ${unit ?? ''}` : undefined)}>
      {pct !== undefined ? (
        <span className="meter-bar" aria-hidden>
          <span style={{ width: `${pct}%` }} />
        </span>
      ) : null}
      <span className="xs">{text}</span>
    </span>
  );
}

export function formatNum(n: number): string {
  if (!Number.isFinite(n)) return '—';
  if (Math.abs(n) >= 100) return Math.round(n).toLocaleString();
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function formatBytes(bytes: number | undefined | null): string {
  if (bytes === undefined || bytes === null || !Number.isFinite(bytes)) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = bytes;
  let i = 0;
  while (Math.abs(v) >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toLocaleString(undefined, { maximumFractionDigits: i === 0 ? 0 : 1 })} ${units[i]}`;
}

export function formatTime(iso: string | undefined | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

export function formatRelative(iso: string | undefined | null): string {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const diff = Date.now() - t;
  const abs = Math.abs(diff);
  const s = Math.round(abs / 1000);
  const fmt = (n: number, u: string) => `${n}${u}${diff < 0 ? ' from now' : ' ago'}`;
  if (s < 60) return fmt(s, 's');
  const m = Math.round(s / 60);
  if (m < 60) return fmt(m, 'm');
  const h = Math.round(m / 60);
  if (h < 48) return fmt(h, 'h');
  return fmt(Math.round(h / 24), 'd');
}

export function formatDuration(seconds: number | undefined | null): string {
  if (seconds === undefined || seconds === null || !Number.isFinite(seconds)) return '—';
  const s = Math.round(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

export function Time({ iso }: { iso: string | undefined | null }) {
  if (!iso) return <span className="faint">—</span>;
  return (
    <time dateTime={iso} title={formatTime(iso)}>
      {formatRelative(iso)}
    </time>
  );
}

/** Minimal dropdown menu with keyboard dismissal. */
export function Menu({ label, children, size = 'sm' }: { label?: ReactNode; children: ReactNode; size?: 'sm' | 'md' }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
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
  return (
    <div className="menu-anchor" ref={ref}>
      <Button size={size} aria-haspopup="menu" aria-expanded={open} onClick={() => setOpen((o) => !o)} icon={!label} aria-label={label ? undefined : 'More actions'}>
        {label ?? '⋯'}
      </Button>
      {open ? (
        <div className="menu" role="menu" onClick={() => setOpen(false)}>
          {children}
        </div>
      ) : null}
    </div>
  );
}

export function MenuItem({ onClick, children, danger, disabled, title }: { onClick: () => void; children: ReactNode; danger?: boolean; disabled?: boolean; title?: string }) {
  return (
    <button type="button" role="menuitem" className={danger ? 'danger' : undefined} onClick={onClick} disabled={disabled} title={title}>
      {children}
    </button>
  );
}

export function Segmented<T extends string>({ value, options, onChange, label }: { value: T; options: Array<{ value: T; label: ReactNode }>; onChange: (v: T) => void; label: string }) {
  return (
    <div className="pill-group" role="group" aria-label={label}>
      {options.map((o) => (
        <button key={o.value} type="button" aria-pressed={value === o.value} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}
