// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link, NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useAuth } from '@/auth/session';
import { useBackgroundConnectionLost, useLiveUpdates } from '@/api/events';
import { useOrphanedVolumes, useVersion } from '@/api/hooks';
import { useNamespace } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { Logo } from '@/components/Logo';
import { Callout } from '@/components/ui/Primitives';
import { NamespaceSwitcher } from './NamespaceSwitcher';
import { formatBuildVersion, scopedPath } from '@/components/ui/presentation';

function Icon({ d }: { d: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d={d} />
    </svg>
  );
}

const ICONS = {
  clusters: 'M4 6h16M4 12h16M4 18h16M8 6v0M8 12v0M8 18v0',
  events: 'M12 8v4l3 3M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z',
  operations: 'M3 12h4l3-8 4 16 3-8h4',
  settings: 'M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z',
  logout: 'M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9',
};

export function AppShell() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [collapsed, setCollapsed] = useState(false);
  const [live, setLive] = useState(false);
  const version = useVersion();
  const { namespace } = useNamespace();
  const stale = useBackgroundConnectionLost();
  const scoped = (path: string) => scopedPath(path, namespace);
  const orphanedVolumes = useOrphanedVolumes(auth.status === 'authenticated' && auth.can('clusters', 'read'), namespace);
  const orphanedCount = orphanedVolumes.error ? 0 : (orphanedVolumes.data?.items.length ?? 0);
  useLiveUpdates(auth.status === 'authenticated', undefined, setLive);

  const initials = (auth.user?.name ?? '?').slice(0, 2);

  return (
    <div className={['shell', collapsed ? 'collapsed' : ''].join(' ')}>
      <aside className="sidebar">
        <NavLink to={scoped('/clusters')} className="brand" aria-label="Karakoram home">
          <Logo size={26} />
          <span className="brand-text">Karakoram</span>
        </NavLink>
        <NamespaceSwitcher collapsed={collapsed} />
        <nav className="nav" aria-label="Primary">
          <NavLink to={scoped('/clusters')} className={({ isActive }) => ['nav-link', isActive ? 'active' : ''].join(' ')} title="Clusters">
            <Icon d={ICONS.clusters} />
            <span>Clusters</span>
          </NavLink>
          {auth.can('events', 'read') || auth.can('audit', 'read') ? (
            <NavLink to={scoped('/events')} className={({ isActive }) => ['nav-link', isActive ? 'active' : ''].join(' ')} title="Events">
              <Icon d={ICONS.events} />
              <span>Events</span>
            </NavLink>
          ) : null}
          {auth.can('operations', 'read') ? (
            <NavLink to={scoped('/operations')} className={({ isActive }) => ['nav-link', isActive ? 'active' : ''].join(' ')} title="Operations">
              <Icon d={ICONS.operations} />
              <span>Operations</span>
            </NavLink>
          ) : null}
          <div className="nav-section">Administration</div>
          <NavLink to={scoped('/settings/backup-storages')} className={({ isActive }) => ['nav-link', isActive ? 'active' : ''].join(' ')} title="Settings">
            <Icon d={ICONS.settings} />
            <span>Settings</span>
          </NavLink>
          <a href="/api/v1/openapi.yaml" className="nav-link" title="API specification"><span>API specification</span></a>
          <a href="https://karakoram.dev/docs/api/" className="nav-link" target="_blank" rel="noreferrer"><span>API guide ↗</span></a>
        </nav>
        <div className="sidebar-foot">
          <div className="user-chip" title={auth.user?.subject}>
            <span className="avatar" aria-hidden>
              {initials}
            </span>
            <div className="grow truncate">
              <div className="truncate" style={{ color: '#fff', fontWeight: 600 }}>
                {auth.user?.name}
              </div>
              <div className="xs" role="status" style={{ color: live && !stale ? '#7fd1a6' : 'var(--ridge-300)' }}>
                {stale ? 'connection lost — data may be stale' : live ? '● live' : '○ reconnecting — data may be stale'}
                {version.data ? ` · ${formatBuildVersion(version.data.version)}` : ''}
              </div>
            </div>
          </div>
          <Link to="/change-password" className="nav-link" title="Change password">{collapsed ? 'Password' : 'Change password'}</Link>
          <div className="row" style={{ justifyContent: collapsed ? 'center' : 'space-between' }}>
            <Button
              variant="ghost"
              size="sm"
              icon
              aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
              aria-pressed={collapsed}
              onClick={() => setCollapsed((c) => !c)}
            >
              {collapsed ? '»' : '«'}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              icon={collapsed}
              aria-label="Sign out"
              onClick={async () => {
                await auth.logout();
                navigate('/login');
              }}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
                <path d={ICONS.logout} />
              </svg>
              {collapsed ? null : <span>Sign out</span>}
            </Button>
          </div>
        </div>
      </aside>
      <main className="main" id="main">
        {orphanedCount > 0 ? (
          <div className="orphaned-volume-banner">
            <Callout tone="warn">
              <Link to={namespace ? `/settings/orphaned-volumes?namespace=${encodeURIComponent(namespace)}` : '/settings/orphaned-volumes'}>
                {orphanedCount} orphaned platform {orphanedCount === 1 ? 'volume' : 'volumes'} awaiting deletion
              </Link>
            </Callout>
          </div>
        ) : null}
        <Outlet />
      </main>
    </div>
  );
}
