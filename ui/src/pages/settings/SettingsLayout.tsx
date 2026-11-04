// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { NavLink, Outlet } from 'react-router-dom';
import { useAuth } from '@/auth/session';
import { PageHeader } from '@/components/ui/Primitives';
import { useNamespace } from '@/namespaces/useNamespace';
import { scopedPath } from '@/components/ui/presentation';
import { useOAuthGrants } from './ConnectedApps';

export function SettingsLayout() {
  const auth = useAuth();
  const grants = useOAuthGrants();
  const { namespace } = useNamespace();
  const items = [
    { to: '/settings/namespaces', label: 'Namespaces', show: auth.can('namespaces', 'read') },
    { to: '/settings/backup-storages', label: 'Backup storages', show: auth.can('backup-storages', 'read') },
    { to: '/settings/orphaned-volumes', label: 'Orphaned volumes', show: auth.can('clusters', 'read') },
    { to: '/settings/accounts', label: 'Accounts', show: auth.can('accounts', 'read') },
    { to: '/settings/rbac', label: 'Roles & permissions', show: auth.can('rbac', 'read') },
    { to: '/settings/api-tokens', label: 'API tokens', show: true },
    { to: '/settings/connected-apps', label: 'Connected apps', show: grants.data?.enabled === true },
    { to: '/settings/sso', label: 'Single sign-on', show: auth.can('settings', 'read') },
    { to: '/settings/notifications', label: 'Notifications', show: auth.can('settings', 'read') },
    { to: '/settings/control-plane-backup', label: 'Control-plane backup', show: auth.can('settings', 'read') },
    { to: '/settings/agent-rollout', label: 'Agent rollout', show: auth.can('settings', 'read') },
    { to: '/settings/monitoring', label: 'Monitoring', show: true },
  ].filter((i) => i.show);
  return (
    <div className="page">
      <PageHeader title="Settings" description="Control-plane configuration for this Karakoram installation." />
      <div className="settings-layout">
        <nav className="settings-nav" aria-label="Settings sections">
          {items.map((i) => (
            <NavLink key={i.to} to={scopedPath(i.to, namespace)} className={({ isActive }) => (isActive ? 'active' : '')}>
              {i.label}
            </NavLink>
          ))}
        </nav>
        <div className="stack" style={{ gap: 16 }}>
          <Outlet />
        </div>
      </div>
    </div>
  );
}
