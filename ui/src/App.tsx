// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Navigate, RouterProvider, createBrowserRouter } from 'react-router-dom';
import { ProtectedRoute } from '@/auth/ProtectedRoute';
import { AppShell } from '@/layout/AppShell';
import { LoginPage } from '@/pages/auth/Login';
import { ChangePasswordPage } from '@/pages/auth/ChangePassword';
import { ClustersListPage } from '@/pages/clusters/ClustersList';
import { ClusterWizardPage } from '@/pages/clusters/ClusterWizard';
import { ClusterDetailLayout, LegacyClusterRedirect } from '@/pages/clusters/detail/ClusterDetail';
import { OverviewTab } from '@/pages/clusters/detail/OverviewTab';
import { TopologyTab } from '@/pages/clusters/detail/TopologyTab';
import { UpgradesTab } from '@/pages/clusters/detail/UpgradesTab';
import { BackupsTab } from '@/pages/clusters/detail/BackupsTab';
import { RestoresTab } from '@/pages/clusters/detail/RestoresTab';
import { UsersTab } from '@/pages/clusters/detail/UsersTab';
import { MonitoringTab } from '@/pages/clusters/detail/MonitoringTab';
import { EventsTab } from '@/pages/clusters/detail/EventsTab';
import { OperationsTab } from '@/pages/clusters/detail/OperationsTab';
import { DeleteTab } from '@/pages/clusters/detail/DeleteTab';
import { EventsPage, NotFoundPage, OperationsPage } from '@/pages/GlobalPages';
import { SettingsLayout } from '@/pages/settings/SettingsLayout';
import { BackupStoragesPage } from '@/pages/settings/BackupStorages';
import { OrphanedVolumesPage } from '@/pages/settings/OrphanedVolumes';
import { NamespacesPage } from '@/pages/settings/Namespaces';
import { AccountsPage } from '@/pages/settings/Accounts';
import { RbacPage } from '@/pages/settings/Rbac';
import { ApiTokensPage } from '@/pages/settings/ApiTokens';
import { SsoPage } from '@/pages/settings/Sso';
import { MonitoringSettingsPage } from '@/pages/settings/Monitoring';

const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    element: <ProtectedRoute allowPasswordChange />,
    children: [{ path: '/change-password', element: <ChangePasswordPage /> }],
  },
  {
    element: <ProtectedRoute />,
    children: [
      {
        element: <AppShell />,
        children: [
          { index: true, element: <Navigate to="/clusters" replace /> },
          { path: '/clusters', element: <ClustersListPage /> },
          { path: '/clusters/new', element: <ClusterWizardPage /> },
          { path: '/clusters/:name', element: <LegacyClusterRedirect /> },
          {
            path: '/clusters/:namespace/:name',
            element: <ClusterDetailLayout />,
            children: [
              { index: true, element: <OverviewTab /> },
              { path: 'topology', element: <TopologyTab /> },
              { path: 'upgrades', element: <UpgradesTab /> },
              { path: 'backups', element: <BackupsTab /> },
              { path: 'restores', element: <RestoresTab /> },
              { path: 'users', element: <UsersTab /> },
              { path: 'monitoring', element: <MonitoringTab /> },
              { path: 'events', element: <EventsTab /> },
              { path: 'operations', element: <OperationsTab /> },
              { path: 'delete', element: <DeleteTab /> },
            ],
          },
          { path: '/events', element: <EventsPage /> },
          { path: '/operations', element: <OperationsPage /> },
          {
            path: '/settings',
            element: <SettingsLayout />,
            children: [
              { index: true, element: <Navigate to="/settings/backup-storages" replace /> },
              { path: 'backup-storages', element: <BackupStoragesPage /> },
              { path: 'orphaned-volumes', element: <OrphanedVolumesPage /> },
              { path: 'namespaces', element: <NamespacesPage /> },
              { path: 'accounts', element: <AccountsPage /> },
              { path: 'rbac', element: <RbacPage /> },
              { path: 'api-tokens', element: <ApiTokensPage /> },
              { path: 'sso', element: <SsoPage /> },
              { path: 'monitoring', element: <MonitoringSettingsPage /> },
            ],
          },
          { path: '*', element: <NotFoundPage /> },
        ],
      },
    ],
  },
]);

export function App() {
  return <RouterProvider router={router} />;
}
