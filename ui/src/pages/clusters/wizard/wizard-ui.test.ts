// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement } from 'react';
import type { ReactNode } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '@/api/client';
import type { ClusterContext } from '../detail/ClusterDetail';

register('../../../../test/tsx-loader.mjs', import.meta.url);
// TSX and Vite aliases require installing the test loader before module loading.
const { default: RabbitMQWizardSection } = await import('../../../engines/rabbitmq/WizardSection.tsx');
const { SchedulesEditor } = await import('../editors/SchedulesEditor.tsx');
const { BackupsTab, CreateBackupDialog } = await import('../detail/BackupsTab.tsx');
const { ToastProvider } = await import('../../../components/ui/Toast.tsx');

const spec: Schemas['ClusterSpec'] = {
  provider: 'rabbitmq', version: '4.3.6', topology: { type: 'cluster' },
  components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 1, minRam: 1, maxRam: 1, minDisk: 5, maxDisk: 5 } } },
};
const provider: Schemas['Provider'] = {
  name: 'rabbitmq', displayName: 'RabbitMQ', versions: [{ version: '4.3.6', major: '4.3', default: true, status: 'supported' }],
  upgradePaths: [], topologies: [], presets: [], backupTypes: ['definitions'], capabilities: ['backups'], operations: ['backup'],
};

const backupWarning = /broker definitions \(vhosts, users, permissions, policies, and queues\/exchanges\/bindings metadata\), not messages\. They do not provide point-in-time restore/;
const backupSensitivity = /Exports contain user password hashes\. Treat exports and their object storage as sensitive/;

function renderWithQueries(element: ReactNode, context?: ClusterContext) {
  const client = new QueryClient({ defaultOptions: { queries: { enabled: false, retry: false, gcTime: 0 } } });
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client },
      createElement(ToastProvider, null,
        createElement(MemoryRouter, null, context
          ? createElement(Routes, null, createElement(Route, { element: createElement(Outlet, { context }) }, createElement(Route, { index: true, element })))
          : element))));
  } finally {
    client.clear();
  }
}

test('RabbitMQ wizard points configuration overrides to the Topology step', () => {
  const markup = renderToStaticMarkup(createElement(RabbitMQWizardSection, { provider, spec, onChange: () => {} }));
  assert.match(markup, /rabbitmq.conf overrides under Topology/);
  assert.doesNotMatch(markup, /overrides under Resources/);
});

test('wizard schedule editor discloses definitions-only backup limits only for RabbitMQ', () => {
  for (const name of ['rabbitmq', 'psmdb', 'other']) {
    const markup = renderToStaticMarkup(createElement(SchedulesEditor, {
      provider: name, value: [], onChange: () => {}, storages: [], backupTypes: name === 'rabbitmq' ? ['definitions'] : ['logical'],
    }));
    if (name === 'rabbitmq') {
      assert.match(markup, backupWarning);
      assert.match(markup, backupSensitivity);
      assert.match(markup, /karakoram\.dev\/docs\/rabbitmq\/backups\//);
    } else assert.doesNotMatch(markup, /Definitions backups only|point-in-time restore/);
  }
});

test('backup create dialog discloses definitions-only backup limits only for RabbitMQ', () => {
  for (const name of ['rabbitmq', 'psmdb']) {
    const markup = renderWithQueries(createElement(CreateBackupDialog, {
      provider: name, namespace: 'default', cluster: 'brokers', storages: ['archive'], backupTypes: name === 'rabbitmq' ? ['definitions'] : ['logical'], onClose: () => {},
    }));
    if (name === 'rabbitmq') {
      assert.match(markup, backupWarning);
      assert.match(markup, backupSensitivity);
    } else assert.doesNotMatch(markup, /Definitions backups only|point-in-time restore/);
  }
});

test('detail backup policy discloses definitions-only backup limits only for RabbitMQ', () => {
  for (const name of ['rabbitmq', 'psmdb']) {
    const context: ClusterContext = {
      cluster: { namespace: 'default', name: 'brokers', generation: 1, createdAt: '', updatedAt: '', spec: { ...spec, provider: name }, status: { phase: 'Ready', observedGeneration: 1, members: [], conditions: [] } },
      provider: { ...provider, name, backupTypes: name === 'rabbitmq' ? ['definitions'] : ['logical'] },
      namespace: 'default', name: 'brokers', base: '/clusters/default/brokers', can: () => true,
    };
    const markup = renderWithQueries(createElement(BackupsTab), context);
    assert.match(markup, /Backup policy/);
    if (name === 'rabbitmq') {
      assert.match(markup, backupWarning);
      assert.match(markup, backupSensitivity);
    } else assert.doesNotMatch(markup, /Definitions backups only|point-in-time restore/);
  }
});
