// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement, isValidElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { matchRoutes } from 'react-router-dom';
import type { Schemas } from '@/api/client';

register('../../../test/tsx-loader.mjs', import.meta.url);
// TSX and aliased modules must load after registering the source loader.
const { ChannelDialog } = await import('./Notifications.tsx');
const { BackupPolicyDialog, BackupReport } = await import('./ControlPlaneBackup.tsx');
const { AgentRolloutDialog } = await import('./AgentRollout.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { appRoutes } = await import('../../App.tsx');

const unchanged = () => { assert.fail('Server rendering must not submit or close a dialog'); };
function render(element: ReactElement): string {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try { return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, element))); }
  finally { client.clear(); }
}

const policy: Schemas['ControlPlaneBackupPolicy'] = { enabled: true, storage: { namespace: 'production', name: 'state-backups' }, schedule: '0 3 * * *', retainCount: 14 };

test('all installation operations pages resolve through the protected settings layout', () => {
  for (const [path, component] of [['notifications', 'NotificationsPage'], ['control-plane-backup', 'ControlPlaneBackupPage'], ['agent-rollout', 'AgentRolloutPage']]) {
    const matches = matchRoutes(appRoutes, `/settings/${path}`);
    assert.ok(matches);
    const names = matches.map((match) => {
      const element = match.route.element;
      return isValidElement(element) && typeof element.type === 'function' ? element.type.name : undefined;
    });
    assert.ok(names.includes('ProtectedRoute'));
    assert.ok(names.includes('SettingsLayout'));
    assert.equal(names.at(-1), component);
  }
});

test('channel editor presents write-only empty secret fields and never serializes supplied secrets', () => {
  const existing: Schemas['NotificationChannel'] = { name: 'webhook-alerts', type: 'webhook', enabled: true, webhook: { host: 'hooks.example.com', urlConfigured: true, signingSecretConfigured: true, url: 'private-url-sentinel', signingSecret: 'private-signing-sentinel' } };
  const markup = render(createElement(ChannelDialog, { existing, onClose: unchanged }));
  assert.match(markup, /Edit channel webhook-alerts/);
  assert.match(markup, /Saved host: hooks.example.com/);
  assert.equal((markup.match(/type="password"/g) ?? []).length, 2);
  assert.doesNotMatch(markup, /private-url-sentinel|private-signing-sentinel/);
  assert.match(markup, /All categories/);
  assert.match(markup, /A specific list excludes installation-wide events/);
  assert.match(markup, />Save channel<\/button>/);
});

test('email editor provides TLS transport and recipient fields without password readback', () => {
  const existing: Schemas['NotificationChannel'] = { name: 'email-alerts', type: 'email', enabled: false, email: { host: 'smtp.example.com', port: 465, security: 'tls', username: 'alerts', passwordConfigured: true, password: 'private-email-sentinel', from: 'alerts@example.com', to: ['ops@example.com'] } };
  const markup = render(createElement(ChannelDialog, { existing, onClose: unchanged }));
  assert.match(markup, /Implicit TLS/);
  assert.match(markup, /SMTP password/);
  assert.match(markup, /value="ops@example.com"/);
  assert.doesNotMatch(markup, /private-email-sentinel/);
});

test('backup policy editor exposes storage identity, UTC cron and bounded retention without credentials', () => {
  const markup = render(createElement(BackupPolicyDialog, { policy, onClose: unchanged }));
  assert.match(markup, /Enable scheduled backups/);
  assert.match(markup, /Storage namespace/);
  assert.match(markup, /value="production"/);
  assert.match(markup, /value="state-backups"/);
  assert.match(markup, /Schedule \(UTC\)/);
  assert.match(markup, /min="1" max="1000"/);
  assert.match(markup, />Save policy<\/button>/);
  assert.doesNotMatch(markup, /type="password"/);
});

test('backup details expose durable terminal and rebuild metadata', () => {
  const backup: Schemas['ControlPlaneBackup'] = { id: 'run-1', phase: 'Failed', trigger: 'manual', storage: policy.storage, createdAt: '2026-10-21T01:00:00Z', startedAt: '2026-10-21T01:00:01Z', completedAt: '2026-10-21T01:00:02Z', error: 'Upload failed', objectKey: 'state/run-1.db.gz', sizeBytes: 1024, sha256: 'digest-value', schemaVersion: 'schema-version', karakoramVersion: 'release-version', keyFingerprint: 'kkfp1:fingerprint' };
  const markup = renderToStaticMarkup(createElement(BackupReport, { backup }));
  for (const value of ['Upload failed', 'Failed', 'production/state-backups', 'state/run-1.db.gz', '1 KiB', 'digest-value', 'schema-version', 'release-version', 'kkfp1:fingerprint']) assert.ok(markup.includes(value), value);
  assert.match(markup, /role="alert"/);
  assert.match(markup, /date[Tt]ime="2026-10-21T01:00:02Z"/);
});

test('agent rollout editor bounds concurrency and does not allow editing current build metadata', () => {
  const settings: Schemas['AgentRolloutSettings'] = { automatic: false, maxConcurrentClusters: 1, currentBuild: { version: 'current-version-sentinel', commit: 'commit-sentinel', build: 'build-sentinel' } };
  const markup = render(createElement(AgentRolloutDialog, { settings, onClose: unchanged }));
  assert.match(markup, /Automatically roll out new agent builds/);
  assert.match(markup, /min="1" max="5"/);
  assert.match(markup, /disruptive member restarts/);
  assert.match(markup, />Save rollout settings<\/button>/);
  assert.doesNotMatch(markup, /current-version-sentinel|commit-sentinel|build-sentinel/);
});
