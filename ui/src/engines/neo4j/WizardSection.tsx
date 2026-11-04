// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Switch, TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import type { WizardSectionProps } from '../types';
import { options, settingKeys } from './spec';

export function WizardSection({ spec, onChange }: WizardSectionProps) {
  const value = options(spec);
  const set = (key: string, next: unknown) => {
    const updated = { ...value };
    if (next === undefined) delete updated[key]; else updated[key] = next;
    onChange({ ...spec, engineOptions: updated });
  };
  const settings = value.settings && typeof value.settings === 'object' ? value.settings as Record<string, string> : {};
  return <div className="stack">
    <Callout tone="warn">Neo4j Community is a single instance without high availability. Every backup, restart, upgrade, TLS rotation and memory change is a full outage.</Callout>
    <TextField label="Heap (MiB, optional)" type="number" min={256} max={31744} step={1} value={typeof value.heapMB === 'number' ? value.heapMB : ''} onChange={(e) => set('heapMB', e.target.value === '' ? undefined : Number(e.target.value))} />
    <TextField label="Page cache (MiB, optional)" type="number" min={64} step={1} value={typeof value.pageCacheMB === 'number' ? value.pageCacheMB : ''} onChange={(e) => set('pageCacheMB', e.target.value === '' ? undefined : Number(e.target.value))} />
    <p className="small muted">Heap: 256–31744 MiB; page cache: at least 64 MiB. Together they must fit within 80% of minimum RAM, not the autoscaling maximum. Empty values use resource-derived defaults (40% heap and 25% page cache, rounded down to 64 MiB). Memory changes restart the database and cause a full outage.</p>
    <Switch label="Accept scheduled backup downtime" checked={value.acceptBackupDowntime === true} onChange={(e) => set('acceptBackupDowntime', e.target.checked)} />
    <Callout tone="warn">Every backup stops the database for the entire dump and upload. Scheduled backups require explicit acceptance of this full outage; no maintenance window is enforced.</Callout>
    <details><summary>Allowed Neo4j settings</summary><div className="stack">
      <p className="small muted">Only these query settings are accepted. Paths, networking, TLS, authentication and JVM settings are managed. Empty fields use the Neo4j default.</p>
      {settingKeys.map((key) => <TextField key={key} label={key} value={settings[key] ?? ''} onChange={(e) => {
        const next = { ...settings };
        if (e.target.value) next[key] = e.target.value; else delete next[key];
        set('settings', Object.keys(next).length ? next : undefined);
      }} />)}
    </div></details>
  </div>;
}
