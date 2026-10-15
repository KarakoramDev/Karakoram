// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { Checkbox, SelectField, TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { scheduleErrors } from '../wizard/schedules';

type Schedule = Schemas['BackupSchedule'];
type BackupType = Schemas['BackupSchedule']['type'];


export const CRON_PRESETS: Array<{ label: string; cron: string }> = [
  { label: 'Every hour', cron: '0 * * * *' },
  { label: 'Daily at 02:00 UTC', cron: '0 2 * * *' },
  { label: 'Every 6 hours', cron: '0 */6 * * *' },
  { label: 'Weekly (Sunday 03:00 UTC)', cron: '0 3 * * 0' },
];


export interface SchedulesEditorProps {
  value: Schedule[];
  onChange: (items: Schedule[]) => void;
  storages: string[];
  defaultStorage?: string;
  backupTypes: BackupType[];
  provider: string;
}

export function SchedulesEditor({ value, onChange, storages, defaultStorage, backupTypes, provider }: SchedulesEditorProps) {
  const update = (i: number, patch: Partial<Schedule>) => onChange(value.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  return (
    <div className="stack" style={{ gap: 12 }}>
      <RabbitMQBackupNotice provider={provider} />
      {value.length === 0 ? <p className="small muted">No schedules. Backups can still be taken on demand.</p> : null}
      {value.map((s, i) => {
        const errors = scheduleErrors(s, value);
        return (
        <fieldset key={i} className="card" style={{ padding: 14, margin: 0, border: '1px solid var(--border)' }}>
          <legend className="sr-only">Schedule {i + 1}</legend>
          <div className="grid-3">
            <TextField label="Name" value={s.name} onChange={(e) => update(i, { name: e.target.value })} placeholder="nightly" mono error={errors.name} />
            <TextField
              label="Cron (UTC)"
              value={s.cron}
              onChange={(e) => update(i, { cron: e.target.value })}
              mono
              list={`cron-presets-${i}`}
              error={errors.cron}
              hint="e.g. 0 2 * * * for daily at 02:00"
            />
            <datalist id={`cron-presets-${i}`}>
              {CRON_PRESETS.map((p) => (
                <option key={p.cron} value={p.cron}>
                  {p.label}
                </option>
              ))}
            </datalist>
            <TextField label="Retain (count)" type="number" min={1} step={1} value={s.retainCount} onChange={(e) => update(i, { retainCount: Number(e.target.value) })} error={errors.retainCount} />
          </div>
          <div className="grid-3" style={{ marginTop: 12, alignItems: 'end' }}>
            <SelectField label="Type" value={s.type} onChange={(e) => update(i, { type: e.target.value as BackupType })}>
              {backupTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </SelectField>
            <SelectField label="Storage" value={s.storage} onChange={(e) => update(i, { storage: e.target.value })} error={errors.storage}>
              <option value="">Choose…</option>
              {storages.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </SelectField>
            <div className="row" style={{ justifyContent: 'space-between', height: 34 }}>
              <Checkbox label="Enabled" checked={s.enabled} onChange={(e) => update(i, { enabled: e.target.checked })} />
              <Button size="sm" variant="ghost" onClick={() => onChange(value.filter((_, j) => j !== i))} aria-label={`Remove schedule ${s.name || i + 1}`}>
                Remove
              </Button>
            </div>
          </div>
        </fieldset>
        );
      })}
      <div>
        <Button
          size="sm"
          onClick={() =>
            onChange([
              ...value,
              {
                name: value.length === 0 ? 'daily' : `schedule-${value.length + 1}`,
                cron: '0 2 * * *',
                type: backupTypes[0] ?? 'logical',
                storage: defaultStorage ?? storages[0] ?? '',
                retainCount: 7,
                enabled: true,
              },
            ])
          }
        >
          + Add schedule
        </Button>
      </div>
    </div>
  );
}

export function RabbitMQBackupNotice({ provider }: { provider?: string }) {
  return provider === 'rabbitmq' ? <Callout tone="info" title="Definitions backups only">
    RabbitMQ definitions backups capture broker definitions (vhosts, users, permissions, policies, and queues/exchanges/bindings metadata), not messages. They do not provide point-in-time restore. Exports contain user password hashes. Treat exports and their object storage as sensitive. See the <a href="https://github.com/KarakoramDev/Karakoram/blob/main/docs/user-guide/rabbitmq.md#definitions-backups-and-cleanup" target="_blank" rel="noreferrer">RabbitMQ backup and restore guide</a>.
  </Callout> : null;
}
