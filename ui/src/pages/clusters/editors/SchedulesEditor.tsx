// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { Checkbox, SelectField, TextField } from '@/components/ui/Field';

type Schedule = Schemas['BackupSchedule'];
type BackupType = Schemas['BackupSchedule']['type'];

const CRON_FIELD = /^(\*|[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*)(\/[0-9]+)?$/;

export function validateCron(expr: string): boolean {
  const parts = expr.trim().split(/\s+/);
  return parts.length === 5 && parts.every((p) => CRON_FIELD.test(p));
}

export const CRON_PRESETS: Array<{ label: string; cron: string }> = [
  { label: 'Every hour', cron: '0 * * * *' },
  { label: 'Daily at 02:00 UTC', cron: '0 2 * * *' },
  { label: 'Every 6 hours', cron: '0 */6 * * *' },
  { label: 'Weekly (Sunday 03:00 UTC)', cron: '0 3 * * 0' },
];

export function validateSchedules(items: Schedule[]): string[] {
  const errs: string[] = [];
  const seen: Record<string, true> = {};
  items.forEach((s, i) => {
    const label = s.name || `#${i + 1}`;
    if (!/^[a-z][a-z0-9-]{0,30}$/.test(s.name)) errs.push(`Schedule ${label}: name must be lowercase letters, digits and dashes.`);
    if (seen[s.name]) errs.push(`Schedule ${label}: duplicate name.`);
    seen[s.name] = true;
    if (!validateCron(s.cron)) errs.push(`Schedule ${label}: cron must have five fields.`);
    if (!s.storage) errs.push(`Schedule ${label}: choose a storage.`);
    if (!(s.retainCount >= 1)) errs.push(`Schedule ${label}: retain at least one backup.`);
  });
  return errs;
}

export interface SchedulesEditorProps {
  value: Schedule[];
  onChange: (items: Schedule[]) => void;
  storages: string[];
  defaultStorage?: string;
  backupTypes: BackupType[];
}

export function SchedulesEditor({ value, onChange, storages, defaultStorage, backupTypes }: SchedulesEditorProps) {
  const update = (i: number, patch: Partial<Schedule>) => onChange(value.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  const errors = validateSchedules(value);
  return (
    <div className="stack" style={{ gap: 12 }}>
      {value.length === 0 ? <p className="small muted">No schedules. Backups can still be taken on demand.</p> : null}
      {value.map((s, i) => (
        <fieldset key={i} className="card" style={{ padding: 14, margin: 0, border: '1px solid var(--border)' }}>
          <legend className="sr-only">Schedule {i + 1}</legend>
          <div className="grid-3">
            <TextField label="Name" value={s.name} onChange={(e) => update(i, { name: e.target.value })} placeholder="nightly" mono />
            <TextField
              label="Cron (UTC)"
              value={s.cron}
              onChange={(e) => update(i, { cron: e.target.value })}
              mono
              list={`cron-presets-${i}`}
              error={s.cron && !validateCron(s.cron) ? 'Five fields: minute hour day month weekday' : undefined}
              hint="e.g. 0 2 * * * for daily at 02:00"
            />
            <datalist id={`cron-presets-${i}`}>
              {CRON_PRESETS.map((p) => (
                <option key={p.cron} value={p.cron}>
                  {p.label}
                </option>
              ))}
            </datalist>
            <TextField label="Retain (count)" type="number" min={1} value={s.retainCount} onChange={(e) => update(i, { retainCount: Number(e.target.value) || 1 })} />
          </div>
          <div className="grid-3" style={{ marginTop: 12, alignItems: 'end' }}>
            <SelectField label="Type" value={s.type} onChange={(e) => update(i, { type: e.target.value as BackupType })}>
              {backupTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </SelectField>
            <SelectField label="Storage" value={s.storage} onChange={(e) => update(i, { storage: e.target.value })}>
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
      ))}
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
      {errors.length ? (
        <ul className="field-error" role="alert" style={{ margin: 0, paddingLeft: 18 }}>
          {errors.map((e) => (
            <li key={e}>{e}</li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
