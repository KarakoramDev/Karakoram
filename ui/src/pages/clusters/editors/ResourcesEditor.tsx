// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { ChoiceGroup, TextField } from '@/components/ui/Field';

type Resources = Schemas['Resources'];

export interface ResourcesEditorProps {
  value: Resources;
  onChange: (r: Resources) => void;
  /** Current persisted value; disk may only grow relative to it. */
  persisted?: Resources;
  /** Component has no data volume (routers). */
  noDisk?: boolean;
  idPrefix: string;
}

export function validateResources(r: Resources, persisted?: Resources, noDisk = false): string[] {
  const errs: string[] = [];
  if (!(r.minCpu >= 1) || !(r.maxCpu >= 1)) errs.push('CPU must be at least 1 core.');
  if (!Number.isInteger(r.minCpu) || !Number.isInteger(r.maxCpu) || (r.startCpu !== undefined && !Number.isInteger(r.startCpu))) {
    errs.push('CPU values must be whole cores.');
  }
  if (r.minCpu > r.maxCpu) errs.push('Minimum CPU must not exceed maximum CPU.');
  if (!(r.minRam >= 0.5) || !(r.maxRam >= 0.5)) errs.push('RAM must be at least 0.5 GiB.');
  if (r.minRam > r.maxRam) errs.push('Minimum RAM must not exceed maximum RAM.');
  if (!noDisk) {
    if (!(r.minDisk >= 5) || !(r.maxDisk >= 5)) errs.push('Disk must be at least 5 GiB.');
    if (r.minDisk > r.maxDisk) errs.push('Minimum disk must not exceed maximum disk.');
    if (persisted && (r.minDisk < persisted.minDisk || r.maxDisk < persisted.maxDisk)) errs.push('Disk can only grow; shrinking volumes is not supported.');
  }
  if (r.startCpu !== undefined && (r.startCpu < r.minCpu || r.startCpu > r.maxCpu)) errs.push('Start CPU must lie between minimum and maximum CPU.');
  return errs;
}

function num(v: string): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function ResourcesEditor({ value, onChange, persisted, noDisk, idPrefix }: ResourcesEditorProps) {
  const set = (patch: Partial<Resources>) => onChange({ ...value, ...patch });
  const errors = validateResources(value, persisted, noDisk);
  return (
    <div className="stack" style={{ gap: 14 }}>
      <ChoiceGroup<Resources['cpuMode']>
        name={`${idPrefix}-cpuMode`}
        legend="CPU mode"
        value={value.cpuMode}
        onChange={(cpuMode) => set({ cpuMode })}
        options={[
          { value: 'SHARED', title: 'Shared', description: 'Burstable cores, lower cost. Fine for dev and light workloads.' },
          { value: 'DEDICATED', title: 'Dedicated', description: 'Reserved cores with predictable latency for production.' },
        ]}
      />
      <div className="grid-3">
        <TextField id={`${idPrefix}-minCpu`} label="Min CPU (cores)" type="number" min={1} step={1} value={value.minCpu} onChange={(e) => set({ minCpu: num(e.target.value) })} />
        <TextField id={`${idPrefix}-maxCpu`} label="Max CPU (cores)" type="number" min={1} step={1} value={value.maxCpu} onChange={(e) => set({ maxCpu: num(e.target.value) })} />
        <TextField
          id={`${idPrefix}-startCpu`}
          label="Start CPU (optional)"
          type="number"
          min={1}
          step={1}
          value={value.startCpu ?? ''}
          onChange={(e) => set({ startCpu: e.target.value === '' ? undefined : num(e.target.value) })}
          hint="Initial allocation on boot."
        />
      </div>
      <div className="grid-2">
        <TextField id={`${idPrefix}-minRam`} label="Min RAM (GiB)" type="number" min={0.5} step={0.25} value={value.minRam} onChange={(e) => set({ minRam: num(e.target.value) })} />
        <TextField id={`${idPrefix}-maxRam`} label="Max RAM (GiB)" type="number" min={0.5} step={0.25} value={value.maxRam} onChange={(e) => set({ maxRam: num(e.target.value) })} />
      </div>
      {noDisk ? null : (
        <div className="grid-2">
          <TextField
            id={`${idPrefix}-minDisk`}
            label="Min disk (GiB)"
            type="number"
            min={Math.max(persisted?.minDisk ?? 5, 5)}
            step={1}
            value={value.minDisk}
            onChange={(e) => set({ minDisk: num(e.target.value) })}
            hint={persisted ? `Currently ${persisted.minDisk} GiB. Disk only grows.` : undefined}
          />
          <TextField
            id={`${idPrefix}-maxDisk`}
            label="Max disk (GiB)"
            type="number"
            min={Math.max(persisted?.maxDisk ?? 5, 5)}
            step={1}
            value={value.maxDisk}
            onChange={(e) => set({ maxDisk: num(e.target.value) })}
            hint={persisted ? `Currently ${persisted.maxDisk} GiB.` : 'Volumes auto-grow between min and max.'}
          />
        </div>
      )}
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
