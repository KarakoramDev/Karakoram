// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { ChoiceGroup, TextField } from '@/components/ui/Field';
import { resourceErrors, type ResourceMinimums } from '../wizard/resources';

type Resources = Schemas['Resources'];

export interface ResourcesEditorProps {
  value: Resources;
  onChange: (r: Resources) => void;
  /** Current persisted value; disk may only grow relative to it. */
  persisted?: Resources;
  /** Component has no data volume (routers). */
  noDisk?: boolean;
  idPrefix: string;
  minimums: ResourceMinimums;
  showErrors?: boolean;
}


function num(v: string): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function ResourcesEditor({ value, onChange, persisted, noDisk, idPrefix, minimums, showErrors = true }: ResourcesEditorProps) {
  const set = (patch: Partial<Resources>) => onChange({ ...value, ...patch });
  const errors = showErrors ? resourceErrors(value, persisted, noDisk, minimums) : {};
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
        <TextField id={`${idPrefix}-minCpu`} label="Min CPU (cores)" type="number" min={minimums.cpu} step={1} value={value.minCpu} onChange={(e) => set({ minCpu: num(e.target.value) })} error={errors.minCpu} />
        <TextField id={`${idPrefix}-maxCpu`} label="Max CPU (cores)" type="number" min={minimums.cpu} step={1} value={value.maxCpu} onChange={(e) => set({ maxCpu: num(e.target.value) })} error={errors.maxCpu} />
        <TextField
          id={`${idPrefix}-startCpu`}
          label="Start CPU (optional)"
          type="number"
          min={minimums.cpu}
          step={1}
          value={value.startCpu ?? ''}
          onChange={(e) => set({ startCpu: e.target.value === '' ? undefined : num(e.target.value) })}
          hint="Initial allocation on boot."
          error={errors.startCpu}
        />
      </div>
      <div className="grid-2">
        <TextField id={`${idPrefix}-minRam`} label="Min RAM (GiB)" type="number" min={minimums.ram} step={0.25} value={value.minRam} onChange={(e) => set({ minRam: num(e.target.value) })} error={errors.minRam} />
        <TextField id={`${idPrefix}-maxRam`} label="Max RAM (GiB)" type="number" min={minimums.ram} step={0.25} value={value.maxRam} onChange={(e) => set({ maxRam: num(e.target.value) })} error={errors.maxRam} />
      </div>
      {noDisk ? null : (
        <div className="grid-2">
          <TextField
            id={`${idPrefix}-minDisk`}
            label="Min disk (GiB)"
            type="number"
            min={Math.max(persisted?.minDisk ?? minimums.disk, minimums.disk)}
            step={1}
            value={value.minDisk}
            onChange={(e) => set({ minDisk: num(e.target.value) })}
            hint={persisted ? `Currently ${persisted.minDisk} GiB. Disk only grows.` : undefined}
            error={errors.minDisk}
          />
          <TextField
            id={`${idPrefix}-maxDisk`}
            label="Max disk (GiB)"
            type="number"
            min={Math.max(persisted?.maxDisk ?? minimums.disk, minimums.disk)}
            step={1}
            value={value.maxDisk}
            onChange={(e) => set({ maxDisk: num(e.target.value) })}
            hint={persisted ? `Currently ${persisted.maxDisk} GiB.` : 'Volumes auto-grow between min and max.'}
            error={errors.maxDisk}
          />
        </div>
      )}
    </div>
  );
}
