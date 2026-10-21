// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import type { WizardSectionProps } from '../types';
import { Checkbox, TextField } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { API_MODULES, INFERENCE_MODULES, options } from './helpers';

export default function WizardSection({ spec, onChange }: WizardSectionProps) {
  const o = options(spec);
  const update = (patch: Record<string, unknown>) => onChange({ ...spec, engineOptions: { ...o, ...patch } });
  return <section className="stack" aria-label="Weaviate options">
    <Callout>Single members have no redundancy. Clusters retain three fixed Raft voters; additional data members do not rebalance existing shards. Client TLS does not encrypt internal transports: require an isolated private network.</Callout>
    <div className="grid-2">
      <TextField type="number" label="Replication minimum factor" min={1} max={spec.components.engine.replicas} value={o.replicationMinimumFactor} onChange={(e) => update({ replicationMinimumFactor: Number(e.target.value) })} />
      <TextField type="number" label="Query defaults limit" min={1} max={10000} value={o.queryDefaultsLimit} onChange={(e) => update({ queryDefaultsLimit: Number(e.target.value) })} />
      <TextField type="number" label="Disk warning percent" min={1} max={94} value={o.diskWarningPercent} onChange={(e) => update({ diskWarningPercent: Number(e.target.value) })} />
      <TextField type="number" label="Disk readonly percent" min={2} max={95} value={o.diskReadonlyPercent} onChange={(e) => update({ diskReadonlyPercent: Number(e.target.value) })} />
    </div>
    <Checkbox label="Async replication" checked={o.asyncReplication} onChange={(e) => update({ asyncReplication: e.target.checked })} />
    <fieldset className="stack"><legend>API-based modules</legend><div className="grid-2">{API_MODULES.map((module) => <Checkbox key={module} label={module} checked={o.modules.includes(module)} onChange={(e) => update({ modules: e.target.checked ? [...o.modules, module] : o.modules.filter((m) => m !== module) })} />)}</div></fieldset>
    <p className="small muted">backup-s3 is managed automatically. Supply inference-provider API keys in native request headers, not in cluster configuration. Module changes restart members.</p>
    <fieldset className="stack"><legend>Inference endpoints</legend><p className="small muted">Optional external services; Karakoram does not provision them. Enter an HTTP(S) URL without credentials, query or fragment; leave blank to disable.</p>{INFERENCE_MODULES.map((module) => <TextField key={module} label={module} value={o.inference[module]?.url ?? ''} onChange={(e) => {
      const inference = { ...o.inference };
      if (e.target.value) inference[module] = { url: e.target.value }; else delete inference[module];
      update({ inference });
    }} />)}</fieldset>
  </section>;
}
