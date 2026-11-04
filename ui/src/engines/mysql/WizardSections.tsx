// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Button } from '@/components/ui/Button';
import { SelectField, Switch, TextField } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { ResourcesEditor } from '@/pages/clusters/editors/ResourcesEditor';
import { resourceMinimums } from '@/pages/clusters/wizard/resources';
import type { WizardSectionProps } from '../types';
import { CONSISTENCIES, PARAMETERS, component, defaultComponent, object, withComponent } from './spec';

export function TopologySection({ provider, spec, onChange }: WizardSectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return <div className="stack">
    <Callout tone="warn">{spec.provider === 'pxc' ? 'PXC uses Galera with three or five members and one ProxySQL-routed writer. A majority must remain Primary and Synced. Quorum loss rejects reads and writes; recovery must never bootstrap an unfenced partition.' : spec.topology.type === 'single' ? 'Single is not highly available. Maintenance interrupts database access.' : 'Group Replication provides automatic failover and needs a majority. Writes block on quorum loss.'}</Callout>
    <SelectField label="Data members" value={spec.components.engine.replicas} onChange={(e) => onChange(withComponent(spec, 'engine', { ...spec.components.engine, replicas: Number(e.target.value) }))}>{(spec.topology.type === 'single' ? [1] : [3, 5]).map((n) => <option key={n} value={n}>{n}</option>)}</SelectField>
    <TextField label="Proxy replicas" type="number" min={spec.topology.type === 'single' ? 0 : 1} max={5} step={1} value={component(spec, 'proxy')?.replicas ?? 0} onChange={(e) => onChange(withComponent(spec, 'proxy', { ...(component(spec, 'proxy') ?? defaultComponent(topology, 'proxy')), replicas: Number(e.target.value) }))} hint="ProxySQL supplies the application endpoint. Single permits zero; Group Replication and PXC require at least one. Clients must reconnect after failover." />
    {spec.provider === 'pxc' ? <Callout tone="info" title="Check schema compatibility">Strict mode stays ENFORCING. Use InnoDB tables with primary keys; non-transactional writes and LOCK TABLES are unsupported. Galera certification can abort transactions, and timeouts can leave commit outcomes unknown. Use idempotent application retries.</Callout> : null}
  </div>;
}
export function ResourcesSection({ provider, spec, onChange }: WizardSectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return <div className="stack">{['engine', 'proxy'].map((name) => {
    const c = component(spec, name);
    return c?.replicas ? <Card key={name} title={`${name === 'engine' ? 'Data members' : 'ProxySQL'} · ${c.replicas} replicas`}><ResourcesEditor idPrefix={`mysql-${name}`} value={c.resources} noDisk={name !== 'engine'} minimums={resourceMinimums(spec.provider, topology, name)} onChange={(resources) => onChange(withComponent(spec, name, { ...c, resources }))} /></Card> : null;
  })}</div>;
}
export function OptionsSection({ spec, onChange }: WizardSectionProps) {
  const options = object(spec.engineOptions), parameters = object(options.parameters), proxy = object(options.proxy), group = object(options.groupReplication), binlog = object(options.binlog);
  const set = (patch: Record<string, unknown>) => onChange({ ...spec, engineOptions: { ...options, ...patch } });
  return <div className="stack">
    <Switch label="Proxy read/write split" checked={proxy.readWriteSplit === true} onChange={(e) => set({ proxy: { ...proxy, readWriteSplit: e.target.checked } })} />
    <Switch label="Separate reader port (3307)" checked={proxy.readerPort === true} onChange={(e) => set({ proxy: { ...proxy, readerPort: e.target.checked } })} />
    <Callout tone="warn">Read/write splitting and the reader port are routing conveniences, not a consistency guarantee. Reads may be stale; route read-after-write transactions to the writer.{spec.provider === 'pxc' ? ' Galera certification does not imply that a reader has applied every committed transaction.' : ' Choose suitable Group Replication consistency.'}</Callout>
    {spec.provider === 'pxc' ? <Callout tone="info" title="Galera replication">Encrypted IST reuses retained Galera cache when possible; otherwise a full SST copies data from a donor. Group Replication settings do not apply. Slow members can apply flow control to the whole Primary component.</Callout> : <>
      <SelectField label="Group Replication consistency" value={String(group.consistency ?? CONSISTENCIES[0])} onChange={(e) => set({ groupReplication: { ...group, consistency: e.target.value } })}>{CONSISTENCIES.map((v) => <option key={v} value={v}>{v}</option>)}</SelectField>
      <TextField label="Unreachable majority timeout (seconds)" type="number" min={1} max={31536000} value={Number(group.unreachableMajorityTimeout ?? 30)} onChange={(e) => set({ groupReplication: { ...group, unreachableMajorityTimeout: Number(e.target.value) } })} />
    </>}
    <TextField label="Binlog archive interval (seconds)" type="number" min={1} max={86400} value={Number(binlog.archiveInterval ?? 300)} onChange={(e) => set({ binlog: { ...binlog, archiveInterval: Number(e.target.value) } })} />
    <TextField label="Binlog expiry (seconds)" type="number" min={Number(binlog.archiveInterval ?? 300) + 1} max={2147483647} value={Number(binlog.expireSeconds ?? 604800)} onChange={(e) => set({ binlog: { ...binlog, expireSeconds: Number(e.target.value) } })} />
    <strong>MySQL parameters</strong>
    {Object.entries(parameters).map(([key, value]) => <div className="row" key={key}><SelectField label="Parameter" value={key} onChange={(e) => set({ parameters: Object.fromEntries(Object.entries(parameters).map(([k, v]) => [k === key ? e.target.value : k, v])) })}><option value="">Choose parameter</option>{PARAMETERS.map((p) => <option key={p} disabled={p !== key && p in parameters} value={p}>{p}</option>)}</SelectField><TextField label="Value" value={String(value)} onChange={(e) => set({ parameters: { ...parameters, [key]: e.target.value } })} /><Button onClick={() => set({ parameters: Object.fromEntries(Object.entries(parameters).filter(([k]) => k !== key)) })}>Remove</Button></div>)}
    <Button disabled={'' in parameters} onClick={() => set({ parameters: { ...parameters, '': '' } })}>Add parameter</Button>
    <p className="small muted">Only backend-allowlisted settings are offered. Values are validated by the backend, which is authoritative. Dynamic parameters can apply live; restart-required changes roll members. TLS, ports, data paths and replication are managed.</p>
  </div>;
}
