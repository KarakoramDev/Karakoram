// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Button } from '@/components/ui/Button';
import { SelectField, Switch, TextField } from '@/components/ui/Field';
import { Card } from '@/components/ui/Primitives';
import { ResourcesEditor } from '@/pages/clusters/editors/ResourcesEditor';
import { resourceMinimums } from '@/pages/clusters/wizard/resources';
import type { WizardSectionProps } from '../types';
import { SUPABASE_COMPONENTS, applyPreset, component, defaultComponent, dependencies, object, toggleComponent, withComponent } from './spec';

export function TopologySection({ provider, spec, onChange }: WizardSectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return <div className="stack">
    <SelectField label="Data members" value={spec.components.engine.replicas} onChange={(e) => onChange(withComponent(spec, 'engine', { ...spec.components.engine, replicas: Number(e.target.value) }))}>{(spec.topology.type === 'single' ? [1] : [3, 5]).map((n) => <option key={n} value={n}>{n}</option>)}</SelectField>
    <TextField label="Proxy replicas" type="number" min={spec.topology.type === 'single' ? 0 : 1} step={1} value={component(spec, 'proxy')?.replicas ?? 0} onChange={(e) => onChange(withComponent(spec, 'proxy', { ...(component(spec, 'proxy') ?? defaultComponent(topology, 'proxy')), replicas: Number(e.target.value) }))} hint="ProxySQL supplies the stable libpq endpoint. Zero is permitted only for single-member topology." />
    {spec.provider === 'supabase' ? <>
      <SelectField label="Component preset" value="" onChange={(e) => { if (e.target.value) onChange(applyPreset(spec, topology, e.target.value, provider)); }}><option value="">Choose preset to replace selection</option>{['minimal', 'standard', 'full'].map((name) => <option key={name} value={name}>{name}</option>)}</SelectField>
      {SUPABASE_COMPONENTS.map((name) => <div className="row-between wrap" key={name}><Switch label={`${name}${dependencies(topology, name).length ? ` (requires ${dependencies(topology, name).join(', ')})` : ''}`} checked={Boolean(component(spec, name)?.replicas)} onChange={(e) => onChange(toggleComponent(spec, topology, name, e.target.checked))} />{component(spec, name)?.replicas ? <TextField label={`${name} replicas`} type="number" min={1} step={1} value={component(spec, name)!.replicas} onChange={(e) => onChange(withComponent(spec, name, { ...component(spec, name)!, replicas: Number(e.target.value) }))} /> : null}</div>)}
      <p className="small muted">Enabling a component enables its dependencies. Disabling a dependency also disables its dependents.</p>
    </> : null}
  </div>;
}

export function ResourcesSection({ provider, spec, onChange }: WizardSectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return <div className="stack">{['engine', 'proxy', ...(spec.provider === 'supabase' ? SUPABASE_COMPONENTS : [])].map((name) => {
    const c = component(spec, name);
    return c?.replicas ? <Card key={name} title={`${name === 'engine' ? 'Data members' : name} · ${c.replicas} replicas`}><ResourcesEditor idPrefix={`pg-${name}`} value={c.resources} noDisk={name !== 'engine'} minimums={resourceMinimums(spec.provider, topology, name)} onChange={(resources) => onChange(withComponent(spec, name, { ...c, resources }))} /></Card> : null;
  })}</div>;
}

export function OptionsSection({ provider, spec, onChange }: WizardSectionProps) {
  const options = object(spec.engineOptions), parameters = object(options.parameters), proxy = object(options.proxy);
  const set = (patch: Record<string, unknown>) => onChange({ ...spec, engineOptions: { ...options, ...patch } });
  return <div className="stack">
    <SelectField label="PostgreSQL major" value={provider.versions.find((v) => v.version === spec.version)?.major ?? spec.version.split('.')[0]} onChange={(e) => {
      const versions = provider.versions.filter((v) => v.major === e.target.value && v.status !== 'unavailable');
      const version = versions.find((v) => v.default) ?? versions[0];
      if (version) onChange({ ...spec, version: version.version });
    }}>{[...new Set(provider.versions.filter((v) => v.status !== 'unavailable').map((v) => v.major))].map((major) => <option key={major} value={major}>{major}</option>)}</SelectField>
    {spec.topology.type === 'ha' ? <SelectField label="Synchronous replication" value={String(options.synchronousMode ?? 'off')} onChange={(e) => set({ synchronousMode: e.target.value })}>{['off', 'on', 'strict'].map((v) => <option key={v} value={v}>{v}</option>)}</SelectField> : null}
    <Switch label="Proxy read/write split" checked={proxy.readWriteSplit === true} onChange={(e) => set({ proxy: { ...proxy, readWriteSplit: e.target.checked } })} />
    <Switch label="Separate reader port (5433)" checked={proxy.readerPort === true} onChange={(e) => set({ proxy: { ...proxy, readerPort: e.target.checked } })} />
    <strong>PostgreSQL parameters</strong>
    {Object.entries(parameters).map(([key, value], index) => <div className="row" key={index}>
      <TextField label="Parameter" value={key} onChange={(e) => set({ parameters: Object.fromEntries(Object.entries(parameters).map(([k, v]) => [k === key ? e.target.value : k, v])) })} />
      <TextField label="Value" value={String(value)} onChange={(e) => set({ parameters: { ...parameters, [key]: e.target.value } })} />
      <Button onClick={() => set({ parameters: Object.fromEntries(Object.entries(parameters).filter(([k]) => k !== key)) })}>Remove</Button>
    </div>)}
    <Button onClick={() => set({ parameters: { ...parameters, '': '' } })} disabled={'' in parameters}>Add parameter</Button>
    <p className="xs faint">Only provider-allowlisted PostgreSQL settings are accepted. TLS, ports, WAL archiving and data paths are managed.</p>
  </div>;
}
