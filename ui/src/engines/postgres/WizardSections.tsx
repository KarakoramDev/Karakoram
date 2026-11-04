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
    {spec.topology.type === 'distributed' ? <>
      <TextField label="Worker groups" type="number" min={1} max={8} step={1} value={Number(object(spec.engineOptions?.citus).workerGroups ?? 2)} onChange={(e) => onChange({ ...spec, engineOptions: { ...spec.engineOptions, citus: { ...object(spec.engineOptions?.citus), workerGroups: Number(e.target.value) } } })} hint="1–8 worker groups. Reducing the count drains shards before retiring a group." />
      <TextField label="Citus database" value={String(object(spec.engineOptions?.citus).database ?? 'app')} onChange={(e) => onChange({ ...spec, engineOptions: { ...spec.engineOptions, citus: { ...object(spec.engineOptions?.citus), database: e.target.value } } })} hint="The database in which Citus is installed (default: app)." />
    </> : null}
    <SelectField label={spec.topology.type === 'distributed' ? 'Data members per group' : 'Data members'} value={spec.components.engine.replicas} onChange={(e) => onChange(withComponent(spec, 'engine', { ...spec.components.engine, replicas: Number(e.target.value) }))}>{(spec.topology.type === 'single' ? [1] : spec.topology.type === 'distributed' ? [1, 3] : [3, 5]).map((n) => <option key={n} value={n}>{n}</option>)}</SelectField>
    <TextField label="Proxy replicas" type="number" min={spec.topology.type === 'single' || spec.topology.type === 'distributed' && spec.components.engine.replicas === 1 ? 0 : 1} step={1} value={component(spec, 'proxy')?.replicas ?? 0} onChange={(e) => onChange(withComponent(spec, 'proxy', { ...(component(spec, 'proxy') ?? defaultComponent(topology, 'proxy')), replicas: Number(e.target.value) }))} hint="ProxySQL supplies the stable libpq endpoint. Zero is permitted only for single-member groups." />
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
  const profiles = object(provider.engineOptions?.profiles);
  const selectedProfiles = Array.isArray(options.profiles) ? options.profiles.filter((id): id is string => typeof id === 'string') : [];
  const major = provider.versions.find((v) => v.version === spec.version)?.major ?? spec.version.split('.')[0] ?? '';
  return <div className="stack">
    <SelectField label="PostgreSQL major" value={major} onChange={(e) => {
      const versions = provider.versions.filter((v) => v.major === e.target.value && v.status !== 'unavailable');
      const version = versions.find((v) => v.default) ?? versions[0];
      if (version) onChange({ ...spec, version: version.version });
    }}>{[...new Set(provider.versions.filter((v) => v.status !== 'unavailable').map((v) => v.major))].map((major) => <option key={major} value={major}>{major}</option>)}</SelectField>
    {spec.provider === 'postgres' && Array.isArray(profiles.items) ? <fieldset className="stack">
      <legend>PostgreSQL profiles</legend>
      {profiles.items.map((value) => {
        const profile = object(value), id = String(profile.id ?? '');
        const capability = object(object(profile.majors)[major]);
        const available = capability.available === true;
        const extensions = Array.isArray(capability.extensions) ? capability.extensions.map((extension) => String(object(extension).name)).join(', ') : '';
        return <div key={id}>
          <Switch label={`${id}${extensions ? ` · ${extensions}` : ''}`} checked={selectedProfiles.includes(id)} disabled={!available} onChange={(e) => set({ profiles: e.target.checked ? [...selectedProfiles, id] : selectedProfiles.filter((name) => name !== id) })} />
          {!available ? <p className="small muted">{String(capability.reason ?? `Unavailable on PostgreSQL ${major}`)}</p> : null}
        </div>;
      })}
      <p className="small muted">Profiles install extensions and are additive: existing profiles cannot be removed after creation.</p>
    </fieldset> : null}
    {spec.topology.type !== 'single' ? <SelectField label="Synchronous replication" value={String(options.synchronousMode ?? 'off')} onChange={(e) => set({ synchronousMode: e.target.value })}>{['off', 'on', 'strict'].map((v) => <option key={v} value={v}>{v}</option>)}</SelectField> : null}
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
