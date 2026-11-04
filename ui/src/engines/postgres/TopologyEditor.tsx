// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { SelectField, Switch } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { TLSEditor, ExposureEditor } from '@/pages/clusters/editors/AccessEditor';
import { validateExposure, validateTLS, wireExposure } from '@/pages/clusters/wizard/access';
import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { detailProvider, reviewChanges } from '../index';
import type { TopologyEditorProps } from '../types';
import { SupabaseOptions } from '../supabase/WizardSection';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { component, defaultComponent, normalizeSpec, postgresChangeReview, validateComponents, validateSpec } from './spec';
import { DistributedTopology } from './DistributedTopology';

export function TopologyEditor({ cluster, provider: catalog, editable, busy, submit }: TopologyEditorProps) {
  const provider = detailProvider(cluster.spec.provider, catalog)!;
  const [draft, setDraft] = useState(() => structuredClone(cluster.spec));
  const [seed, setSeed] = useState(() => JSON.stringify(cluster.spec));
  const [confirm, setConfirm] = useState(false);
  const [allAddresses, setAllAddresses] = useState(false);
  const submission = useSubmissionKey(JSON.stringify(draft));
  const { patch, consequences } = reviewChanges(cluster.spec, draft, {
    ...postgresChangeReview,
    buildPatch: (current, proposed) => {
      const result = postgresChangeReview.buildPatch(current, proposed);
      for (const key of ['tls', 'exposure', 'monitoring'] as const) if (JSON.stringify(current[key]) !== JSON.stringify(proposed[key])) result[key] = key === 'exposure' && proposed.exposure ? wireExposure(proposed.exposure, allAddresses) : proposed[key];
      return result;
    },
  });
  const topology = provider.topologies.find((t) => t.type === draft.topology.type);
  const validation = validateComponents(draft, topology, (r, stateless, name) => validateResources(r, component(cluster.spec, name)?.resources, stateless, resourceMinimums(draft.provider, topology, name)));
  const errors = [...validateSpec(draft), ...validation.topology, ...validation.resources, ...validateTLS(draft.tls, cluster.spec.tls?.ca), ...(patch.exposure ? validateExposure(draft.exposure, allAddresses) : [])];
  if (cluster.spec.topology.type === 'ha' && draft.topology.type !== 'ha') errors.push('HA cannot be converted to single.');
  if ((cluster.spec.topology.type === 'distributed') !== (draft.topology.type === 'distributed')) errors.push('Distributed topology cannot be converted to or from another topology.');
  if (seed !== JSON.stringify(cluster.spec)) errors.push('The cluster specification changed. Discard to load the current specification before applying changes.');
  const props = { provider, namespace: cluster.namespace, spec: draft, onChange: (next: typeof draft) => setDraft(normalizeSpec(next)) };
  const Options = draft.provider === 'supabase' ? SupabaseOptions : OptionsSection;
  return <div className="stack">
    <DistributedTopology />
    <fieldset disabled={!editable || busy} className="stack" style={{ border: 0, margin: 0, padding: 0 }}>
      <Card title="PostgreSQL topology"><SelectField label="Topology" disabled={cluster.spec.topology.type === 'distributed'} value={draft.topology.type} onChange={(e) => setDraft(normalizeSpec({ ...draft, topology: { type: e.target.value as 'single' | 'ha' }, components: { ...draft.components, engine: { ...draft.components.engine, replicas: e.target.value === 'ha' ? 3 : 1 }, extra: { ...draft.components.extra, proxy: { ...(component(draft, 'proxy') ?? defaultComponent(topology, 'proxy')), replicas: Math.max(e.target.value === 'ha' ? 1 : 0, component(draft, 'proxy')?.replicas ?? 0) } } } }))}><option value="single" disabled={cluster.spec.topology.type === 'ha'}>Single</option><option value="ha">HA</option>{cluster.spec.topology.type === 'distributed' ? <option value="distributed">Distributed</option> : null}</SelectField><TopologySection {...props} /></Card>
      <ResourcesSection {...props} />
      <Card title="Engine options"><Options {...props} /></Card>
      <Card title="TLS"><TLSEditor storedCA={cluster.spec.tls?.ca} value={draft.tls ?? { mode: 'requireTLS', ca: 'selfSigned' }} onChange={(tls) => setDraft({ ...draft, tls })} /></Card>
      <Card title="Database exposure"><ExposureEditor value={draft.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] }} onChange={(exposure) => setDraft({ ...draft, exposure })} allAddressesConfirmed={allAddresses} onAllAddressesConfirmedChange={setAllAddresses} publicPort={5432} publicPortLabel="PostgreSQL" /></Card>
      <Switch label="Collect monitoring metrics" checked={draft.monitoring?.enabled ?? true} onChange={(e) => setDraft({ ...draft, monitoring: { enabled: e.target.checked } })} />
    </fieldset>
    {editable ? <div className="save-bar"><span className="field-error grow">{errors[0]}</span><Button disabled={busy} onClick={() => { setDraft(structuredClone(cluster.spec)); setSeed(JSON.stringify(cluster.spec)); setAllAddresses(false); }}>Discard</Button><Button variant="primary" disabled={busy || errors.length > 0 || !Object.keys(patch).length} onClick={() => setConfirm(true)}>Review & apply</Button></div> : null}
    <ConfirmDialog open={confirm} onClose={() => setConfirm(false)} title="Apply PostgreSQL changes" confirmLabel="Apply" busy={busy} onConfirm={() => { setConfirm(false); void submit(patch, submission.key).then((c) => { setDraft(structuredClone(c.spec)); setSeed(JSON.stringify(c.spec)); setAllAddresses(false); submission.reset(); }, () => undefined); }}><Callout tone="warn">{consequences.join(' ')}</Callout><pre className="code">{JSON.stringify({ spec: patch }, null, 2)}</pre></ConfirmDialog>
  </div>;
}
