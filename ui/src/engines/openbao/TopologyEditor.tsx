// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { SelectField, Switch } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { ResourcesEditor } from '@/pages/clusters/editors/ResourcesEditor';
import { ExposureEditor, TLSEditor } from '@/pages/clusters/editors/AccessEditor';
import { validateExposure, validateTLS, wireExposure } from '@/pages/clusters/wizard/access';
import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import { memberChoices } from '@/pages/clusters/wizard/validation';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { detailProvider, reviewChanges } from '../index';
import type { TopologyEditorProps } from '../types';
import OpenBaoOptionsSection from './WizardSection';
import { API_PORT, MEMBER_COUNTS, failureTolerance, openbaoChangeReview, readOptions, validateOptions } from './spec';

export function TopologyEditor({ cluster, provider: catalog, editable, busy, submit }: TopologyEditorProps) {
  const provider = detailProvider(cluster.spec.provider, catalog)!;
  const [draft, setDraft] = useState(() => structuredClone(cluster.spec));
  const [seed, setSeed] = useState(() => JSON.stringify(cluster.spec));
  const [confirm, setConfirm] = useState(false);
  const [allAddresses, setAllAddresses] = useState(false);
  const submission = useSubmissionKey(JSON.stringify(draft));
  const { patch, consequences } = reviewChanges(cluster.spec, draft, {
    ...openbaoChangeReview,
    buildPatch: (current, proposed) => {
      const result = openbaoChangeReview.buildPatch(current, proposed);
      if (result.exposure && proposed.exposure) result.exposure = wireExposure(proposed.exposure, allAddresses);
      return result;
    },
  });
  const topology = provider.topologies.find((t) => t.type === draft.topology.type);
  const choices = memberChoices(topology, 'engine');
  const sizes = choices.length ? choices : [...MEMBER_COUNTS];
  const errors = [
    ...validateOptions(readOptions(draft), false),
    ...(sizes.includes(draft.components.engine.replicas) ? [] : [`Member count must be one of: ${sizes.join(', ')}.`]),
    ...validateResources(draft.components.engine.resources, cluster.spec.components.engine.resources, false, resourceMinimums(draft.provider, topology, 'engine')).map((e) => `Members: ${e}`),
    ...validateTLS(draft.tls, cluster.spec.tls?.ca),
    ...(patch.exposure ? validateExposure(draft.exposure, allAddresses) : []),
  ];
  if (draft.tls?.mode && draft.tls.mode !== 'requireTLS') errors.push('OpenBao always requires TLS.');
  if (seed !== JSON.stringify(cluster.spec)) errors.push('The cluster specification changed. Discard to load the current specification before applying changes.');
  const members = draft.components.engine.replicas;

  return <div className="stack">
    <fieldset disabled={!editable || busy} className="stack" style={{ border: 0, margin: 0, padding: 0 }}>
      <Card title="Raft members">
        <SelectField label="Members" value={members} onChange={(e) => setDraft({ ...draft, components: { ...draft.components, engine: { ...draft.components.engine, replicas: Number(e.target.value) } } })} hint={readOptions(draft).storage.type === 'postgresql' ? 'Stateless HA peers on PostgreSQL storage; any count keeps the data.' : members === 1 ? 'A single Raft member has no redundancy.' : `Tolerates ${failureTolerance(members)} member failure${members === 3 ? '' : 's'}; members are added before any is removed.`}>
          {sizes.map((n) => <option key={n} value={n}>{n}</option>)}
        </SelectField>
      </Card>
      <Card title={`Member resources · ${members} replicas`}>
        <ResourcesEditor idPrefix="openbao-engine" value={draft.components.engine.resources} persisted={cluster.spec.components.engine.resources} minimums={resourceMinimums(draft.provider, topology, 'engine')} onChange={(resources) => setDraft({ ...draft, components: { ...draft.components, engine: { ...draft.components.engine, resources } } })} />
      </Card>
      <Card title="Engine options"><OpenBaoOptionsSection provider={provider} namespace={cluster.namespace} spec={draft} onChange={setDraft} locked /></Card>
      <Card title="TLS"><TLSEditor storedCA={cluster.spec.tls?.ca} value={draft.tls ?? { mode: 'requireTLS', ca: 'selfSigned' }} onChange={(tls) => setDraft({ ...draft, tls })} /></Card>
      <Card title="API exposure"><ExposureEditor value={draft.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] }} onChange={(exposure) => setDraft({ ...draft, exposure })} allAddressesConfirmed={allAddresses} onAllAddressesConfirmedChange={setAllAddresses} publicPort={API_PORT} publicPortLabel="HTTPS API" /></Card>
      <Switch label="Collect monitoring metrics" checked={draft.monitoring?.enabled ?? true} onChange={(e) => setDraft({ ...draft, monitoring: { enabled: e.target.checked } })} />
    </fieldset>
    {editable ? <div className="save-bar"><span className="field-error grow">{errors[0]}</span><Button disabled={busy} onClick={() => { setDraft(structuredClone(cluster.spec)); setSeed(JSON.stringify(cluster.spec)); setAllAddresses(false); }}>Discard</Button><Button variant="primary" disabled={busy || errors.length > 0 || !Object.keys(patch).length} onClick={() => setConfirm(true)}>Review & apply</Button></div> : null}
    <ConfirmDialog open={confirm} onClose={() => setConfirm(false)} title="Apply OpenBao changes" confirmLabel="Apply" busy={busy} onConfirm={() => { setConfirm(false); void submit(patch, submission.key).then((c) => { setDraft(structuredClone(c.spec)); setSeed(JSON.stringify(c.spec)); setAllAddresses(false); submission.reset(); }, () => undefined); }}>
      {consequences.length ? <Callout tone="warn">{consequences.join(' ')}</Callout> : null}
      <pre className="code">{JSON.stringify({ spec: patch }, null, 2)}</pre>
    </ConfirmDialog>
  </div>;
}
