// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useMemo, useState } from 'react';
import type { Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Switch } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { ExposureEditor, TLSEditor } from '@/pages/clusters/editors/AccessEditor';
import { validateExposure, validateTLS, wireExposure } from '@/pages/clusters/wizard/access';
import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { detailProvider, reviewChanges } from '../index';
import type { TopologyEditorProps } from '../types';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { PUBLIC_PORT, componentOf, elasticsearchChangeReview, validateComponents, validateSpec } from './spec';

type ClusterSpec = Schemas['ClusterSpec'];

function draftFrom(spec: ClusterSpec): ClusterSpec {
  const draft = structuredClone(spec);
  draft.tls = { mode: spec.tls?.mode ?? 'requireTLS', ca: spec.tls?.ca ?? 'selfSigned' };
  draft.exposure = { public: spec.exposure?.public ?? false, ipType: spec.exposure?.ipType ?? 'IPV6', allowlist: [...(spec.exposure?.allowlist ?? [])] };
  return draft;
}

/** Topology & scaling editor: nodes, stack components, resources, engine options, TLS and exposure. */
export function TopologyEditor({ cluster, provider: catalog, editable, busy, submit }: TopologyEditorProps) {
  // detailProvider only yields undefined for psmdb; readers without catalog access get a stub with empty topologies.
  const provider = detailProvider(cluster.spec.provider, catalog)!;
  const [draft, setDraft] = useState<ClusterSpec>(() => draftFrom(cluster.spec));
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [allAddressesConfirmed, setAllAddressesConfirmed] = useState(false);
  const submission = useSubmissionKey(JSON.stringify(draft));

  // Re-seed when the persisted spec changes under us and the user has no edits.
  const persistedKey = JSON.stringify(cluster.spec);
  const [seedKey, setSeedKey] = useState(persistedKey);
  const { patch, consequences } = useMemo(() => reviewChanges(cluster.spec, draft, {
    ...elasticsearchChangeReview,
    buildPatch: (current, proposed) => {
      const result = elasticsearchChangeReview.buildPatch(current, proposed);
      if (result.exposure && proposed.exposure) result.exposure = wireExposure(proposed.exposure, allAddressesConfirmed);
      return result;
    },
  }), [cluster.spec, draft, allAddressesConfirmed]);
  const dirty = Object.keys(patch).length > 0;
  useEffect(() => {
    if (persistedKey !== seedKey && !dirty) {
      setDraft(draftFrom(cluster.spec));
      setSeedKey(persistedKey);
    }
  }, [persistedKey, seedKey, cluster.spec, dirty]);

  const topology = provider.topologies.find((t) => t.type === draft.topology.type);
  const components = validateComponents(draft, topology, (resources, stateless, name) => validateResources(resources, componentOf(cluster.spec, name)?.resources, stateless, resourceMinimums(draft.provider, topology, name)));
  const errors = [
    ...validateSpec(draft, cluster.spec),
    ...components.topology,
    ...components.resources,
    ...validateTLS(draft.tls, cluster.spec.tls?.ca),
    ...validateExposure(draft.exposure, !patch.exposure || allAddressesConfirmed),
  ];
  const discard = () => {
    setDraft(draftFrom(cluster.spec));
    setSeedKey(persistedKey);
    setConfirmOpen(false);
  };

  return (
    <>
      <fieldset disabled={!editable} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
        <Card title={topology?.displayName ?? (draft.topology.type === 'dedicatedMasters' ? 'Dedicated masters' : 'Cluster')}>
          <div className="stack" style={{ gap: 16 }}>
            <p className="small muted" style={{ margin: 0 }}>
              The topology type is fixed after creation. Nodes join as soon as they are provisioned; removals drain shards one node at a time and are blocked while the remaining data nodes cannot hold every replica.
            </p>
            <TopologySection provider={provider} spec={draft} persisted={cluster.spec} onChange={setDraft} />
          </div>
        </Card>
        <ResourcesSection provider={provider} spec={draft} persisted={cluster.spec} onChange={setDraft} />
        <Card title="Elastic Stack options">
          <OptionsSection provider={provider} spec={draft} persisted={cluster.spec} onChange={setDraft} />
        </Card>
        <Card title="Transport security">
          <TLSEditor storedCA={cluster.spec.tls?.ca} value={draft.tls ?? { mode: 'requireTLS', ca: 'selfSigned' }} onChange={(tls) => setDraft((d) => ({ ...d, tls }))} />
        </Card>
        <Card title="Network exposure">
          <ExposureEditor value={draft.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] }} onChange={(exposure) => setDraft((d) => ({ ...d, exposure }))} allAddressesConfirmed={allAddressesConfirmed} onAllAddressesConfirmedChange={setAllAddressesConfirmed} publicPort={PUBLIC_PORT} publicPortLabel="HTTPS" />
        </Card>
        <Card title="Monitoring">
          <Switch label="Collect per-member metrics" checked={draft.monitoring?.enabled ?? true} onChange={(e) => setDraft((d) => ({ ...d, monitoring: { enabled: e.target.checked } }))} />
        </Card>
      </fieldset>

      {editable ? (
        <div className="save-bar">
          {errors.length ? <span className="field-error grow">{errors[0]}</span> : dirty ? <span className="small muted grow">{Object.keys(patch).length} change set{Object.keys(patch).length > 1 ? 's' : ''} pending</span> : <span className="grow" />}
          <Button onClick={discard} disabled={!dirty}>
            Discard
          </Button>
          <Button variant="primary" disabled={!dirty || errors.length > 0} onClick={() => setConfirmOpen(true)}>
            Review & apply
          </Button>
        </div>
      ) : null}

      <ConfirmDialog
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => {
          setConfirmOpen(false);
          // Failures are reported by the shared mutation; the draft stays for another attempt.
          submit(patch, submission.key).then((c) => {
            setDraft(draftFrom(c.spec));
            setSeedKey(JSON.stringify(c.spec));
            submission.reset();
          }, () => undefined);
        }}
        title="Apply topology changes"
        confirmLabel="Apply"
        busy={busy}
      >
        <div className="stack">
          {consequences.length ? (
            <Callout tone="warn" title="What happens next">
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {consequences.map((d) => (
                  <li key={d}>{d}</li>
                ))}
              </ul>
            </Callout>
          ) : null}
          <details>
            <summary className="small">Merge patch to be sent</summary>
            <pre className="code">{JSON.stringify({ spec: patch }, null, 2)}</pre>
          </details>
        </div>
      </ConfirmDialog>
    </>
  );
}
