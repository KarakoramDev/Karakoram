// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useMemo, useState } from 'react';
import type { Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Switch } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { engineRules } from '@/pages/clusters/wizard/engine';
import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { detailProvider, reviewChanges } from '../index';
import type { TopologyEditorProps } from '../types';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { componentOf, temporalChangeReview, validateComponents, validateSpec } from './spec';

type ClusterSpec = Schemas['ClusterSpec'];

/** Topology & scaling editor: instance counts, resources, mutable Temporal options and monitoring. TLS and exposure are provider-managed. */
export function TopologyEditor({ cluster, provider: catalog, editable, busy, submit }: TopologyEditorProps) {
  // detailProvider only yields undefined for psmdb; readers without catalog access get a stub with empty topologies.
  const provider = detailProvider(cluster.spec.provider, catalog)!;
  const [draft, setDraft] = useState<ClusterSpec>(() => structuredClone(cluster.spec));
  const [confirmOpen, setConfirmOpen] = useState(false);
  const submission = useSubmissionKey(JSON.stringify(draft));

  // Re-seed when the persisted spec changes under us and the user has no edits.
  const persistedKey = JSON.stringify(cluster.spec);
  const [seedKey, setSeedKey] = useState(persistedKey);
  const { patch, consequences } = useMemo(() => reviewChanges(cluster.spec, draft, temporalChangeReview), [cluster.spec, draft]);
  const dirty = Object.keys(patch).length > 0;
  useEffect(() => {
    if (persistedKey !== seedKey && !dirty) {
      setDraft(structuredClone(cluster.spec));
      setSeedKey(persistedKey);
    }
  }, [persistedKey, seedKey, cluster.spec, dirty]);

  const topology = provider.topologies.find((t) => t.type === draft.topology.type);
  const components = validateComponents(draft, topology, (resources, stateless, name) => validateResources(resources, componentOf(cluster.spec, name)?.resources, stateless, resourceMinimums(draft.provider, topology, name)));
  const errors = [...validateSpec(draft, cluster.spec), ...components.topology, ...components.resources];
  const discard = () => {
    setDraft(structuredClone(cluster.spec));
    setSeedKey(persistedKey);
    setConfirmOpen(false);
  };

  return (
    <>
      <fieldset disabled={!editable} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
        <Card title={engineRules(draft.provider).topology[draft.topology.type]?.title ?? topology?.displayName ?? 'Temporal topology'}>
          <div className="stack" style={{ gap: 16 }}>
            <p className="small muted" style={{ margin: 0 }}>
              The topology type, PostgreSQL persistence and history shard count are fixed after creation. Instances are stateless and roll one at a time in the order history, matching, frontend, worker, server, Web UI.
            </p>
            <TopologySection namespace={cluster.namespace} provider={provider} spec={draft} persisted={cluster.spec} onChange={setDraft} />
          </div>
        </Card>
        <ResourcesSection namespace={cluster.namespace} provider={provider} spec={draft} persisted={cluster.spec} onChange={setDraft} />
        <Card title="Temporal options">
          <OptionsSection namespace={cluster.namespace} provider={provider} spec={draft} persisted={cluster.spec} clusterName={cluster.name} legend={false} onChange={setDraft} />
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
            setDraft(structuredClone(c.spec));
            setSeedKey(JSON.stringify(c.spec));
            submission.reset();
          }, () => undefined);
        }}
        title="Apply Temporal changes"
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
