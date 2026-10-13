// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useClusters } from '@/api/hooks';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { ChoiceGroup, Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout, Card, Menu, MenuItem } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import { ResourcesEditor } from '../editors/ResourcesEditor';
import { ExposureEditor, TLSEditor } from '../editors/AccessEditor';
import { useClusterContext } from './ClusterDetail';
import { scalingReview } from './detail-state';
import { resourceMinimums, validateResources } from '../wizard/resources';
import { memberChoices } from '../wizard/validation';
import { validateExposure, validateTLS } from '../wizard/access';
import { useSubmissionKey } from './use-submission-key';
import { buildPatch, draftFrom, type Draft } from './topology';
import { MemberHealthBadge, MemberRole, useMemberRoles } from '../cluster-utils';
import { engines, publicExposureMetadata, replicaLimits, supportsOperation, validateReplicas } from '@/engines';

const RS_SIZES = [1, 3, 5, 7];


export function TopologyTab() {
  const { cluster, provider, namespace, name, base, can } = useClusterContext();
  const roles = useMemberRoles(cluster.status.members, base);
  const toast = useToast();
  const queryClient = useQueryClient();
  const clusters = useClusters(namespace);
  const topology = provider?.topologies.find((t) => t.type === cluster.spec.topology.type);
  const minimums = {
    engine: resourceMinimums(cluster.spec.provider, topology, 'engine'),
    configServer: resourceMinimums(cluster.spec.provider, topology, 'configServer'),
    router: resourceMinimums(cluster.spec.provider, topology, 'router'),
  };
  const engineChoices = memberChoices(topology, 'engine');
  const configChoices = memberChoices(topology, 'configServer');
  const engineSizes = engineChoices.length ? engineChoices : RS_SIZES;
  const configSizes = configChoices.length ? configChoices : RS_SIZES;
  const [draft, setDraft] = useState<Draft>(() => draftFrom(cluster.spec));
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [replaceMember, setReplaceMember] = useState<string | null>(null);
  const submission = useSubmissionKey(JSON.stringify(draft));
  const [allAddressesConfirmed, setAllAddressesConfirmed] = useState(false);

  // Re-seed when the persisted spec changes under us and the user has no edits.
  const persistedKey = JSON.stringify(cluster.spec);
  const [seedKey, setSeedKey] = useState(persistedKey);
  useEffect(() => {
    if (persistedKey !== seedKey && Object.keys(buildPatch(cluster.spec, draft)).length === 0) {
      setDraft(draftFrom(cluster.spec));
      setSeedKey(persistedKey);
    }
  }, [persistedKey, seedKey, cluster.spec, draft]);

  const sharded = cluster.spec.topology.type === 'sharded';
  const mongo = cluster.spec.provider === 'psmdb';
  const engineUI = engines[cluster.spec.provider];
  const limits = replicaLimits(topology, 'engine');
  const editable = can('update') && cluster.status.phase !== 'Deleting' && cluster.status.phase !== 'Suspended';
  const patch = useMemo(() => buildPatch(cluster.spec, draft), [cluster.spec, draft]);
  const dirty = Object.keys(patch).length > 0;

  const errors: string[] = [];
  if (mongo) {
    if (!engineSizes.includes(draft.engineReplicas)) errors.push(`Member count must be one of: ${engineSizes.join(', ')}.`);
  } else errors.push(...validateReplicas(draft.engineReplicas, limits));
  errors.push(...validateResources(draft.engineResources, cluster.spec.components.engine.resources, false, minimums.engine).map((e) => `Members: ${e}`));
  if (sharded) {
    if (draft.shards < (cluster.spec.topology.shards ?? 0)) errors.push('Shards cannot be removed.');
    if (!configSizes.includes(draft.configReplicas)) errors.push(`Config servers must be one of: ${configSizes.join(', ')}.`);
    if (draft.routerReplicas < 1) errors.push('At least one router is required.');
    if (draft.configResources) errors.push(...validateResources(draft.configResources, cluster.spec.components.configServer?.resources, false, minimums.configServer).map((e) => `Config servers: ${e}`));
    if (draft.routerResources) errors.push(...validateResources(draft.routerResources, undefined, true, minimums.router).map((e) => `Routers: ${e}`));
  }
  errors.push(...validateTLS(draft.tls, cluster.spec.tls?.ca ?? 'selfSigned'));
  if (patch.exposure) errors.push(...validateExposure(draft.exposure, allAddressesConfirmed));

  const save = useMutation({
    mutationFn: () => unwrap(api.PATCH('/clusters/{cluster}', { headers: { ...idempotencyHeaders(submission.key), 'Content-Type': 'application/merge-patch+json' }, params: { path: { cluster: name }, query: { namespace } }, body: { spec: patch } })),
    onSuccess: (c) => {
      submission.reset();
      setAllAddressesConfirmed(false);
      queryClient.setQueryData(keys.cluster(namespace, name), c);
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(<OperationNotice operationId={c.operationId}>Changes submitted; the cluster is reconciling.</OperationNotice>);
      setConfirmOpen(false);
      setDraft(draftFrom(c.spec));
      setSeedKey(JSON.stringify(c.spec));
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const replace = useMutation({
    mutationFn: (member: string) => unwrap(api.POST('/clusters/{cluster}/members/{member}/replace', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name, member }, query: { namespace } } })),
    onSuccess: (op) => {
      submission.reset();
      toast.success(<OperationNotice operationId={op.id}>Replacement started.</OperationNotice>);
      setReplaceMember(null);
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const disruptive = scalingReview(cluster.spec, patch);

  const members = cluster.status.members.filter((m) => (roles.get(m.name) ?? m.role) !== 'router');

  return (
    <div className="stack" style={{ gap: 16 }}>
      {!editable ? <Callout tone="info">{can('update') ? `Editing is unavailable while the cluster is ${cluster.status.phase}.` : 'You have read-only access to this cluster.'}</Callout> : null}
      {cluster.status.observedGeneration < cluster.generation ? <Callout tone="info">A previous change is still being reconciled (generation {cluster.status.observedGeneration} of {cluster.generation}).</Callout> : null}

      <fieldset disabled={!editable} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
        <Card title={sharded ? 'Shards and members' : 'Members'}>
          <div className="stack" style={{ gap: 16 }}>
            {sharded ? (
              <div className="grid-3">
                <TextField
                  label="Shards"
                  type="number"
                  min={cluster.spec.topology.shards ?? 1}
                  value={draft.shards}
                  onChange={(e) => setDraft((d) => ({ ...d, shards: Number(e.target.value) || d.shards }))}
                  hint="Add shards only; removal is not supported."
                />
                <TextField label="Routers" type="number" min={1} value={draft.routerReplicas} onChange={(e) => setDraft((d) => ({ ...d, routerReplicas: Number(e.target.value) || 1 }))} />
                <div className="field">
                  <span className="field-label">Config servers</span>
                  <div className="pill-group" role="group" aria-label="Config server members">
                    {configSizes.map((n) => (
                      <button key={n} type="button" aria-pressed={draft.configReplicas === n} onClick={() => setDraft((d) => ({ ...d, configReplicas: n }))}>
                        {n}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            ) : null}
            {!mongo && !limits.allowedReplicas?.length ? <TextField label="Nodes" type="number" min={limits.minReplicas} max={limits.maxReplicas} value={draft.engineReplicas} onChange={(e) => setDraft((d) => ({ ...d, engineReplicas: Number(e.target.value) }))} /> : <ChoiceGroup<string>
              name="replicas"
              legend={sharded ? 'Members per shard' : 'Members'}
              value={String(draft.engineReplicas)}
              onChange={(v) => setDraft((d) => ({ ...d, engineReplicas: Number(v) }))}
              options={(mongo ? engineSizes : limits.allowedReplicas ?? []).map((n) => ({
                value: String(n),
                title: `${n}`,
                description: mongo ? (n > cluster.spec.components.engine.replicas ? 'adds members (initial sync)' : n < cluster.spec.components.engine.replicas ? 'removes members (quorum-safe)' : 'current') : undefined,
              }))}
              hint={mongo ? 'Members are added one at a time and must complete initial sync before the next. Removal always keeps a voting majority.' : undefined}
            />}
          </div>
        </Card>

        <Card title={sharded ? 'Shard member resources' : 'Member resources'}>
          <ResourcesEditor minimums={minimums.engine} showErrors idPrefix="engine" value={draft.engineResources} persisted={cluster.spec.components.engine.resources} onChange={(r) => setDraft((d) => ({ ...d, engineResources: r }))} />
        </Card>
        {sharded && draft.configResources ? (
          <Card title="Config server resources">
            <ResourcesEditor minimums={minimums.configServer} showErrors idPrefix="cfg" value={draft.configResources} persisted={cluster.spec.components.configServer?.resources} onChange={(r) => setDraft((d) => ({ ...d, configResources: r }))} />
          </Card>
        ) : null}
        {sharded && draft.routerResources ? (
          <Card title="Router resources">
            <ResourcesEditor minimums={minimums.router} showErrors idPrefix="router" noDisk value={draft.routerResources} onChange={(r) => setDraft((d) => ({ ...d, routerResources: r }))} />
          </Card>
        ) : null}

        <Card title="Transport security">
          <TLSEditor storedCA={cluster.spec.tls?.ca ?? 'selfSigned'} value={draft.tls} onChange={(tls) => setDraft((d) => ({ ...d, tls }))} />
        </Card>
        <Card title="Network exposure">
          <ExposureEditor allAddressesConfirmed={allAddressesConfirmed} onAllAddressesConfirmedChange={setAllAddressesConfirmed} value={draft.exposure} onChange={(exposure) => setDraft((d) => ({ ...d, exposure }))} {...publicExposureMetadata(engineUI, { ...cluster.spec, tls: draft.tls, exposure: draft.exposure })} />
        </Card>
        <Card title="Monitoring & engine configuration">
          <div className="stack" style={{ gap: 14 }}>
            <Switch label="Collect per-member metrics" checked={draft.monitoring} onChange={(e) => setDraft((d) => ({ ...d, monitoring: e.target.checked }))} />
            <TextAreaField label={engineUI?.engineConfigLabel ?? (mongo ? 'Engine configuration overrides (YAML)' : 'Engine configuration overrides')} placeholder={engineUI?.engineConfigPlaceholder} mono rows={6} value={draft.engineConfig} onChange={(e) => setDraft((d) => ({ ...d, engineConfig: e.target.value }))} hint="Validated server-side; applied with a rolling restart." />
          </div>
        </Card>
      </fieldset>

      {editable ? (
        <div className="save-bar">
          {errors.length ? <span className="field-error grow">{errors[0]}</span> : dirty ? <span className="small muted grow">{Object.keys(patch).length} change set{Object.keys(patch).length > 1 ? 's' : ''} pending</span> : <span className="grow" />}
          <Button
            onClick={() => {
              setDraft(draftFrom(cluster.spec));
              setSeedKey(persistedKey);
            }}
            disabled={!dirty}
          >
            Discard
          </Button>
          <Button variant="primary" disabled={!dirty || errors.length > 0} onClick={() => setConfirmOpen(true)}>
            Review & apply
          </Button>
        </div>
      ) : null}

      {supportsOperation(provider, 'replace-member') ? <Card title="Replace a member" flush>
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Member</th>
                <th>Set</th>
                <th>Role</th>
                <th>Health</th>
                <th className="actions">Action</th>
              </tr>
            </thead>
            <tbody>
              {members.map((m) => (
                <tr key={m.name}>
                  <td className="mono">{m.name}</td>
                  <td className="mono">{m.set}</td>
                  <td><MemberRole member={m} roles={roles} /></td>
                  <td><MemberHealthBadge member={m} suspended={cluster.status.phase === 'Suspended'} /></td>
                  <td className="actions">
                    {editable ? (
                      <Menu>
                        <MenuItem onClick={() => setReplaceMember(m.name)} danger>
                          Replace with new member…
                        </MenuItem>
                      </Menu>
                    ) : null}
                  </td>
                </tr>
              ))}
              {members.length === 0 ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No members yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </Card> : null}

      <ConfirmDialog open={confirmOpen} onClose={() => { setConfirmOpen(false); save.reset(); submission.reset(); }} onConfirm={() => save.mutateAsync()} title="Apply topology changes" confirmLabel="Apply" busy={save.isPending} error={save.error}>
        <div className="stack">
          {disruptive.length ? (
            <Callout tone="warn" title="What will change">
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {disruptive.map((d) => (
                  <li key={d}>{d}</li>
                ))}
              </ul>
            </Callout>
          ) : null}
          <details>
            <summary className="small">Merge patch to be sent</summary>
            <pre className="code">{JSON.stringify({ spec: patch }, null, 2)}</pre>
          </details>
          {clusters.data && clusters.data.items.length > 1 && patch.exposure ? <p className="xs faint">Other clusters are unaffected.</p> : null}
        </div>
      </ConfirmDialog>

      <ConfirmDialog
        open={replaceMember !== null}
        onClose={() => { setReplaceMember(null); replace.reset(); submission.reset(); }}
        onConfirm={() => replaceMember && replace.mutateAsync(replaceMember)}
        title={`Replace ${replaceMember ?? ''}`}
        confirmLabel="Replace member"
        danger
        typeToConfirm={replaceMember ?? undefined}
        busy={replace.isPending}
        error={replace.error}
      >
        <p className="small">
          A new member with the next index is created and completes initial sync before <span className="mono">{replaceMember}</span> is removed from the set and its services and volume are deleted. If it is the primary, a
          step-down happens first.
        </p>
      </ConfirmDialog>
    </div>
  );
}
