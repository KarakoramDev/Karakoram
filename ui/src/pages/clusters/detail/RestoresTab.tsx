// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { useNamespaceOptions } from '@/api/hooks';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button, LinkButton } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { ChoiceGroup, TextField } from '@/components/ui/Field';
import { Callout, Card, ErrorBlock, Loading, Time, formatTime } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import { BackupPicker, PitrPicker, SourceClusterPicker } from '../ClusterWizard';
import { OpPhaseBadge } from '../cluster-utils';
import { useClusterContext } from './ClusterDetail';
import { useSubmissionKey } from './use-submission-key';
import { engines, hasCapability, supportsOperation, type EngineUI, type RestoreExtras } from '@/engines';
import { useLocalRequest } from '../use-local-request';

export function RestoresTab() {
  const { cluster, provider, namespace, name, base, can } = useClusterContext();
  const [params, setParams] = useSearchParams();
  const restores = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'restores'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/restores', { params: { path: { cluster: name }, query: { namespace, limit: 200 } } })),
    refetchInterval: 15_000,
  });
  const [open, setOpen] = useState(params.has('backup') || params.has('pitr'));
  const canCreate = can('create', 'restores') && supportsOperation(provider, 'restore');
  const inFlight = restores.data?.items.some((r) => r.phase === 'Pending' || r.phase === 'Running');

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="row-between wrap">
        <p className="small muted">
          In-place restores replace the data of <span className="mono">{name}</span>. To keep this cluster untouched, restore into a new cluster instead.
        </p>
        <div className="row">
          {can('create') ? (
            <LinkButton to={`/clusters/new?namespace=${encodeURIComponent(namespace)}&from=${encodeURIComponent(name)}`} size="md">
              Restore into new cluster
            </LinkButton>
          ) : null}
          {canCreate ? (
            <Button variant="primary" onClick={() => setOpen(true)} disabled={cluster.status.phase === 'Restoring' || inFlight}>
              Restore in place…
            </Button>
          ) : null}
        </div>
      </div>

      <Card title="Restore history" flush>
        {restores.isPending ? (
          <Loading />
        ) : restores.error ? (
          <div className="card-body">
            <ErrorBlock error={restores.error} retry={() => restores.refetch()} />
          </div>
        ) : restores.data.items.length === 0 ? (
          <div className="table-empty">No restores have been performed on this cluster.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phase</th>
                  <th>Source</th>
                  <th>Started</th>
                  <th>Completed</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {restores.data.items.map((r) => (
                  <tr key={r.name}>
                    <td className="mono">{r.name}</td>
                    <td>
                      <OpPhaseBadge phase={r.phase} />
                    </td>
                    <td className="small">
                      {r.source.sourceCluster !== name ? <span className="mono">{r.source.sourceCluster} · </span> : null}
                      {r.source.backup ? <span className="mono">backup {r.source.backup}</span> : <span>point in time {formatTime(r.source.pitrTime)}</span>}
                    </td>
                    <td className="small">
                      <Time iso={r.startedAt} />
                    </td>
                    <td className="small">
                      <Time iso={r.completedAt} />
                    </td>
                    <td className="small" style={{ color: 'var(--danger)' }}>
                      {r.error ?? ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {open && supportsOperation(provider, 'restore') ? (
        <RestoreDialog
          namespace={namespace}
          cluster={name}
          base={base}
          initialBackup={params.get('backup') ?? undefined}
          pitr={hasCapability(provider, 'pitr')}
          mongo={cluster.spec.provider === 'psmdb'}
          postgres={cluster.spec.provider === 'postgres'}
          engine={engines[cluster.spec.provider]}
          onClose={() => {
            setOpen(false);
            if (params.has('backup') || params.has('pitr')) setParams({}, { replace: true });
          }}
        />
      ) : null}
    </div>
  );
}

function RestoreDialog({ namespace, cluster, base, initialBackup, pitr, mongo, postgres, engine, onClose }: { namespace: string; cluster: string; base: string; initialBackup?: string; pitr: boolean; mongo: boolean; postgres: boolean; engine: EngineUI | undefined; onClose: () => void }) {
  const toast = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const namespaceOptions = useNamespaceOptions();
  const [mode, setMode] = useState<'backup' | 'pitr'>('backup');
  const [sourceNamespace, setSourceNamespace] = useState(namespace);
  const [sourceCluster, setSourceCluster] = useState(cluster);
  const [backup, setBackup] = useState(initialBackup ?? '');
  const [pitrTime, setPitrTime] = useState('');
  const [name, setName] = useState(() => `restore-${new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '')}`);
  const [acknowledgeBoundConsumers, setAcknowledgeBoundConsumers] = useState(false);
  const bindings = useQuery({
    queryKey: keys.clusterSub(namespace, cluster, 'postgres-bindings'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/postgres/bindings', { params: { path: { cluster }, query: { namespace } } })),
    enabled: postgres,
  });
  // Write-only engine request fields (e.g. OpenBao unseal shares); dialog state only, sent once.
  const [extras, setExtras] = useState<RestoreExtras>({});
  const submission = useSubmissionKey(JSON.stringify({ name, sourceNamespace, sourceCluster, mode, backup, pitrTime, acknowledgeBoundConsumers }));
  useEffect(() => {
    if (initialBackup) setBackup(initialBackup);
  }, [initialBackup]);

  const cross = sourceNamespace !== namespace;
  const create = useLocalRequest({
    request: () =>
      unwrap(
        api.POST('/clusters/{cluster}/restores', {
          headers: idempotencyHeaders(submission.key),
          params: { path: { cluster }, query: { namespace } },
          body: {
            name,
            acknowledgeBoundConsumers,
            ...(cross ? { sourceNamespace } : {}),
            source: { sourceCluster, ...(mode === 'backup' ? { backup } : { pitrTime }) },
            ...extras,
          },
        }),
      ),
    onSuccess: (r) => {
      submission.reset();
      toast.success(<OperationNotice operationId={r.operationId}>Restore {r.name} started.</OperationNotice>);
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, cluster) });
      onClose();
      navigate(`${base}/operations`);
    },
    onSettled: () => { setExtras({}); },
  });

  const RestoreSection = engine?.restoreSection;
  const extraErrors = engine?.validateRestore?.(extras) ?? [];
  const valid = /^[a-z0-9][a-z0-9-]{0,62}$/.test(name) && sourceCluster !== '' && (mode === 'backup' ? backup !== '' : pitrTime !== '') && (!postgres || (!bindings.isPending && !bindings.error && (!bindings.data?.items.length || acknowledgeBoundConsumers))) && extraErrors.length === 0;

  return (
    <Dialog
      open
      onClose={onClose}
      title="Restore in place"
      wide
      busy={create.isPending}
      error={create.error}
      onSubmit={() => valid && create.run()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="danger" loading={create.isPending} disabled={!valid}>
            Restore and overwrite data
          </Button>
        </>
      }
    >
      <Callout tone="danger" title="Current data will be replaced">
        {mongo ? 'The cluster becomes unavailable while the restore runs. Members are stopped (physical) or data is dropped and reloaded (logical). Take a fresh backup first if you may need the current state.' : 'The cluster becomes unavailable while the engine restores the backup. Take a fresh backup first if you may need the current state.'}
      </Callout>
      {postgres && bindings.error ? <ErrorBlock error={bindings.error} retry={() => bindings.refetch()} /> : null}
      {bindings.data?.items.length ? <Callout tone="warn" title="Bound consumers will be affected">
        <ul>{bindings.data.items.map((b) => <li key={b.id}>{b.consumer.kind}/{b.consumer.namespace}/{b.consumer.name}</li>)}</ul>
        <label><input type="checkbox" checked={acknowledgeBoundConsumers} onChange={(e) => setAcknowledgeBoundConsumers(e.target.checked)} required /> I acknowledge that restoring this source rolls back consumer data and can invalidate credentials.</label>
      </Callout> : null}
      <TextField label="Restore name" mono value={name} onChange={(e) => setName(e.target.value)} required />
      <SourceClusterPicker
        targetNamespace={namespace}
        namespaces={namespaceOptions}
        namespace={sourceNamespace}
        cluster={sourceCluster}
        onChange={(ns, c) => {
          setSourceNamespace(ns);
          setSourceCluster(c);
          setBackup('');
          setPitrTime('');
          create.reset();
        }}
      />
      <ChoiceGroup<'backup' | 'pitr'>
        name="restore-mode"
        legend="Restore point"
        value={mode}
        onChange={setMode}
        options={[
          { value: 'backup', title: 'From backup', description: 'Exact state of a completed backup.' },
          ...(pitr ? [{ value: 'pitr' as const, title: 'Point in time', description: 'Any instant within the PITR window.' }] : []),
        ]}
      />
      {mode === 'backup' ? <BackupPicker namespace={sourceNamespace} cluster={sourceCluster} value={backup} onChange={setBackup} /> : <PitrPicker namespace={sourceNamespace} cluster={sourceCluster} value={pitrTime} onChange={setPitrTime} />}
      {RestoreSection ? <RestoreSection source={sourceCluster ? { namespace: sourceNamespace, cluster: sourceCluster } : undefined} inPlace={sourceCluster === cluster && sourceNamespace === namespace} value={extras} onChange={(next) => { setExtras(next); create.reset(); }} showErrors /> : null}
    </Dialog>
  );
}
