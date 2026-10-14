// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Checkbox } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from './ClusterDetail';
import { OperationNotice } from '@/components/OperationNotice';
import { operationPath } from '@/components/operations';

export function DeleteTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const navigate = useNavigate();
  const toast = useToast();
  const queryClient = useQueryClient();
  const [keepVolumes, setKeepVolumes] = useState(false);
  const [keepBackups, setKeepBackups] = useState(true);
  const [open, setOpen] = useState(false);

  const del = useMutation({
    mutationFn: () => unwrap(api.DELETE('/clusters/{cluster}', { params: { path: { cluster: name }, query: { namespace, keepVolumes, keepBackups } } })),
    onSuccess: (operation) => {
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
      toast.success(<OperationNotice operationId={operation.id}>{keepVolumes ? `${name} is being suspended; volumes retained.` : `${name} is being deleted.`}</OperationNotice>);
      setOpen(false);
      navigate(operationPath(operation.id));
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (!can('delete')) {
    return <Callout tone="info">You do not have permission to delete this cluster.</Callout>;
  }
  const deleting = cluster.status.phase === 'Deleting';
  const suspended = cluster.status.phase === 'Suspended';
  const backupCount = cluster.spec.backup?.storage ? 'backups on ' + cluster.spec.backup.storage : 'no backup storage configured';

  return (
    <div className="stack" style={{ gap: 16 }}>
      <Card title={suspended ? 'Delete suspended cluster' : 'Delete cluster'}>
        <div className="stack" style={{ gap: 14 }}>
          <Callout tone="danger" title="Destructive action">
            Deleting removes every member runtime service for <span className="mono">{name}</span>. Choose what to keep below; the defaults keep backups and remove data volumes.
          </Callout>
          <Checkbox
            label="Keep data volumes (suspend instead of delete)"
            description="Runtime services are removed but volumes, cluster secrets and metadata stay. The cluster shows as Suspended and can be resumed later. Volumes continue to incur storage cost."
            checked={keepVolumes}
            disabled={suspended}
            onChange={(e) => setKeepVolumes(e.target.checked)}
          />
          <Checkbox
            label="Keep backups in object storage"
            description={`Backup data and PITR oplog slices remain (${backupCount}). Unchecking queues deletion of all backups for this cluster.`}
            checked={keepBackups}
            onChange={(e) => setKeepBackups(e.target.checked)}
          />
          <div>
            <Button variant="danger" disabled={deleting} onClick={() => setOpen(true)}>
              {keepVolumes && !suspended ? 'Suspend cluster…' : 'Delete cluster…'}
            </Button>
          </div>
        </div>
      </Card>
      <ConfirmDialog
        open={open}
        onClose={() => { setOpen(false); del.reset(); }}
        onConfirm={() => del.mutateAsync()}
        title={keepVolumes && !suspended ? `Suspend ${name}` : `Delete ${name}`}
        confirmLabel={keepVolumes && !suspended ? 'Suspend' : 'Delete permanently'}
        danger
        typeToConfirm={name}
        busy={del.isPending}
        error={del.error}
      >
        <ul className="small" style={{ margin: 0, paddingLeft: 18 }}>
          <li>Runtime services: removed</li>
          <li>Data volumes: {keepVolumes ? 'kept (resumable)' : 'deleted'}</li>
          <li>Backups: {keepBackups ? 'kept' : 'deleted from storage'}</li>
          <li>Database users, TLS material, system credentials: {keepVolumes ? 'kept' : 'deleted'}</li>
        </ul>
      </ConfirmDialog>
    </div>
  );
}
