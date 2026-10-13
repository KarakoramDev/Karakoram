// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useOrphanedVolumes } from '@/api/hooks';
import { Callout, Card, ErrorBlock, Loading, Time } from '@/components/ui/Primitives';

export function OrphanedVolumesPage() {
  const volumes = useOrphanedVolumes();

  return (
    <>
      <Callout>
        <p>
          Deletion requests for these volumes failed after their runtimes were removed. Karakoram removed them from their clusters and keeps retrying deletion in the background with exponential backoff, capped at 1 hour. These volumes may still be billed by Zerops.
        </p>
        <p>
          You can delete them manually in the Zerops GUI or with <code>zcli service delete {'<hostname>'}</code>. The record disappears on the next retry after deletion.
        </p>
      </Callout>
      <Card title="Orphaned volumes" flush>
        {volumes.isPending ? (
          <Loading />
        ) : volumes.error ? (
          <div className="card-body">
            <ErrorBlock error={volumes.error} retry={() => volumes.refetch()} />
          </div>
        ) : volumes.data.items.length === 0 ? (
          <div className="table-empty">No orphaned volumes</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th scope="col">Hostname</th>
                  <th scope="col">Zerops service ID</th>
                  <th scope="col">Cluster</th>
                  <th scope="col">Member</th>
                  <th scope="col" className="num">Attempts</th>
                  <th scope="col">First error</th>
                  <th scope="col">Last error</th>
                  <th scope="col">First failed</th>
                  <th scope="col">Orphaned at</th>
                  <th scope="col">Last attempt</th>
                  <th scope="col">Next attempt</th>
                  <th scope="col">Last process ID</th>
                </tr>
              </thead>
              <tbody>
                {volumes.data.items.map((volume) => (
                  <tr key={volume.hostname}>
                    <td className="mono nowrap">{volume.hostname}</td>
                    <td className="mono small nowrap">{volume.serviceId}</td>
                    <td className="mono nowrap">{volume.cluster}</td>
                    <td className="mono nowrap">{volume.member}</td>
                    <td className="num">{volume.attempts}</td>
                    <td><VolumeError label="First error" message={volume.firstError} /></td>
                    <td><VolumeError label="Last error" message={volume.lastError} /></td>
                    <td className="small nowrap"><Time iso={volume.firstFailedAt} /></td>
                    <td className="small nowrap"><Time iso={volume.orphanedAt} /></td>
                    <td className="small nowrap"><Time iso={volume.lastAttemptAt} /></td>
                    <td className="small nowrap"><Time iso={volume.nextAttemptAt} /></td>
                    <td className="mono small nowrap">{volume.lastProcessId || <span className="faint">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </>
  );
}

function VolumeError({ label, message }: { label: string; message: string }) {
  if (!message) return <span className="faint">—</span>;

  return (
    <details className="orphaned-volume-error">
      <summary title={message} aria-label={`${label}: ${message}`}>{message}</summary>
      <p>{message}</p>
    </details>
  );
}
