// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, CopyButton, ErrorBlock, KV, Loading, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { DEFAULT_TTL_HOURS, MAX_TTL_HOURS, certificateFiles, certificateState, cliExample, commonNameError, readOptions, shortFingerprint, ttlError, type CertificateState, type IssuedClientCertificate } from './spec';

const STATE_BADGE: Record<CertificateState, { tone: 'ok' | 'warn' | 'danger'; label: string; title: string }> = {
  valid: { tone: 'ok', label: 'Current CA', title: 'Issued by the CA currently used for new client certificates.' },
  'superseded-ca': { tone: 'danger', label: 'Invalid after CA rotation', title: 'The signing CA has changed. This certificate no longer authenticates after rotation completes; it may still work during the rolling trust overlap. Issue a new one.' },
  expired: { tone: 'warn', label: 'Expired', title: 'Past its notAfter time.' },
};

/** Browser download of generated text without touching storage or global state. */
function downloadFile(name: string, content: string) {
  const url = URL.createObjectURL(new Blob([content], { type: 'application/x-pem-file' }));
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  URL.revokeObjectURL(url);
}

export function CertificatesTab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const certificates = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'temporal-client-certificates'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/temporal/client-certificates', { params: { path: { cluster: name }, query: { namespace } } })),
  });
  const [issuing, setIssuing] = useState(false);
  const canIssue = can('read', 'cluster-credentials');
  const requireClientAuth = readOptions(cluster.spec).frontend.requireClientAuth;
  const items = [...(certificates.data?.items ?? [])].sort((a, b) => b.issuedAt.localeCompare(a.issuedAt));
  const now = Date.now();

  return (
    <div className="stack" style={{ gap: 16 }}>
      {requireClientAuth ? (
        <Callout tone="info" title="Mutual TLS on the frontend">
          SDK clients and the temporal CLI must present a certificate signed by this cluster’s CA. Private keys are returned exactly once at issue time and never stored by Karakoram. Certificates cannot be revoked individually: rotating TLS certificates replaces the CA and invalidates every certificate issued before it.
        </Callout>
      ) : (
        <Callout tone="warn" title="Client certificates are optional on this cluster">
          The frontend accepts TLS clients without a certificate (client auth is off). Certificates issued here still work and become mandatory once client auth is turned on under <Link to={`${base}/topology`}>Topology & scaling</Link>. Rotating TLS certificates replaces the CA and invalidates every certificate issued before it.
        </Callout>
      )}

      <Card
        title="Issued client certificates"
        flush
        actions={
          canIssue ? (
            <Button variant="primary" size="sm" onClick={() => setIssuing(true)}>
              Issue certificate…
            </Button>
          ) : (
            <span className="xs faint">Issuing requires cluster-credentials read permission.</span>
          )
        }
      >
        {certificates.isPending ? (
          <Loading />
        ) : certificates.error ? (
          <div className="card-body">
            <ErrorBlock error={certificates.error} retry={() => certificates.refetch()} />
          </div>
        ) : items.length === 0 ? (
          <div className="table-empty">No client certificates issued yet.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Common name</th>
                  <th>Serial</th>
                  <th>Issued</th>
                  <th>Expires</th>
                  <th>Fingerprint</th>
                  <th>Issuing CA</th>
                </tr>
              </thead>
              <tbody>
                {items.map((c) => {
                  const state = certificateState(c, now);
                  const badge = STATE_BADGE[state];
                  return (
                    <tr key={c.serial}>
                      <td className="mono">{c.commonName}</td>
                      <td className="mono small" title={c.serial}>{c.serial.length > 16 ? `${c.serial.slice(0, 16)}…` : c.serial}</td>
                      <td className="small"><Time iso={c.issuedAt} /></td>
                      <td className="small"><Time iso={c.notAfter} exact /></td>
                      <td className="mono small" title={c.fingerprintSha256}>{shortFingerprint(c.fingerprintSha256)}</td>
                      <td>
                        <span className="row wrap" style={{ gap: 6 }}>
                          <span className="mono small" title={c.caFingerprintSha256}>{shortFingerprint(c.caFingerprintSha256)}</span>
                          <Badge tone={badge.tone} title={badge.title}>{badge.label}</Badge>
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {issuing ? <IssueDialog namespace={namespace} cluster={name} onClose={() => setIssuing(false)} /> : null}
    </div>
  );
}

/**
 * Hands the one-time bundle to `receive` and resolves with non-secret metadata only, so the
 * TanStack mutation cache (which retains `state.data` after the dialog closes) never holds the key.
 */
export async function issueCertificate(post: () => Promise<IssuedClientCertificate>, receive: (issued: IssuedClientCertificate) => void): Promise<{ commonName: string; serial: string }> {
  const issued = await post();
  receive(issued);
  return { commonName: issued.commonName, serial: issued.serial };
}

function IssueDialog({ namespace, cluster, onClose }: { namespace: string; cluster: string; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [commonName, setCommonName] = useState('');
  const [ttlHours, setTtlHours] = useState(DEFAULT_TTL_HOURS);
  // The issued bundle lives only in this dialog's state; closing it discards the key for good.
  const [issued, setIssued] = useState<IssuedClientCertificate | null>(null);
  const submission = useSubmissionKey(JSON.stringify({ commonName, ttlHours }));
  const cnError = commonNameError(commonName);
  const ttlProblem = ttlError(ttlHours);
  const valid = !cnError && !ttlProblem;

  const issue = useMutation({
    mutationFn: () => issueCertificate(() => unwrap(api.POST('/clusters/{cluster}/temporal/client-certificates', { headers: idempotencyHeaders(submission.key), params: { path: { cluster }, query: { namespace } }, body: { commonName: commonName.trim(), ttlHours } })), setIssued),
    onSuccess: (result) => {
      submission.reset();
      toast.success(`Certificate for ${result.commonName} issued.`);
      void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, cluster, 'temporal-client-certificates') });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (issued) {
    const files = certificateFiles(issued);
    return (
      <Dialog open onClose={onClose} title="Client certificate issued — save it now" wide footer={<Button variant="primary" onClick={onClose}>Done, I have saved the key</Button>}>
        <div className="stack" style={{ gap: 14 }}>
          <Callout tone="warn" title="The private key cannot be retrieved again">
            Karakoram keeps only the certificate’s metadata. Download or copy the key, certificate and CA before closing this dialog; afterwards the only option is issuing a new certificate.
          </Callout>
          <KV
            items={[
              ['Common name', <span className="mono">{issued.commonName}</span>],
              ['Serial', <span className="mono">{issued.serial}</span>],
              ['Expires', <Time iso={issued.notAfter} exact />],
              ['Issuing CA', <span className="mono" title={issued.caFingerprintSha256}>{shortFingerprint(issued.caFingerprintSha256)}</span>],
            ]}
          />
          <div className="grid-2">
            <div className="stack" style={{ gap: 4 }}>
              <strong className="small">Frontend address</strong>
              <CodeLine value={issued.frontendAddress} />
            </div>
            <div className="stack" style={{ gap: 4 }}>
              <strong className="small">TLS server name</strong>
              <CodeLine value={issued.serverName} />
            </div>
          </div>
          <div className="stack" style={{ gap: 8 }}>
            {files.map((file) => (
              <div key={file.name} className="row-between wrap" style={{ gap: 8 }}>
                <span className="mono small">{file.name}</span>
                <span className="row" style={{ gap: 6 }}>
                  <CopyButton value={file.content} label={`Copy ${file.name.endsWith('.key') ? 'key' : 'PEM'}`} />
                  <Button size="sm" onClick={() => downloadFile(file.name, file.content)}>Download</Button>
                </span>
              </div>
            ))}
            <Button size="sm" style={{ alignSelf: 'flex-end' }} onClick={() => files.forEach((file) => downloadFile(file.name, file.content))}>Download all three files</Button>
          </div>
          <details>
            <summary className="small">temporal CLI example</summary>
            <CodeLine value={cliExample(issued)} />
          </details>
        </div>
      </Dialog>
    );
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title="Issue client certificate"
      busy={issue.isPending}
      error={issue.error}
      onSubmit={() => valid && issue.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={issue.isPending} disabled={!valid}>
            Issue certificate
          </Button>
        </>
      }
    >
      <div className="stack" style={{ gap: 12 }}>
        <TextField label="Common name" mono required autoFocus value={commonName} onChange={(e) => setCommonName(e.target.value)} placeholder="orders-service" hint="Identifies the client in Temporal logs; one certificate per workload is recommended." error={commonName && cnError ? cnError : undefined} />
        <TextField label="Validity (hours)" type="number" min={1} max={MAX_TTL_HOURS} step={1} style={{ maxWidth: 220 }} value={ttlHours} onChange={(e) => setTtlHours(Number(e.target.value))} hint={`Default ${DEFAULT_TTL_HOURS} hours (30 days); at most ${MAX_TTL_HOURS} hours (397 days).`} error={ttlProblem} />
        <p className="xs faint" style={{ margin: 0 }}>The private key is generated on the control plane, returned once in the response and never stored.</p>
      </div>
    </Dialog>
  );
}
