// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, KV } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { readOptions, replicasOf } from './spec';

export function WebUITab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const options = readOptions(cluster.spec);
  const ui = replicasOf(cluster.spec, 'ui');
  const endpoints = (cluster.status.connection?.endpoints ?? []).filter((e) => e.name === 'ui');
  const uiMembers = cluster.status.members.filter((m) => m.set === 'ui');
  const [secret, setSecret] = useState('');
  const seal = useMutation({
    mutationFn: () => unwrap(api.PUT('/clusters/{cluster}/temporal/ui/oidc-client-secret', { params: { path: { cluster: name }, query: { namespace } }, body: { clientSecret: secret } })),
    onSuccess: () => {
      setSecret('');
      toast.success('OIDC client secret stored; the Web UI is reconciling with it.');
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  return (
    <div className="stack" style={{ gap: 16 }}>
      <Card title="Web UI">
        {ui === 0 ? (
          <Callout tone="info">The Web UI is disabled. Enable it under <Link to={`${base}/topology`}>Topology & scaling</Link>.</Callout>
        ) : (
          <KV
            items={[
              ['Instances', `${ui} · ${uiMembers.filter((m) => m.health).length} healthy`],
              ['Access', options.ui.publicHttps ? <span><Badge tone="warn">Public HTTPS</Badge> <span className="small muted">OIDC login required</span></span> : <Badge plain>Private to the project network</Badge>],
              ['URL', endpoints.length ? <span className="stack" style={{ gap: 2 }}>{endpoints.map((e) => <a key={e.url} href={e.url} target="_blank" rel="noreferrer">{e.url}</a>)}</span> : options.ui.publicHttps ? <span className="faint">Public route not provisioned yet</span> : uiMembers[0] ? <span className="mono">http://{uiMembers[0].name}:8080</span> : undefined],
              options.ui.oidc && ['OIDC issuer', <span className="mono">{options.ui.oidc.issuer}</span>],
              options.ui.oidc && ['OIDC client ID', <span className="mono">{options.ui.oidc.clientId}</span>],
            ]}
          />
        )}
      </Card>

      <Card title="OIDC client secret">
        <div className="stack" style={{ gap: 12 }}>
          <p className="small muted" style={{ margin: 0 }}>
            The secret is sealed on the control plane and handed only to the Web UI process. It is never returned by the API or shown here, so this form cannot tell whether one is already stored — submitting replaces it.
          </p>
          {!options.ui.publicHttps ? <Callout tone="info">OIDC login applies only to a public Web UI. Enable public HTTPS and configure the issuer and client ID under <Link to={`${base}/topology`}>Topology & scaling</Link>; the secret can be stored ahead of time.</Callout> : null}
          {can('update') ? (
            <form
              className="stack"
              style={{ gap: 12 }}
              onSubmit={(e) => {
                e.preventDefault();
                if (secret.trim()) seal.mutate();
              }}
            >
              <TextField label="Client secret" type="password" autoComplete="off" mono value={secret} onChange={(e) => setSecret(e.target.value)} style={{ maxWidth: 480 }} hint="Write-only. Issued by your identity provider for the configured client ID." />
              <div className="row">
                <Button type="submit" variant="primary" loading={seal.isPending} disabled={!secret.trim()}>
                  Store client secret
                </Button>
                {secret ? <Button variant="ghost" onClick={() => setSecret('')}>Clear</Button> : null}
              </div>
            </form>
          ) : (
            <p className="xs faint" style={{ margin: 0 }}>Storing the secret requires cluster update permission.</p>
          )}
        </div>
      </Card>
    </div>
  );
}
