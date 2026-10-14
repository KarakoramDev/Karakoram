// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Link } from 'react-router-dom';
import { useClusters } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';
import { spansNamespaces, useNamespace } from '@/namespaces/useNamespace';
import { Badge, Card, CodeLine, KV } from '@/components/ui/Primitives';
import { clusterPath } from '../clusters/detail/ClusterDetail';

export function MonitoringSettingsPage() {
  const auth = useAuth();
  const { namespace } = useNamespace();
  const clusters = useClusters(namespace);
  const showNamespace = !namespace && spansNamespaces(clusters.data?.items ?? []);
  const origin = window.location.origin;
  const scrapeURL = `${origin}/metrics`;
  const enabled = (clusters.data?.items ?? []).filter((c) => c.spec.monitoring?.enabled !== false);
  const disabled = (clusters.data?.items ?? []).filter((c) => c.spec.monitoring?.enabled === false);

  const scrapeConfig = `scrape_configs:
  - job_name: karakoram
    scheme: ${origin.startsWith('https') ? 'https' : 'http'}
    metrics_path: /metrics
    authorization:
      credentials_file: /etc/prometheus/karakoram-token   # kk_... API token with monitoring:read
    static_configs:
      - targets: ["${window.location.host}"]`;

  return (
    <>
      <Card title="Prometheus endpoint">
        <div className="stack">
          <p className="small muted">
            Karakoram's node agents collect engine metrics from every member and the control plane aggregates them. Per-cluster dashboards live under each cluster's Monitoring tab; external systems scrape all clusters at once from the endpoint below using
            an API token. Only clusters the token's owner may read (<code>monitoring:read</code>) are included.
          </p>
          <KV items={[['Endpoint', <CodeLine value={scrapeURL} />], ['Auth', 'Authorization: Bearer <API token> (create under API tokens)'], ['Format', 'Prometheus text exposition; labels namespace, cluster, member. Operation/document series also use operation; network series use direction.'], ['Filter', 'Optional ?namespace=<name> limits the scrape to one namespace']]} />
          <div className="section-title">Example scrape config</div>
          <pre className="code">{scrapeConfig}</pre>
          <p className="xs faint">
            Retention of in-UI samples is bounded by the control plane; point Prometheus/VictoriaMetrics at the endpoint for long-term storage and alerting. Need a token? <Link to="/settings/api-tokens">Create one</Link>.
          </p>
        </div>
      </Card>
      <Card title="Collection status" flush>
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                {showNamespace ? <th>Namespace</th> : null}
                <th>Cluster</th>
                <th>Metrics</th>
                <th>Members reporting</th>
              </tr>
            </thead>
            <tbody>
              {[...enabled, ...disabled].map((c) => (
                <tr key={`${c.namespace || DEFAULT_NAMESPACE}/${c.name}`}>
                  {showNamespace ? <td className="mono small">{c.namespace || DEFAULT_NAMESPACE}</td> : null}
                  <td>
                    <Link to={`${clusterPath(c.namespace || DEFAULT_NAMESPACE, c.name)}/monitoring`} className="mono strong">
                      {c.name}
                    </Link>
                  </td>
                  <td>
                    <Badge tone={c.spec.monitoring?.enabled === false ? 'neutral' : 'ok'} plain>
                      {c.spec.monitoring?.enabled === false ? 'disabled' : 'enabled'}
                    </Badge>
                  </td>
                  <td className="small">
                    {c.status.members.filter((m) => m.agent?.connected).length} / {c.status.members.length} agents connected
                  </td>
                </tr>
              ))}
              {(clusters.data?.items ?? []).length === 0 ? (
                <tr>
                  <td colSpan={showNamespace ? 4 : 3} className="table-empty">
                    {auth.can('clusters', 'read') ? 'No clusters.' : 'No clusters visible to you.'}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </Card>
    </>
  );
}
