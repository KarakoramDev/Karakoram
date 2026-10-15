// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Link } from 'react-router-dom';
import { Callout, Card, CopyButton } from '../../components/ui/Primitives';
import { useClusterContext } from '../../pages/clusters/detail/ClusterDetail';
import type { Schemas } from '../../api/client';

const guide = 'https://github.com/KarakoramDev/Karakoram/blob/main/docs/user-guide/rabbitmq.md#quorum-blocked-restarts-and-replica-growth';

export function QuorumRecoveryPanel({ cluster, base }: { cluster: Schemas['Cluster']; base: string }) {
  const conditions = cluster.status.conditions;
  const blockers = conditions.filter((condition) => condition.type === 'QuorumCriticalQueues' && condition.status === 'True');
  const recovery = conditions.filter((condition) => condition.type === 'QuorumReplicaRecovery' && condition.status === 'True');
  const leaving = cluster.status.members.filter((member) => member.state === 'LEAVING_BLOCKED');
  return <div className="stack">
    <Callout tone={blockers.length ? 'warn' : 'info'} title={blockers.length ? 'Rolling restart blocked by queue quorum' : 'Grow quorum queue replicas explicitly'}>
      {blockers.length ? blockers.map((condition, index) => <p key={index}>{condition.message}</p>) : 'No quorum-critical queues are currently reported. Adding brokers does not grow existing quorum queue groups.'}
      <p>Use the <a href={guide} target="_blank" rel="noreferrer">replica-growth procedure</a>. Native Management API changes require administrative RabbitMQ credentials and bypass Karakoram RBAC. They are not submitted as Karakoram operations.</p>
      <Link to={base}>Connection details and admin credential reveal</Link>
    </Callout>
    {recovery.map((condition, index) => <Card key={index} title="Targeted Management API commands" actions={<CopyButton value={condition.message ?? ''} label="Copy recovery procedure" />}>
      <p>Run commands one at a time from inside the project. Confirm an online majority first; verify voting membership and replica catch-up after every addition. Do not paste the entire procedure as a script.</p>
      <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{condition.message}</pre>
    </Card>)}
    {leaving.map((member) => <Callout key={member.name} tone="warn" title={`Removal blocked: ${member.name}`}>
      This broker is still present and leaving, not healthy. Inspect MemberRemovalBlocked and resolve the reported queue or survivor issue before removal can continue.
    </Callout>)}
  </div>;
}

export default function QuorumRecovery() {
  const { cluster, base } = useClusterContext();
  return <QuorumRecoveryPanel cluster={cluster} base={base} />;
}
