// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { WizardSectionProps } from '../types';
import { versionSelectable } from '../index';
import { Checkbox, ChoiceGroup, SelectField, Switch } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { buildEngineOptions, managementIsPublic, NODE_COUNTS, PLUGIN_GROUPS, quorumFailureTolerance, selectedPluginGroups } from './helpers';

export default function RabbitMQWizardSection({ provider, spec, onChange }: WizardSectionProps) {
  const selected = selectedPluginGroups(spec.engineOptions);
  const publicManagement = managementIsPublic(spec.engineOptions);
  const nodes = spec.components.engine.replicas;
  const tls = spec.tls ?? { mode: 'requireTLS' as const, ca: 'selfSigned' as const };
  return (
    <section className="stack" aria-label="RabbitMQ configuration">
      <div className="grid-2">
        <SelectField label="RabbitMQ version" value={spec.version} onChange={(event) => onChange({ ...spec, version: event.target.value })}>
          {provider.versions.map((version) => (
            <option key={version.version} value={version.version} disabled={!versionSelectable(provider.name, version)}>
              {version.version}{version.default ? ' (default)' : ''}{version.status !== 'supported' ? ` — ${version.status}` : ''}
            </option>
          ))}
        </SelectField>
        <SelectField label="RabbitMQ nodes" value={nodes} onChange={(event) => onChange({ ...spec, components: { ...spec.components, engine: { ...spec.components.engine, replicas: Number(event.target.value) } } })}>
          {NODE_COUNTS.map((count) => <option key={count} value={count}>{count}{count === 1 ? ' — no HA' : ` — tolerates ${quorumFailureTolerance(count)} node failure${count === 3 ? '' : 's'}`}</option>)}
        </SelectField>
      </div>
      <Callout tone={nodes === 1 ? 'warn' : 'info'} title={nodes === 1 ? 'Single node: no high availability' : 'Quorum queues need a majority of replicas'}>
        {nodes === 1 ? 'A node failure interrupts service.' : `A quorum queue replicated across ${nodes} nodes tolerates ${quorumFailureTolerance(nodes)} node failure${nodes === 3 ? '' : 's'} (floor((N − 1) / 2)).`}
        {' '}This applies to the queue’s replica count, not just cluster size. New quorum queues start with up to three replicas; five or seven cluster nodes do not automatically give every queue more replicas. Zerops does not guarantee separate-machine placement, so a machine failure can affect multiple nodes.
      </Callout>
      <ChoiceGroup name="rabbitmq-tls" legend="RabbitMQ TLS mode" value={tls.mode} onChange={(mode) => onChange({ ...spec, tls: { ...tls, mode } })} options={[
        { value: 'requireTLS', title: 'Require TLS', description: 'AMQPS only; encrypted management API and inter-node distribution.' },
        { value: 'preferTLS', title: 'Prefer TLS', description: 'AMQP and AMQPS listeners; plaintext clients are still accepted.' },
        { value: 'disabled', title: 'Disabled', description: 'Unencrypted AMQP and management API. Use only on trusted private networks.' },
      ]} />
      <Switch label="Publish the management UI over HTTPS" checked={publicManagement} disabled={tls.mode === 'requireTLS' && !publicManagement} onChange={(event) => onChange({ ...spec, engineOptions: { ...buildEngineOptions(selected, event.target.checked) } })} />
      {tls.mode === 'requireTLS' ? <Callout tone={publicManagement ? 'warn' : 'info'}>Public management is unavailable with requireTLS: Zerops HTTPS subdomains forward plaintext HTTP to brokers. Keep management private; secure public routing needs verified HTTPS backends or TLS TCP passthrough.</Callout> : publicManagement ? <Callout tone="warn">The management UI/API becomes publicly reachable through Zerops HTTPS routing, with a private plaintext HTTP hop, and requires RabbitMQ credentials. The AMQP IP allowlist does not protect this separate endpoint.</Callout> : null}
      <fieldset className="stack">
        <legend className="field-label">Optional plugins</legend>
        {PLUGIN_GROUPS.map((group) => <Checkbox key={group.id} label={group.label} description={`${group.description} Includes its management plugin.`} checked={selected.includes(group.id)} onChange={(event) => onChange({ ...spec, engineOptions: { ...buildEngineOptions(event.target.checked ? [...selected, group.id] : selected.filter((id) => id !== group.id), publicManagement) } })} />)}
      </fieldset>
      <p className="small muted">Management and Prometheus plugins are always enabled. Set the public AMQPS allowlist and certificate authority in Access &amp; TLS; set rabbitmq.conf overrides under Topology.</p>
    </section>
  );
}
