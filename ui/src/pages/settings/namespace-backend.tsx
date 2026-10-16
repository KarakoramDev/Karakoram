// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { SelectField, TextField } from '@/components/ui/Field';
import { Badge, KV } from '@/components/ui/Primitives';
import type { Backend, NamespaceForm } from './namespace-form';

const CAPABILITY_LABELS = {
  publicTcp: 'Public TCP',
  tcpAllowlist: 'TCP allowlist',
  publicHttps: 'Public HTTPS',
  diskGrowth: 'Volume growth',
  volumeQuota: 'Volume quota',
  placement: 'Anti-affinity placement',
  exec: 'Exec',
  logs: 'Platform logs',
  metrics: 'Platform metrics',
  scopedTokens: 'Scoped tokens',
  dedicatedCpu: 'Dedicated CPU',
  horizontalScaling: 'Horizontal scaling',
  envChangeRestarts: 'Env change restarts',
} satisfies Record<Schemas['Capability'], string>;

export function BackendLabel({ kind, backends }: { kind: string; backends: Backend[] }) {
  const backend = backends.find((item) => item.kind === kind);
  return (
    <span className="row wrap" style={{ gap: 4 }}>
      {backend?.displayName || kind}
      {backend?.experimental ? <Badge tone="warn" plain>experimental</Badge> : null}
    </span>
  );
}

export function NamespaceBackendCell({ kind, backends }: { kind: string; backends: Backend[] }) {
  return <td className="small"><BackendLabel kind={kind} backends={backends} /></td>;
}

export function NamespaceCapabilities({ capabilities }: { capabilities: Schemas['Capability'][] }) {
  return (
    <KV items={Object.entries(CAPABILITY_LABELS).map(([capability, label]) => {
      const supported = capabilities.includes(capability as Schemas['Capability']);
      return [label, <Badge tone={supported ? 'ok' : 'neutral'} plain>{supported ? 'Supported' : 'Not supported'}</Badge>];
    })} />
  );
}

export function NamespaceBackendFields({ backends, backend, form, onChange }: {
  backends: Backend[];
  backend: Backend | undefined;
  form: NamespaceForm;
  onChange: (patch: Partial<NamespaceForm>) => void;
}) {
  const zerops = backend?.kind === 'zerops';
  return (
    <>
      {backends.length === 1 ? (
        <TextField label="Backend" value={`${backend?.displayName || backend?.kind}${backend?.experimental ? ' (experimental)' : ''}`} readOnly />
      ) : (
        <SelectField label="Backend" required value={backend?.kind ?? ''} onChange={(e) => onChange({ backend: e.target.value, config: {} })}>
          <option value="" disabled>Select a backend</option>
          {backends.map((item) => <option key={item.kind} value={item.kind}>{item.displayName || item.kind}{item.experimental ? ' (experimental)' : ''}</option>)}
        </SelectField>
      )}
      {backend?.experimental ? <Badge tone="warn" plain>experimental</Badge> : null}
      <TextField label={zerops ? 'Zerops project ID' : 'Target ID'} mono required value={form.projectId} onChange={(e) => onChange({ projectId: e.target.value })} hint={zerops ? 'Each project can belong to only one namespace.' : 'The backend target for this namespace.'} autoComplete="off" />
      <TextField label={zerops ? 'Project token' : 'API token'} type="password" required value={form.token} onChange={(e) => onChange({ token: e.target.value })} hint={zerops ? 'A project-scoped Zerops access token. Validated against the project before anything is saved; sealed at rest and never returned.' : 'An API token for the selected backend. Sealed at rest and never returned.'} autoComplete="new-password" />
      {backend?.configKeys.map((key) => (
        <TextField key={`${backend.kind}:${key}`} label={`${key} (optional)`} value={form.config[key] ?? ''} onChange={(e) => onChange({ config: { ...form.config, [key]: e.target.value } })} autoComplete="off" />
      ))}
    </>
  );
}
