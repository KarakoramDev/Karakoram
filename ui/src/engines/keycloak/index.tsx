// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Switch, TextField } from '@/components/ui/Field';
import type { EngineUI, WizardSectionProps } from '../types';
import { object } from '../postgres/spec';

function KeycloakOptions({ spec, onChange }: WizardSectionProps) {
  const options = object(spec.engineOptions);
  const database = object(options.database);
  const set = (key: string, value: unknown) => onChange({ ...spec, engineOptions: { ...options, [key]: value } });
  const db = (key: string, value: string) => set('database', { ...database, [key]: value });
  return <div className="stack">
    <TextField label="Source PostgreSQL cluster" value={String(database.cluster ?? '')} onChange={(e) => db('cluster', e.target.value)} />
    <TextField label="Source namespace (empty uses this namespace)" value={String(database.namespace ?? '')} onChange={(e) => db('namespace', e.target.value)} />
    <TextField label="Database name" value={String(database.name ?? 'keycloak')} onChange={(e) => db('name', e.target.value)} />
    <TextField label="Keycloak origin (for example https://login.example.com)" value={String(options.hostname ?? '')} onChange={(e) => set('hostname', e.target.value)} />
    <TextField label="Trusted proxy headers (forwarded or xforwarded)" value={String(options.proxyHeaders ?? '')} onChange={(e) => set('proxyHeaders', e.target.value)} />
    <Switch label="Public HTTPS routing" checked={options.publicHttps === true} onChange={(e) => set('publicHttps', e.target.checked)} />
    <p className="small muted">The source cluster must already be ready. Karakoram creates a dedicated owning login and database, then connects using TLS verify-full. Source administrator credentials never reach Keycloak. Only trust proxy headers from an ingress that overwrites them.</p>
    <p className="small muted">Keycloak serves HTTP on private port 8080 behind a TLS-terminating ingress. Management health on 9000 stays loopback-only. Bootstrap credentials are sealed; use the documented offline kc.sh bootstrap-admin recovery to establish your own administrator. Back up and restore through the source PostgreSQL cluster.</p>
  </div>;
}

export const keycloakUI: EngineUI = {
  label: 'Keycloak',
  description: 'Native identity service consuming a dedicated PostgreSQL binding.',
  wizardSection: KeycloakOptions,
  validateSpec: (spec) => typeof object(object(spec.engineOptions).database).cluster === 'string' && String(object(object(spec.engineOptions).database).cluster).trim() ? [] : ['Select a source PostgreSQL cluster.'],
  specSummary: () => 'Single Keycloak runtime · external PostgreSQL binding',
  topologySummary: () => 'One runtime; maintenance interrupts authentication',
  monitoringPanels: [{ title: 'Keycloak readiness', names: ['keycloakReady'], unit: '', yMax: 1 }],
};
