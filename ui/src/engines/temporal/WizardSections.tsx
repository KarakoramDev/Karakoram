// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { errorMessage } from '@/api/client';
import { useClusters, useNamespaceOptions } from '@/api/hooks';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';
import { Button } from '@/components/ui/Button';
import { SelectField, Switch, TextField } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { ResourcesEditor } from '@/pages/clusters/editors/ResourcesEditor';
import { resourceMinimums } from '@/pages/clusters/wizard/resources';
import type { WizardSectionProps } from '../types';
import {
  COMPONENT_DESCRIPTION,
  COMPONENT_LABEL,
  DEFAULT_HISTORY_SHARDS,
  DEFAULT_RETENTION,
  DOCS_URL,
  MAX_HISTORY_SHARDS,
  MIN_POSTGRES_MAJOR,
  activeComponents,
  componentOf,
  defaultDatabase,
  defaultVisibilityDatabase,
  formatRetention,
  retentionLabel,
  renameNamespace,
  historyShardsError,
  limitsFor,
  namespaceNameError,
  oidcErrors,
  postgresMajor,
  readOptions,
  replicaError,
  replicasOf,
  retentionError,
  serverComponents,
  withComponent,
  withReplicas,
  writeOptions,
  type ComponentName,
  type NamespaceDeclaration,
  type TemporalOptions,
} from './spec';

/** `persisted` (detail editor) locks the immutable blocks and annotates scaling; `clusterName` fills the database placeholders. */
interface SectionProps extends WizardSectionProps {
  persisted?: WizardSectionProps['spec'];
  clusterName?: string;
  /** The detail editor wraps the section in a titled card and hides the legend. */
  legend?: boolean;
}

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

function scaleHint(current: number | undefined, n: number): string | undefined {
  if (current === undefined || current === n) return undefined;
  return n > current ? `adds ${n - current}` : `removes ${current - n}`;
}

/** Topology step: instance counts per role of the chosen topology plus the optional Web UI. */
export function TopologySection({ provider, spec, onChange, persisted }: SectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  const split = spec.topology.type === 'split';
  const ui = replicasOf(spec, 'ui');
  const uiLimits = limitsFor(topology, 'ui');
  return (
    <div className="stack" style={{ gap: 20 }}>
      <p className="small muted" style={{ margin: 0 }}>
        {split
          ? 'Each Temporal service runs as its own stateless component and scales independently. History instances own the history shards; more frontends add client capacity.'
          : 'One stateless process runs the history, matching, frontend and worker roles. Add instances for redundancy; switch to the split topology for independent scaling.'}{' '}
        State lives in the bound PostgreSQL cluster, so instances can be replaced freely.
      </p>
      <div className="stack" style={{ gap: 14 }}>
        {serverComponents(spec.topology.type).map((name) => {
          const limits = limitsFor(topology, name);
          const replicas = replicasOf(spec, name);
          const current = persisted ? replicasOf(persisted, name) : undefined;
          const error = replicaError(replicas, limits);
          return (
            <div key={name} className="row-between wrap" style={{ gap: 12 }}>
              <div className="stack" style={{ gap: 2 }}>
                <span className="field-label" style={{ margin: 0 }}>{COMPONENT_LABEL[name]}</span>
                <span className="faint xs">{COMPONENT_DESCRIPTION[name]}</span>
                {error ? <span className="field-error" role="alert">{error}</span> : scaleHint(current, replicas) ? <span className="xs muted">{scaleHint(current, replicas)}</span> : null}
              </div>
              <div className="pill-group" role="group" aria-label={`${COMPONENT_LABEL[name]} instances`}>
                {range(limits.minReplicas ?? 1, limits.maxReplicas ?? 1).map((n) => (
                  <button key={n} type="button" aria-pressed={replicas === n} onClick={() => onChange(withReplicas(spec, topology, name, n))}>
                    {n}
                  </button>
                ))}
              </div>
            </div>
          );
        })}
      </div>
      <fieldset className="field" style={{ border: 0, padding: 0, margin: 0 }}>
        <legend className="field-label" style={{ padding: 0, marginBottom: 6 }}>
          Web UI
        </legend>
        <div className="row-between wrap" style={{ gap: 12 }}>
          <Switch
            label={
              <span>
                Temporal Web UI <span className="faint xs">{COMPONENT_DESCRIPTION.ui}</span>
              </span>
            }
            checked={ui > 0}
            onChange={(e) => onChange(withReplicas(spec, topology, 'ui', e.target.checked ? Math.max(1, Math.min(uiLimits.maxReplicas ?? 1, persisted ? replicasOf(persisted, 'ui') : 1)) : 0))}
          />
          {ui > 0 ? (
            <div className="pill-group" role="group" aria-label="Web UI instances">
              {range(1, uiLimits.maxReplicas ?? 1).map((n) => (
                <button key={n} type="button" aria-pressed={ui === n} onClick={() => onChange(withReplicas(spec, topology, 'ui', n))}>
                  {n}
                </button>
              ))}
            </div>
          ) : null}
        </div>
        <span className="field-hint">Private to the project network unless public HTTPS with OIDC login is enabled under Temporal options.</span>
      </fieldset>
    </div>
  );
}

/** Resources step: one editor per component of the topology. All components are stateless with a small runtime disk. */
export function ResourcesSection({ provider, spec, onChange, persisted }: SectionProps) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return (
    <>
      {activeComponents(spec).map((name) => {
        const component = componentOf(spec, name);
        if (!component || component.replicas === 0) return null;
        return (
          <Card key={name} title={`${COMPONENT_LABEL[name]} · ${component.replicas} × resources`}>
            <div className="stack" style={{ gap: 12 }}>
              <ResourcesEditor idPrefix={`temporal-${name}`} minimums={resourceMinimums(spec.provider, topology, name)} showErrors value={component.resources} persisted={persisted ? componentOf(persisted, name)?.resources : undefined} onChange={(resources) => onChange(withComponent(spec, name, { ...component, resources }))} />
              <p className="xs faint" style={{ margin: 0 }}>
                {resourceHint(name)}
              </p>
            </div>
          </Card>
        );
      })}
    </>
  );
}

function resourceHint(name: ComponentName): string {
  switch (name) {
    case 'history':
    case 'server':
      return 'RAM caches workflow mutable state; give this component the most memory. The disk is a runtime scratch volume only — all state is in PostgreSQL.';
    case 'matching':
      return 'Task-queue dispatch is CPU-light; 1 GiB RAM suits most clusters.';
    case 'frontend':
      return 'gRPC entry point; scale CPU with client request rate rather than RAM.';
    case 'worker':
      return 'Runs Temporal’s internal system workflows. Small resources are enough.';
    case 'ui':
      return 'The Web UI is a small Go process; 0.5 GiB RAM is plenty.';
  }
}

/** Basics step: PostgreSQL source, databases, history shards, client auth, namespaces and Web UI access. */
export function OptionsSection({ namespace, spec, onChange, persisted, clusterName, legend = true }: SectionProps) {
  const options = readOptions(spec);
  const previous = persisted ? readOptions(persisted) : undefined;
  const set = (fn: (o: TemporalOptions) => TemporalOptions) => onChange(writeOptions(spec, fn(options)));
  const immutable = persisted !== undefined;
  const pg = options.persistence.postgres;
  const ownNamespace = namespace || DEFAULT_NAMESPACE;
  const sourceNamespace = pg.namespace || ownNamespace;
  const namespaces = useNamespaceOptions();
  const namespaceChoices = namespaces.includes(sourceNamespace) ? namespaces : [...namespaces, sourceNamespace];
  const clusters = useClusters(sourceNamespace);
  const sources = clusters.data?.items.filter((c) => c.spec.provider === 'postgres') ?? [];
  const chosen = sources.find((c) => c.name === pg.cluster);
  const tooOld = chosen !== undefined && postgresMajor(chosen.spec.version) < MIN_POSTGRES_MAJOR;
  const placeholderName = clusterName || '<cluster>';
  const uiEnabled = replicasOf(spec, 'ui') > 0;
  const oidc = oidcErrors(options);
  const shards = historyShardsError(options.historyShards);

  return (
    <fieldset className="field" style={{ border: 0, padding: 0, margin: 0 }}>
      {legend ? (
        <legend className="field-label" style={{ padding: 0, marginBottom: 6 }}>
          Temporal options
        </legend>
      ) : null}
      <div className="stack" style={{ gap: 16 }}>
        {spec.engineConfig?.trim() ? <Callout tone="danger" title="Unsupported inherited configuration">Temporal is configured through these options, not engine configuration overrides. <Button size="sm" onClick={() => onChange({ ...spec, engineConfig: undefined })}>Clear inherited overrides</Button></Callout> : null}

        <div className="stack" style={{ gap: 12 }}>
          <div className="stack" style={{ gap: 2 }}>
            <span className="field-label" style={{ margin: 0 }}>PostgreSQL persistence</span>
            <span className="faint xs">Temporal stores workflow state and visibility in a Karakoram PostgreSQL cluster (major {MIN_POSTGRES_MAJOR} or later). Creation is rejected when PostgreSQL bindings are unavailable on this control plane.</span>
          </div>
          <div className="grid-2">
            <SelectField
              label="Source namespace"
              value={pg.namespace ?? ''}
              disabled={immutable}
              onChange={(e) => set((o) => ({ ...o, persistence: { postgres: { ...o.persistence.postgres, cluster: '', ...(e.target.value ? { namespace: e.target.value } : { namespace: undefined }) } } }))}
              hint={pg.namespace ? 'A source in another namespace must grant this cluster a binding.' : `Same namespace as this cluster (${ownNamespace}).`}
            >
              <option value="">Same as this cluster</option>
              {namespaceChoices.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </SelectField>
            <SelectField
              label="PostgreSQL cluster"
              required
              value={pg.cluster}
              disabled={immutable}
              onChange={(e) => set((o) => ({ ...o, persistence: { postgres: { ...o.persistence.postgres, cluster: e.target.value } } }))}
              error={clusters.error ? errorMessage(clusters.error) : tooOld ? `PostgreSQL ${chosen.spec.version} is older than ${MIN_POSTGRES_MAJOR}; the binding will be refused.` : undefined}
              hint={immutable ? 'Immutable after creation.' : clusters.data && sources.length === 0 ? `No PostgreSQL clusters in namespace ${sourceNamespace}. Create one first.` : 'Both databases are created on this cluster with one binding-managed login role.'}
            >
              <option value="">Choose…</option>
              {!chosen && pg.cluster ? <option value={pg.cluster}>{pg.cluster}</option> : null}
              {sources.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name} · PostgreSQL {c.spec.version}{c.status.phase !== 'Ready' ? ` · ${c.status.phase}` : ''}
                </option>
              ))}
            </SelectField>
          </div>
          <div className="grid-2">
            <TextField label="Persistence database (optional)" mono disabled={immutable} value={pg.database ?? ''} placeholder={defaultDatabase(placeholderName)} onChange={(e) => set((o) => ({ ...o, persistence: { postgres: { ...o.persistence.postgres, database: e.target.value.trim() || undefined } } }))} hint="Default temporal_<cluster>; hyphens become underscores." />
            <TextField label="Visibility database (optional)" mono disabled={immutable} value={pg.visibilityDatabase ?? ''} placeholder={defaultVisibilityDatabase(placeholderName)} onChange={(e) => set((o) => ({ ...o, persistence: { postgres: { ...o.persistence.postgres, visibilityDatabase: e.target.value.trim() || undefined } } }))} hint="Default temporal_<cluster>_visibility." />
          </div>
        </div>

        <TextField
          label="History shards"
          type="number"
          min={1}
          max={MAX_HISTORY_SHARDS}
          step={1}
          style={{ maxWidth: 220 }}
          disabled={immutable}
          value={options.historyShards}
          onChange={(e) => set((o) => ({ ...o, historyShards: Number(e.target.value) }))}
          error={shards}
          hint={immutable ? 'Immutable after creation.' : `Default ${DEFAULT_HISTORY_SHARDS}, 1–${MAX_HISTORY_SHARDS}. Immutable after creation: shards bound how far history can scale out, so size for the cluster’s eventual throughput.`}
        />

        <div className="stack" style={{ gap: 8 }}>
          <Switch
            label={
              <span>
                Require client certificates on the frontend <span className="faint xs">Mutual TLS: SDK clients present a certificate issued from the Client certificates tab.</span>
              </span>
            }
            checked={options.frontend.requireClientAuth}
            onChange={(e) => set((o) => ({ ...o, frontend: { requireClientAuth: e.target.checked } }))}
          />
          {!options.frontend.requireClientAuth ? <Callout tone="warn" title="Frontend accepts unauthenticated clients">TLS stays on, but any client that can reach port 7233 inside the project network can run and inspect workflows. Keep this off only for trusted networks.</Callout> : null}
        </div>

        <NamespacesEditor value={options.namespaces} onChange={(namespaces) => set((o) => ({ ...o, namespaces }))} previous={previous?.namespaces} />

        <div className="stack" style={{ gap: 12 }}>
          <Switch
            label={
              <span>
                Public HTTPS access to the Web UI <span className="faint xs">Zerops subdomain with a managed certificate, protected by OIDC login; otherwise the UI is reachable only inside the project network.</span>
              </span>
            }
            checked={options.ui.publicHttps}
            disabled={!uiEnabled}
            title={uiEnabled ? undefined : 'Enable the Web UI under Topology first.'}
            onChange={(e) => set((o) => ({ ...o, ui: e.target.checked ? { publicHttps: true, oidc: o.ui.oidc ?? { issuer: '', clientId: '' } } : { publicHttps: false } }))}
          />
          {options.ui.publicHttps ? (
            <div className="stack" style={{ gap: 12 }}>
              <div className="grid-2">
                <TextField label="OIDC issuer" mono required value={options.ui.oidc?.issuer ?? ''} placeholder="https://login.example.com/realms/main" onChange={(e) => set((o) => ({ ...o, ui: { publicHttps: true, oidc: { issuer: e.target.value.trim(), clientId: o.ui.oidc?.clientId ?? '' } } }))} hint="HTTPS discovery URL of your identity provider." />
                <TextField label="OIDC client ID" mono required value={options.ui.oidc?.clientId ?? ''} onChange={(e) => set((o) => ({ ...o, ui: { publicHttps: true, oidc: { issuer: o.ui.oidc?.issuer ?? '', clientId: e.target.value.trim() } } }))} />
              </div>
              {oidc.length ? <span className="field-error" role="alert">{oidc[0]}</span> : null}
              <Callout tone="info" title="Client secret is set after creation">
                The OIDC client secret is write-only and never part of the cluster spec. {immutable ? 'Set or replace it on the Web UI tab.' : 'After the cluster is created, store it on the Web UI tab; the public UI cannot sign users in until then.'} Register <code>https://&lt;ui-subdomain&gt;/auth/sso/callback</code> as the redirect URI with your provider.
              </Callout>
            </div>
          ) : null}
        </div>

        <a className="small" href={DOCS_URL} target="_blank" rel="noreferrer">Temporal provider guide</a>
      </div>
    </fieldset>
  );
}

const MAX_NAMESPACES = 50;

function NamespacesEditor({ value, onChange, previous }: { value: NamespaceDeclaration[]; onChange: (namespaces: NamespaceDeclaration[]) => void; previous?: NamespaceDeclaration[] }) {
  const names = value.map((n) => n.name);
  const update = (i: number, patch: Partial<NamespaceDeclaration>) => onChange(value.map((n, j) => (j === i ? { ...n, ...patch } : n)));
  const defaultDeclared = value.some((n) => n.name === 'default');
  return (
    <div className="stack" style={{ gap: 12 }}>
      <div className="row-between wrap" style={{ gap: 8 }}>
        <div className="stack" style={{ gap: 2 }}>
          <span className="field-label" style={{ margin: 0 }}>
            Namespaces ({value.length}/{MAX_NAMESPACES})
          </span>
          <span className="faint xs">Declared namespaces are registered and kept at the given retention. Removing one here stops managing it; delete it from the Namespaces tab to drop its workflows.</span>
        </div>
        <Button size="sm" disabled={value.length >= MAX_NAMESPACES} onClick={() => onChange([...value, { name: '', retention: '168h' }])}>
          + Add namespace
        </Button>
      </div>
      <p className="small muted" style={{ margin: 0 }}>
        <span className="mono">default</span> · {formatRetention(DEFAULT_RETENTION)} · always ensured by the provider{defaultDeclared ? '' : '; add it below only to set a description'}.
      </p>
      {value.map((n, i) => {
        const fixed = n.name === 'default';
        const retentionLocked = fixed && n.retention === DEFAULT_RETENTION;
        const nameError = namespaceNameError(n.name, names, i);
        const retention = fixed && n.retention !== DEFAULT_RETENTION ? 'The default namespace keeps 72h.' : retentionError(n.retention);
        const isNew = previous !== undefined && !previous.some((p) => p.name === n.name);
        return (
          <fieldset key={i} className="card" style={{ padding: 14, margin: 0, border: '1px solid var(--border)' }}>
            <legend className="sr-only">Namespace {i + 1}</legend>
            <div className="grid-3" style={{ alignItems: 'start' }}>
              <TextField label="Name" mono value={n.name} onChange={(e) => update(i, renameNamespace(n, e.target.value.trim()))} placeholder="orders" error={nameError} hint={isNew ? 'Registered when applied.' : undefined} />
              <TextField label="Retention" mono value={n.retention} readOnly={retentionLocked} onChange={(e) => update(i, { retention: e.target.value.trim() })} placeholder="168h" error={retention} hint={retentionLocked ? 'Fixed for the default namespace.' : !retention ? retentionLabel(n.retention) : undefined} />
              <div className="row" style={{ gap: 8, alignItems: 'flex-end' }}>
                <TextField className="grow" label="Description" value={n.description ?? ''} onChange={(e) => update(i, { description: e.target.value || undefined })} placeholder="Optional" />
                <Button size="sm" variant="ghost" onClick={() => onChange(value.filter((_, j) => j !== i))}>
                  Remove
                </Button>
              </div>
            </div>
          </fieldset>
        );
      })}
      <span className="field-hint">Retention is how long closed workflow histories stay queryable (Go duration, at least 24h; 168h = 7 days). The default namespace is always present with 72h retention.</span>
    </div>
  );
}
