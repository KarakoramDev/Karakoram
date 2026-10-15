// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Button } from '@/components/ui/Button';
import { ChoiceGroup, Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { ResourcesEditor } from '@/pages/clusters/editors/ResourcesEditor';
import { resourceMinimums } from '@/pages/clusters/wizard/resources';
import type { WizardSectionProps } from '../types';
import {
  CLUSTER_SIZES,
  COMPONENT_LABEL,
  DEFAULT_PIPELINE_CONFIG,
  MASTER_SIZES,
  MAX_DATA_NODES,
  MAX_PIPELINES,
  PIPELINE_ENV,
  PIPELINE_ID_RE,
  STACK_COMPONENTS,
  STACK_MAX,
  activeComponents,
  componentOf,
  defaultComponent,
  heapMB,
  limitsFor,
  readOptions,
  replicasOf,
  withComponent,
  withReplicas,
  writeOptions,
  type ComponentName,
  type ElasticOptions,
  type StackComponent,
} from './spec';

const STACK_DESCRIPTION: Record<StackComponent, string> = {
  kibana: 'Web UI for search, dashboards and stack management.',
  logstash: 'Ingest pipelines receiving Beats (5044) and HTTP (8080) events.',
  apm: 'Collects traces and metrics from APM agents (8200).',
};

function haHint(n: number): string {
  if (n === 1) return 'Single node, no redundancy. Replica shards stay unassigned (yellow). Dev only.';
  const tolerated = Math.floor((n - 1) / 2);
  return `${n === 3 ? 'Recommended minimum for HA. ' : ''}Survives ${tolerated} node${tolerated > 1 ? 's' : ''} down.`;
}

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

/** Scaling description relative to the persisted count. */
function scaleHint(current: number | undefined, n: number): string | undefined {
  if (current === undefined) return undefined;
  if (n === current) return 'current';
  return n > current ? `adds ${n - current}` : `removes ${current - n}, drained one node at a time`;
}

function joinHints(...parts: (string | undefined)[]): string {
  return parts.filter(Boolean).join(' · ');
}

/** Topology step: node counts per role and the optional stack components. `persisted` (scaling) annotates changes. */
export function TopologySection({ provider, spec, onChange, persisted }: WizardSectionProps & { persisted?: WizardSectionProps['spec'] }) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  const dedicated = spec.topology.type === 'dedicatedMasters';
  const nodes = replicasOf(spec, 'engine');
  const engineLimits = limitsFor(topology, 'engine');
  const sizes = CLUSTER_SIZES.includes(nodes) ? CLUSTER_SIZES : [...CLUSTER_SIZES, nodes].sort((a, b) => a - b);
  const masters = replicasOf(spec, 'master');
  const currentNodes = persisted ? replicasOf(persisted, 'engine') : undefined;
  const currentMasters = persisted ? replicasOf(persisted, 'master') : undefined;
  return (
    <div className="stack" style={{ gap: 20 }}>
      {dedicated ? (
        <>
          <ChoiceGroup<string>
            name="es-masters"
            legend="Dedicated master nodes"
            value={String(masters)}
            onChange={(v) => onChange(withReplicas(spec, topology, 'master', Number(v)))}
            options={(limitsFor(topology, 'master').allowedReplicas ?? MASTER_SIZES).map((n) => ({ value: String(n), title: `${n} masters`, description: joinHints(`Tolerates ${Math.floor((n - 1) / 2)} master failure${n > 3 ? 's' : ''}; master-only role.`, scaleHint(currentMasters, n)) }))}
            hint={persisted ? 'Masters are added or removed one at a time, never the last elected master first.' : 'Masters hold cluster state only. The count can later be changed between 3 and 5.'}
          />
          <TextField
            label="Data nodes"
            type="number"
            min={engineLimits.minReplicas ?? 1}
            max={engineLimits.maxReplicas ?? MAX_DATA_NODES}
            style={{ maxWidth: 220 }}
            value={nodes}
            onChange={(e) => onChange(withReplicas(spec, topology, 'engine', Number(e.target.value)))}
            hint={joinHints(`1–${engineLimits.maxReplicas ?? MAX_DATA_NODES} nodes with the data, ingest and transform roles. Replica shards need at least 2 data nodes.`, currentNodes !== undefined && nodes < currentNodes ? `Removing ${currentNodes - nodes}: each node is drained of shards and removed one at a time; the remaining nodes must still fit every replica.` : undefined)}
          />
        </>
      ) : (
        <ChoiceGroup<string>
          name="es-nodes"
          legend="Nodes"
          value={String(nodes)}
          onChange={(v) => onChange(withReplicas(spec, topology, 'engine', Number(v)))}
          options={sizes.map((n) => ({ value: String(n), title: `${n} node${n > 1 ? 's' : ''}`, description: joinHints(CLUSTER_SIZES.includes(n) ? haHint(n) : 'Even count: a tie cannot elect a master after losing half the nodes.', scaleHint(currentNodes, n)) }))}
          hint={persisted ? 'Nodes are added and join immediately; removals drain shards one node at a time and keep a voting majority.' : 'Every node is master-eligible and holds data. Odd counts keep a clear master majority; nodes can be added or removed one at a time later.'}
        />
      )}

      <fieldset className="field" style={{ border: 0, padding: 0, margin: 0 }}>
        <legend className="field-label" style={{ padding: 0, marginBottom: 6 }}>
          Stack components
        </legend>
        <div className="stack" style={{ gap: 12 }}>
          {STACK_COMPONENTS.map((name) => {
            const replicas = replicasOf(spec, name);
            const limits = limitsFor(topology, name);
            const max = limits.maxReplicas ?? STACK_MAX[name];
            return (
              <div key={name} className="row-between wrap" style={{ gap: 12 }}>
                <Switch
                  label={
                    <span>
                      {COMPONENT_LABEL[name]} <span className="faint xs">{STACK_DESCRIPTION[name]}</span>
                    </span>
                  }
                  checked={replicas > 0}
                  onChange={(e) => onChange(withReplicas(spec, topology, name, e.target.checked ? Math.max(1, Math.min(max, defaultComponent(topology, name).replicas || 1)) : 0))}
                />
                {replicas > 0 ? (
                  <div className="pill-group" role="group" aria-label={`${COMPONENT_LABEL[name]} instances`}>
                    {range(1, max).map((n) => (
                      <button key={n} type="button" aria-pressed={replicas === n} onClick={() => onChange(withReplicas(spec, topology, name, n))}>
                        {n}
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
        <span className="field-hint">Stack components run the same version as Elasticsearch and are scaled as stateless containers. Kibana is required for the web UI.</span>
      </fieldset>
    </div>
  );
}

/** Resources step: one editor per active component. `persisted` (scaling) lets disk only grow. */
export function ResourcesSection({ provider, spec, onChange, persisted }: WizardSectionProps & { persisted?: WizardSectionProps['spec'] }) {
  const topology = provider.topologies.find((t) => t.type === spec.topology.type);
  return (
    <>
      {activeComponents(spec).map((name) => {
        const component = componentOf(spec, name);
        if (!component) return null;
        const stateless = name === 'kibana' || name === 'logstash' || name === 'apm';
        const heap = heapMB(name, component.resources.minRam);
        return (
          <Card key={name} title={`${COMPONENT_LABEL[name]} · ${component.replicas} × resources`}>
            <div className="stack" style={{ gap: 12 }}>
              <ResourcesEditor idPrefix={`es-${name}`} minimums={resourceMinimums(spec.provider, topology, name)} showErrors noDisk={stateless} value={component.resources} persisted={persisted ? componentOf(persisted, name)?.resources : undefined} onChange={(resources) => onChange(withComponent(spec, name, { ...component, resources }))} />
              <p className="xs faint" style={{ margin: 0 }}>
                {resourceHint(name, heap)}
              </p>
            </div>
          </Card>
        );
      })}
    </>
  );
}

function resourceHint(name: ComponentName, heap: number | undefined): string {
  switch (name) {
    case 'engine':
    case 'master':
      return `Fixed heap of ${heap ?? '—'} MiB derived from minimum RAM (40 % up to 4 GiB, 50 % above). Keep min and max RAM equal; RAM changes trigger a rolling restart. The data volume grows between min and max disk.`;
    case 'kibana':
      return `Node.js old-space limit ${heap ?? '—'} MiB (60 % of minimum RAM). Stateless; uses a small runtime disk.`;
    case 'logstash':
      return `JVM heap ${heap ?? '—'} MiB (50 % of minimum RAM). The persistent queue, when enabled, lives on the container disk.`;
    case 'apm':
      return 'APM Server is a small Go process; 0.5 GiB RAM suits most agents.';
  }
}

/** Basics step: engine options (exposure of stack components, APM auth, Logstash queue and pipelines). */
export function OptionsSection({ spec, onChange }: WizardSectionProps) {
  const options = readOptions(spec);
  const set = (fn: (o: ElasticOptions) => ElasticOptions) => onChange(writeOptions(spec, fn(options)));
  const kibana = replicasOf(spec, 'kibana') > 0;
  const apm = replicasOf(spec, 'apm') > 0;
  const logstash = replicasOf(spec, 'logstash') > 0;
  return (
    <fieldset className="field" style={{ border: 0, padding: 0, margin: 0 }}>
      <legend className="field-label" style={{ padding: 0, marginBottom: 6 }}>
        Elastic Stack options
      </legend>
      <div className="stack" style={{ gap: 14 }}>
        {spec.engineConfig?.trim() ? <Callout tone="danger" title="Unsupported inherited configuration">Elasticsearch uses Elastic Stack options instead of engine configuration overrides. <Button size="sm" onClick={() => onChange({ ...spec, engineConfig: undefined })}>Clear inherited overrides</Button></Callout> : null}
        <Switch
          label={
            <span>
              Public HTTPS access to Kibana <span className="faint xs">Zerops subdomain with a managed certificate; otherwise Kibana is reachable only inside the project network.</span>
            </span>
          }
          checked={options.kibana.public}
          disabled={!kibana}
          title={kibana ? undefined : 'Enable Kibana under Topology first.'}
          onChange={(e) => set((o) => ({ ...o, kibana: { ...o.kibana, public: e.target.checked } }))}
        />
        <Switch
          label={
            <span>
              Public HTTPS access to APM Server <span className="faint xs">Lets agents outside the project send traces; authenticated with the secret token or API keys.</span>
            </span>
          }
          checked={options.apm.public}
          disabled={!apm}
          title={apm ? undefined : 'Enable APM Server under Topology first.'}
          onChange={(e) => set((o) => ({ ...o, apm: { ...o.apm, public: e.target.checked } }))}
        />
        <Switch
          label={
            <span>
              APM agent API-key authentication <span className="faint xs">Accept Elasticsearch API keys in addition to the generated secret token.</span>
            </span>
          }
          checked={options.apm.apiKeys}
          disabled={!apm}
          onChange={(e) => set((o) => ({ ...o, apm: { ...o.apm, apiKeys: e.target.checked } }))}
        />
        <Switch
          label={
            <span>
              Logstash persistent queue <span className="faint xs">Buffers events on the container disk so a restart does not drop in-flight data.</span>
            </span>
          }
          checked={options.logstash.persistentQueue}
          disabled={!logstash}
          title={logstash ? undefined : 'Enable Logstash under Topology first.'}
          onChange={(e) => set((o) => ({ ...o, logstash: { ...o.logstash, persistentQueue: e.target.checked } }))}
        />
        {logstash ? <PipelinesEditor value={options.logstash.pipelines} onChange={(pipelines) => set((o) => ({ ...o, logstash: { ...o.logstash, pipelines } }))} /> : null}
        <a className="small" href="https://github.com/KarakoramDev/Karakoram/blob/main/docs/user-guide/elasticsearch.md#create" target="_blank" rel="noreferrer">Elastic Stack options guide</a>
      </div>
    </fieldset>
  );
}

function PipelinesEditor({ value, onChange }: { value: ElasticOptions['logstash']['pipelines']; onChange: (pipelines: ElasticOptions['logstash']['pipelines']) => void }) {
  const ids = value.map((p) => p.id);
  const update = (i: number, patch: Partial<ElasticOptions['logstash']['pipelines'][number]>) => onChange(value.map((p, j) => (j === i ? { ...p, ...patch } : p)));
  return (
    <div className="stack" style={{ gap: 12 }}>
      <div className="row-between wrap" style={{ gap: 8 }}>
        <span className="field-label" style={{ margin: 0 }}>
          Logstash pipelines ({value.length}/{MAX_PIPELINES})
        </span>
        <Button size="sm" disabled={value.length >= MAX_PIPELINES} onClick={() => onChange([...value, { id: value.length ? `pipeline-${value.length + 1}` : 'main', config: value.length ? '' : DEFAULT_PIPELINE_CONFIG }])}>
          + Add pipeline
        </Button>
      </div>
      {value.length === 0 ? (
        <Callout tone="info">
          Without pipelines Logstash runs a default one: Beats on 5044 and HTTP on 8080 are written to Elasticsearch data streams. Add a pipeline to replace it. Pipelines may reference the environment Karakoram injects:{' '}
          {PIPELINE_ENV.map((env, i) => (
            <span key={env}>
              {i ? ', ' : ''}
              <code>${`{${env}}`}</code>
            </span>
          ))}
          .
        </Callout>
      ) : (
        <p className="xs faint" style={{ margin: 0 }}>
          Elasticsearch connection details are available as{' '}
          {PIPELINE_ENV.map((env, i) => (
            <span key={env}>
              {i ? ', ' : ''}
              <code>${`{${env}}`}</code>
            </span>
          ))}
          ; the writer user may create <code>logs-*</code>, <code>metrics-*</code> and <code>logstash-*</code> indices and data streams.
        </p>
      )}
      {value.map((p, i) => {
        const idError = !PIPELINE_ID_RE.test(p.id) ? 'Lowercase letters, digits, _ or -, 1–32 characters, starting with a letter.' : ids.indexOf(p.id) !== i ? 'Pipeline IDs must be unique.' : undefined;
        return (
          <Card key={i}>
            <div className="stack" style={{ gap: 10 }}>
              <div className="row-between wrap" style={{ gap: 8, alignItems: 'flex-end' }}>
                <TextField label="Pipeline ID" mono value={p.id} onChange={(e) => update(i, { id: e.target.value })} error={idError} style={{ maxWidth: 260 }} />
                <span className="row" style={{ gap: 6 }}>
                  {!p.config.trim() ? (
                    <Button size="sm" onClick={() => update(i, { config: DEFAULT_PIPELINE_CONFIG })}>
                      Insert default
                    </Button>
                  ) : null}
                  <Button size="sm" variant="ghost" onClick={() => onChange(value.filter((_, j) => j !== i))}>
                    Remove
                  </Button>
                </span>
              </div>
              <TextAreaField label="Configuration" mono rows={10} value={p.config} onChange={(e) => update(i, { config: e.target.value })} error={!p.config.trim() ? 'Pipeline configuration must not be empty.' : undefined} placeholder={DEFAULT_PIPELINE_CONFIG} />
            </div>
          </Card>
        );
      })}
    </div>
  );
}
