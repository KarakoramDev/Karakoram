// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { useBackupStorages, useCluster, useClusters, useNamespaceOptions, useProviders, type ClusterSpec, type Provider } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE, clusterObject } from '@/auth/permissions';
import { useNamespace } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { ChoiceGroup, SelectField, Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout, Card, ErrorBlock, KV, Loading, PageHeader } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { ResourcesEditor } from './editors/ResourcesEditor';
import { SchedulesEditor } from './editors/SchedulesEditor';
import { ExposureEditor, TLSEditor } from './editors/AccessEditor';
import { resourcesSummary } from './cluster-utils';
import { clusterPath } from './detail/ClusterDetail';
import { dataSourceFor, dataSourceLabel, wireDataSource } from './data-source';
import { STEPS, claimSubmission, firstInvalidStep, nextStep, normalizeWizardSpec, resetEditedAttempt, type StepId } from './wizard/state';
import { memberChoices, nameError, topologyErrors, validateWizard } from './wizard/validation';
import { resourceMinimums } from './wizard/resources';
import { engineConsequences, engineRules } from './wizard/engine';
import { exposureSummary, totalFootprint } from './wizard/review';
import { validatePitrPoint } from './wizard/restore-point';
import { useSourcePitrWindow } from './wizard/queries';
import { cleanTLS } from './wizard/access';

type Topology = Schemas['ProviderTopology'];



function specFromTopology(provider: Provider, topology: Topology, version: string): ClusterSpec {
  const d = topology.defaults;
  return {
    ...d,
    provider: provider.name,
    version,
    topology: { ...d.topology, type: topology.type },
    components: structuredClone(d.components),
    tls: d.tls ?? { mode: 'requireTLS', ca: 'selfSigned' },
    exposure: d.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] },
    monitoring: d.monitoring ?? { enabled: true },
    backup: d.backup ?? { schedules: [], pitr: { enabled: false, oplogSpanMinutes: 10 } },
  };
}


export function ClusterWizardPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const toast = useToast();
  const queryClient = useQueryClient();
  const [params] = useSearchParams();
  const { namespace: selectedNamespace } = useNamespace();
  const providers = useProviders();
  const namespaceOptions = useNamespaceOptions();

  const [step, setStep] = useState<StepId>('basics');
  const [name, setName] = useState('');
  const [targetNamespace, setTargetNamespace] = useState(selectedNamespace ?? '');
  const [spec, setSpec] = useState<ClusterSpec | null>(null);
  const [presetName, setPresetName] = useState('');
  const [touched, setTouched] = useState<Record<StepId, boolean>>({ basics: false, topology: false, resources: false, backups: false, access: false, review: false });

  const [allAddressesConfirmed, setAllAddressesConfirmed] = useState(false);
  const submissionLocked = useRef(false);
  const submitted = useRef(false);
  const idempotency = useIdempotencyKey();
  // Place the cluster in the only namespace available without asking.
  useEffect(() => {
    if (!targetNamespace && namespaceOptions.length === 1) setTargetNamespace(namespaceOptions[0]!);
  }, [targetNamespace, namespaceOptions]);

  const storages = useBackupStorages(targetNamespace || DEFAULT_NAMESPACE);
  const clusters = useClusters(targetNamespace || DEFAULT_NAMESPACE);

  const provider = useMemo(() => providers.data?.items.find((p) => p.name === spec?.provider) ?? providers.data?.items[0], [providers.data, spec?.provider]);
  const topology = useMemo(() => provider?.topologies.find((t) => t.type === spec?.topology.type), [provider, spec?.topology.type]);

  // A clone request arrives as ?from=<cluster>[&sourceNamespace=<ns>]&backup=<name> or &pitrTime=<iso>.
  const from = params.get('from');
  const fromNamespace = params.get('sourceNamespace') || params.get('namespace') || selectedNamespace || DEFAULT_NAMESPACE;
  const source = useCluster(fromNamespace, from ?? '', Boolean(from));

  // Seed from the catalog defaults once providers are loaded; honour the clone request.
  useEffect(() => {
    if (spec || !provider || (from && source.isPending)) return;
    const defaultVersion = provider.versions.find((v) => v.default)?.version ?? provider.versions[0]?.version ?? '';
    const topo = provider.topologies.find((t) => t.type === 'replicaSet') ?? provider.topologies[0];
    if (!topo) return;
    const seeded = specFromTopology(provider, topo, defaultVersion);
    if (from) {
      const backup = params.get('backup') ?? undefined;
      const pitrTime = params.get('pitrTime') ?? undefined;
      seeded.dataSource = dataSourceFor(fromNamespace, from, { backup, pitrTime });
      const src = source.data;
      if (src) {
        seeded.version = src.spec.version;
        seeded.topology = { ...src.spec.topology };
        seeded.components = structuredClone(src.spec.components);
        seeded.tls = src.spec.tls ? { mode: src.spec.tls.mode, ca: 'selfSigned' } : seeded.tls;
      }
    }
    setSpec(normalizeWizardSpec(seeded));
  }, [provider, spec, params, from, fromNamespace, source.isPending, source.data]);

  const existingNames = useMemo(() => clusters.data?.items.map((c) => c.name) ?? [], [clusters.data]);
  const pitrSource = useSourcePitrWindow(spec?.dataSource?.namespace || targetNamespace || DEFAULT_NAMESPACE, spec?.dataSource?.cluster ?? '', Boolean(spec?.dataSource?.pitrTime));
  const validation = useMemo(() => {
    if (!spec) return null;
    const result = validateWizard(name, targetNamespace, spec, existingNames, provider, allAddressesConfirmed);
    if (spec.dataSource?.pitrTime) result.backups.push(...(pitrSource.error ? [errorMessage(pitrSource.error)] : validatePitrPoint(spec.dataSource.pitrTime, pitrSource.data ?? null)));
    return result;
  }, [name, targetNamespace, spec, existingNames, provider, allAddressesConfirmed, pitrSource.data, pitrSource.error]);

  const create = useMutation({
    mutationFn: (body: Schemas['CreateClusterRequest']) => unwrap(api.POST('/clusters', { params: { query: { namespace: targetNamespace } }, headers: idempotencyHeaders(idempotency.key), body })),
    onSuccess: (c) => {
      idempotency.reset();
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(`Cluster ${c.name} is being created.`);
      navigate(clusterPath(c.namespace || targetNamespace, c.name));
    },
    onError: () => { setStep('review'); },
    onSettled: () => { submissionLocked.current = false; },
  });

  if (!auth.can('clusters', 'create')) {
    return (
      <div className="page">
        <Callout tone="danger">You do not have permission to create clusters.</Callout>
      </div>
    );
  }
  if (providers.isPending || !spec || !provider || !topology || !validation) {
    return (
      <div className="page">
        {providers.error ? <ErrorBlock error={providers.error} retry={() => providers.refetch()} /> : <Loading label="Loading engine catalog…" />}
      </div>
    );
  }

  const editAttempt = () => {
    if (resetEditedAttempt(submitted, submissionLocked.current)) {
      idempotency.reset();
      create.reset();
    }
  };
  const update = (fn: (s: ClusterSpec) => ClusterSpec) => {
    editAttempt();
    setSpec((s) => (s ? normalizeWizardSpec(fn(s)) : s));
  };
  const stepIndex = STEPS.findIndex((s) => s.id === step);
  const errorsFor = (id: StepId): string[] => (id === 'review' ? [] : validation[id]);
  const allErrors = [...validation.basics, ...validation.topology, ...validation.resources, ...validation.backups, ...validation.access];
  const storageNames = storages.data?.items.map((s) => s.name) ?? [];
  const sharded = spec.topology.type === 'sharded';
  const rules = engineRules(spec.provider);
  const counts = topologyErrors(spec, topology);
  const footprint = totalFootprint(spec);

  const goto = (id: StepId) => {
    setTouched((t) => ({ ...t, [step]: true }));
    const movingForward = STEPS.findIndex((s) => s.id === id) > stepIndex;
    if (!movingForward || errorsFor(step).length === 0) setStep(id);
  };

  const applyPreset = (pName: string) => {
    setPresetName(pName);
    setAllAddressesConfirmed(false);
    const preset = provider.presets.find((p) => p.name === pName);
    if (!preset) return;
    update((s) => ({
      ...structuredClone(preset.spec),
      provider: provider.name,
      version: preset.spec.version || s.version,
      dataSource: s.dataSource,
    }));
  };

  const switchTopology = (type: Topology['type']) => {
    const t = provider.topologies.find((x) => x.type === type);
    if (!t) return;
    update((s) => ({ ...specFromTopology(provider, t, s.version), backup: s.backup, tls: s.tls, exposure: s.exposure, monitoring: s.monitoring, dataSource: s.dataSource }));
    setPresetName('');
  };

  const submit = (button: HTMLButtonElement) => {
    if (submissionLocked.current) return;
    const invalid = firstInvalidStep(validation);
    if (invalid) {
      setTouched({ basics: true, topology: true, resources: true, backups: true, access: true, review: true });
      setStep(invalid);
      return;
    }
    if (!claimSubmission(submissionLocked)) return;
    submitted.current = true;
    button.disabled = true;
    // Send exposure.allowAllAddresses = allAddressesConfirmed after the API schema adds it.
    const body: Schemas['CreateClusterRequest'] = { name, spec: cleanSpec(spec, targetNamespace) };
    create.mutate(body);
  };

  return (
    <div className="page">
      <PageHeader
        crumbs={
          <>
            <Link to="/clusters">Clusters</Link> <span>/</span> <span>New</span>
          </>
        }
        title="Create cluster"
        description={targetNamespace ? `Provision a new database cluster in namespace ${targetNamespace}.` : 'Provision a new database cluster in one of your namespaces.'}
      />
      <div className="wizard">
        <aside className="wizard-side">
          <nav className="steps" aria-label="Wizard steps">
            {STEPS.map((s, i) => {
              const errs = errorsFor(s.id);
              const cls = ['step', s.id === step ? 'active' : '', touched[s.id] && errs.length ? 'invalid' : touched[s.id] && i < stepIndex ? 'done' : ''].join(' ');
              return (
                <button key={s.id} type="button" className={cls} onClick={() => goto(s.id)} disabled={create.isPending} aria-current={s.id === step ? 'step' : undefined}>
                  <span className="step-index" aria-hidden>
                    {touched[s.id] && errs.length ? '!' : i + 1}
                  </span>
                  {s.label}
                </button>
              );
            })}
          </nav>
          <dl className="wizard-summary kv">
            <dt>Namespace</dt>
            <dd className="mono">{targetNamespace || '—'}</dd>
            <dt>Name</dt>
            <dd className="mono">{name || '—'}</dd>
            <dt>Engine</dt>
            <dd>
              {provider.displayName} {spec.version}
            </dd>
            <dt>Topology</dt>
            <dd>{sharded ? `${spec.topology.shards ?? 0} shards × ${spec.components.engine.replicas}` : `RS × ${spec.components.engine.replicas}`}</dd>
            <dt>Backups</dt>
            <dd>{spec.backup?.storage ? `${spec.backup.storage}${spec.backup.pitr?.enabled ? ' + PITR' : ''}` : 'none'}</dd>
            <dt>Exposure</dt>
            <dd>{exposureSummary(spec)}</dd>
          </dl>
        </aside>

        <div className="wizard-body">
          {step === 'basics' ? (
            <Card title="Basics">
              <div className="stack" style={{ gap: 16 }}>
                {namespaceOptions.length > 1 || !targetNamespace ? (
                  <SelectField
                    label="Namespace"
                    required
                    value={targetNamespace}
                    onChange={(e) => {
                      setTargetNamespace(e.target.value);
                      // Storages and name uniqueness are per namespace; drop a stale storage choice.
                      // The data source keeps its own explicit namespace, so a chosen source
                      // cluster never changes meaning here; the Basics callout flags the result.
                      update((s) => ({ ...s, backup: s.backup ? { ...s.backup, storage: undefined, schedules: s.backup.schedules?.map((sc) => ({ ...sc, storage: '' })) } : s.backup }));
                    }}
                    hint="The Zerops project that will host the cluster. Backup storages and name uniqueness are per namespace."
                    error={touched.basics && !targetNamespace ? 'Choose a namespace.' : undefined}
                  >
                    <option value="">Choose…</option>
                    {namespaceOptions.map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </SelectField>
                ) : null}
                <TextField
                  label="Cluster name"
                  required
                  mono
                  value={name}
                  onChange={(e) => { editAttempt(); setName(e.target.value); }}
                  placeholder="orders"
                  hint="2–12 lowercase letters and digits; becomes the Zerops hostname prefix (e.g. ordersm1, ordersm2)."
                  error={touched.basics ? nameError(name, existingNames) : undefined}
                  autoFocus
                />
                <div className="grid-2">
                  <SelectField
                    label="Engine"
                    value={provider.name}
                    onChange={(e) => {
                      const p = providers.data?.items.find((x) => x.name === e.target.value);
                      if (!p) return;
                      const t = p.topologies.find((x) => x.type === spec.topology.type) ?? p.topologies[0];
                      if (t) {
                        editAttempt();
                        setAllAddressesConfirmed(false);
                        setSpec(normalizeWizardSpec(specFromTopology(p, t, p.versions.find((v) => v.default)?.version ?? p.versions[0]?.version ?? '')));
                      }
                    }}
                  >
                    {providers.data?.items.map((p) => (
                      <option key={p.name} value={p.name}>
                        {p.displayName}
                      </option>
                    ))}
                  </SelectField>
                  <SelectField label="Version" value={spec.version} onChange={(e) => update((s) => ({ ...s, version: e.target.value }))} error={touched.basics && (!provider.versions.some((v) => v.version === spec.version && v.status !== 'unavailable')) ? 'Choose an available engine version.' : undefined}>
                    {provider.versions.map((v) => (
                      <option key={v.version} value={v.version} disabled={v.status === 'unavailable'}>
                        {v.version}
                        {v.default ? ' (default)' : ''}
                        {v.status === 'deprecated' ? ' — deprecated' : v.status === 'unavailable' ? ' — unavailable' : ''}
                        {v.pbmVersion ? ` · backup ${v.pbmVersion}` : ''}
                      </option>
                    ))}
                  </SelectField>
                </div>
                <ChoiceGroup<Topology['type']>
                  name="topology"
                  legend="Topology"
                  value={spec.topology.type}
                  onChange={switchTopology}
                  options={provider.topologies.map((t) => ({
                    value: t.type,
                    title: rules.topology[t.type]?.title ?? t.displayName,
                    description: rules.topology[t.type]?.description ?? 'Configure the provider components on the Topology step.',
                  }))}
                />
                {provider.presets.length ? (
                  <SelectField label="Preset (optional)" value={presetName} onChange={(e) => applyPreset(e.target.value)} hint="Applies the preset's topology, resources and backup defaults; you can still edit every field.">
                    <option value="">Custom</option>
                    {provider.presets.map((p) => (
                      <option key={p.name} value={p.name}>
                        {p.displayName}
                        {p.description ? ` — ${p.description}` : ''}
                      </option>
                    ))}
                  </SelectField>
                ) : null}
                {spec.dataSource ? (
                  <Callout tone="info" title="Seeding from existing data">
                    This cluster will be restored from <strong className="mono">{dataSourceLabel(spec.dataSource, targetNamespace)}</strong>{' '}
                    {spec.dataSource.backup ? <>backup <span className="mono">{spec.dataSource.backup}</span></> : spec.dataSource.pitrTime ? <>at {spec.dataSource.pitrTime}</> : <>(restore point not chosen yet)</>}.
                    {spec.dataSource.namespace && spec.dataSource.namespace !== targetNamespace ? (
                      <>
                        {' '}
                        The source stays in namespace <span className="mono">{spec.dataSource.namespace}</span>, so this is a cross-namespace restore and needs <code>backups:export</code> on the source cluster.
                      </>
                    ) : null}{' '}
                    Change this under Backups.
                  </Callout>
                ) : null}
              </div>
            </Card>
          ) : null}

          {step === 'topology' ? (
            <Card title={sharded ? 'Sharded topology' : 'Replica set'}>
              <div className="stack" style={{ gap: 16 }}>
                <Callout tone="warn" title="No host anti-affinity on Zerops">
                  Zerops does not guarantee that members land on separate physical hosts. More members improve resilience against process and container failures, but host-failure tolerance cannot be
                  promised. Karakoram reports detected co-location on the cluster overview.
                </Callout>
                {sharded ? (
                  <>
                    <div className="grid-2">
                      <TextField
                        label="Shards"
                        type="number"
                        min={1}
                        value={spec.topology.shards ?? 1}
                        onChange={(e) => update((s) => ({ ...s, topology: { ...s.topology, shards: Number(e.target.value) } }))}
                        hint="Shards can be added later, never removed."
                        error={touched.topology ? counts.shards : undefined}
                      />
                      <TextField
                        label="Routers (mongos)"
                        type="number"
                        min={1}
                        value={spec.components.router?.replicas ?? 1}
                        onChange={(e) =>
                          update((s) => ({
                            ...s,
                            components: { ...s.components, router: { replicas: Number(e.target.value), resources: s.components.router?.resources ?? topology.defaults.components.router?.resources ?? s.components.engine.resources } },
                          }))
                        }
                        hint="Clients connect through routers."
                        error={touched.topology ? counts.router : undefined}
                      />
                    </div>
                    <ChoiceGroup<string>
                      name="shard-members"
                      legend="Members per shard"
                      value={String(spec.components.engine.replicas)}
                      onChange={(v) => update((s) => ({ ...s, components: { ...s.components, engine: { ...s.components.engine, replicas: Number(v) } } }))}
                      options={memberChoices(topology, 'engine').map((n) => ({ value: String(n), title: `${n}`, description: n === 1 ? 'No redundancy' : `Tolerates ${Math.floor((n - 1) / 2)} failure${n > 3 ? 's' : ''}` }))}
                      hint={touched.topology && counts.engine ? <span className="field-error" role="alert">{counts.engine}</span> : undefined}
                    />
                    <ChoiceGroup<string>
                      name="cfg-members"
                      legend="Config server members"
                      value={String(spec.components.configServer?.replicas ?? 3)}
                      onChange={(v) =>
                        update((s) => ({
                          ...s,
                          components: { ...s.components, configServer: { replicas: Number(v), resources: s.components.configServer?.resources ?? topology.defaults.components.configServer?.resources ?? s.components.engine.resources } },
                        }))
                      }
                      options={memberChoices(topology, 'configServer').map((n) => ({ value: String(n), title: `${n}`, description: n === 1 ? 'Dev only' : 'Replicated config metadata' }))}
                      hint={touched.topology && counts.configServer ? <span className="field-error" role="alert">{counts.configServer}</span> : undefined}
                    />
                  </>
                ) : (
                  <ChoiceGroup<string>
                    name="rs-members"
                    legend="Members"
                    value={String(spec.components.engine.replicas)}
                    onChange={(v) => update((s) => ({ ...s, components: { ...s.components, engine: { ...s.components.engine, replicas: Number(v) } } }))}
                    options={memberChoices(topology, 'engine').map((n) => ({ value: String(n), title: `${n} member${n > 1 ? 's' : ''}`, description: n === 1 ? 'Single node, no failover. Dev only.' : `Majority ${Math.ceil(n / 2)}; survives ${Math.floor((n - 1) / 2)} down.` }))}
                    hint={touched.topology && counts.engine ? <span className="field-error" role="alert">{counts.engine}</span> : undefined}
                  />
                )}
                <TextAreaField
                  label="Engine configuration overrides (YAML, optional)"
                  mono
                  rows={5}
                  value={spec.engineConfig ?? ''}
                  onChange={(e) => update((s) => ({ ...s, engineConfig: e.target.value || undefined }))}
                  hint={rules.configHint}
                />
              </div>
            </Card>
          ) : null}

          {step === 'resources' ? (
            <div className="stack" style={{ gap: 16 }}>
              <Card title={sharded ? 'Shard members' : 'Members'}>
                <ResourcesEditor idPrefix="engine" minimums={resourceMinimums(provider.name, topology, 'engine')} showErrors={touched.resources} value={spec.components.engine.resources} onChange={(r) => update((s) => ({ ...s, components: { ...s.components, engine: { ...s.components.engine, resources: r } } }))} />
              </Card>
              {sharded && spec.components.configServer ? (
                <Card title="Config servers">
                  <ResourcesEditor
                    idPrefix="cfg"
                    minimums={resourceMinimums(provider.name, topology, 'configServer')}
                    showErrors={touched.resources}
                    value={spec.components.configServer.resources}
                    onChange={(r) => update((s) => ({ ...s, components: { ...s.components, configServer: { ...s.components.configServer!, resources: r } } }))}
                  />
                </Card>
              ) : null}
              {sharded && spec.components.router ? (
                <Card title="Routers">
                  <ResourcesEditor idPrefix="router" noDisk minimums={resourceMinimums(provider.name, topology, 'router')} showErrors={touched.resources} value={spec.components.router.resources} onChange={(r) => update((s) => ({ ...s, components: { ...s.components, router: { ...s.components.router!, resources: r } } }))} />
                </Card>
              ) : null}
            </div>
          ) : null}

          {step === 'backups' ? (
            <div className="stack" style={{ gap: 16 }}>
              <Card title="Backup storage">
                {storageNames.length === 0 ? (
                  <Callout tone="warn">
                    No backup storages in namespace <span className="mono">{targetNamespace}</span>. <Link to={`/settings/backup-storages?namespace=${encodeURIComponent(targetNamespace)}`}>Add an S3 storage</Link> to enable schedules and PITR. You can still create the cluster now.
                  </Callout>
                ) : null}
                <div className="stack" style={{ gap: 14, marginTop: storageNames.length === 0 ? 12 : 0 }}>
                  <SelectField
                    label="Primary storage"
                    value={spec.backup?.storage ?? ''}
                    onChange={(e) => update((s) => ({ ...s, backup: { ...(s.backup ?? {}), storage: e.target.value || undefined } }))}
                    hint="Default destination for on-demand backups and PITR oplog slices."
                    error={touched.backups && !spec.backup?.storage && ((spec.backup?.schedules?.length ?? 0) > 0 || spec.backup?.pitr?.enabled) ? 'Choose a primary storage for schedules and PITR.' : undefined}
                  >
                    <option value="">None</option>
                    {storageNames.map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </SelectField>
                  <Switch
                    label="Point-in-time recovery (continuous oplog capture)"
                    checked={spec.backup?.pitr?.enabled ?? false}
                    disabled={!spec.backup?.storage}
                    onChange={(e) => update((s) => ({ ...s, backup: { ...(s.backup ?? {}), pitr: { enabled: e.target.checked, oplogSpanMinutes: s.backup?.pitr?.oplogSpanMinutes ?? 10 } } }))}
                  />
                  {spec.backup?.pitr?.enabled ? (
                    <TextField
                      label="Oplog slice interval (minutes)"
                      type="number"
                      min={1}
                      style={{ maxWidth: 220 }}
                      value={spec.backup.pitr.oplogSpanMinutes}
                      onChange={(e) => update((s) => ({ ...s, backup: { ...(s.backup ?? {}), pitr: { enabled: true, oplogSpanMinutes: Number(e.target.value) } } }))}
                      hint="Smaller intervals tighten the recovery point; PITR requires at least one completed base backup."
                      error={touched.backups && (!Number.isInteger(spec.backup.pitr.oplogSpanMinutes) || spec.backup.pitr.oplogSpanMinutes < 1) ? 'Use a whole number of at least 1 minute.' : undefined}
                    />
                  ) : null}
                </div>
              </Card>
              <Card title="Schedules">
                <SchedulesEditor
                  value={spec.backup?.schedules ?? []}
                  onChange={(schedules) => update((s) => ({ ...s, backup: { ...(s.backup ?? {}), schedules } }))}
                  storages={storageNames}
                  defaultStorage={spec.backup?.storage}
                  backupTypes={provider.backupTypes}
                />
              </Card>
              <Card title="Seed data (optional)">
                <DataSourceEditor value={spec.dataSource} onChange={(dataSource) => update((s) => ({ ...s, dataSource }))} targetNamespace={targetNamespace || DEFAULT_NAMESPACE} namespaces={namespaceOptions} showErrors={touched.backups} />
              </Card>
            </div>
          ) : null}

          {step === 'access' ? (
            <div className="stack" style={{ gap: 16 }}>
              <Card title="Transport security">
                <TLSEditor value={spec.tls ?? { mode: 'requireTLS', ca: 'selfSigned' }} onChange={(tls) => update((s) => ({ ...s, tls }))} />
              </Card>
              <Card title="Network exposure">
                <ExposureEditor label={rules.exposureLabel} value={spec.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] }} onChange={(exposure) => update((s) => ({ ...s, exposure }))} allAddressesConfirmed={allAddressesConfirmed} onAllAddressesConfirmedChange={setAllAddressesConfirmed} />
              </Card>
              <Card title="Monitoring">
                <Switch label="Collect per-member metrics (dashboard and Prometheus endpoint)" checked={spec.monitoring?.enabled ?? true} onChange={(e) => update((s) => ({ ...s, monitoring: { enabled: e.target.checked } }))} />
              </Card>
            </div>
          ) : null}

          {step === 'review' ? (
            <Card title="Review">
              <div className="stack" style={{ gap: 16 }}>
                {allErrors.length ? (
                  <Callout tone="danger" title="Fix these before creating">
                    <ul style={{ margin: 0, paddingLeft: 18 }}>
                      {allErrors.map((e) => (
                        <li key={e}>{e}</li>
                      ))}
                    </ul>
                  </Callout>
                ) : null}
                {create.error ? <ErrorBlock error={create.error} /> : null}
                <KV
                  items={[
                    ['Namespace', <span className="mono">{targetNamespace}</span>],
                    ['Name', <span className="mono">{name}</span>],
                    ['Engine', `${provider.displayName} ${spec.version}`],
                    ['Topology', sharded ? `Sharded: ${spec.topology.shards} shards × ${spec.components.engine.replicas} members, ${spec.components.configServer?.replicas} config servers, ${spec.components.router?.replicas} routers` : `Replica set, ${spec.components.engine.replicas} members`],
                    ['Member resources', resourcesSummary(spec.components.engine.resources)],
                    spec.components.configServer && ['Config server resources', resourcesSummary(spec.components.configServer.resources)],
                    spec.components.router && ['Router resources', resourcesSummary(spec.components.router.resources, true)],
                    ['Total runtime footprint', `${footprint.members} instances · ${footprint.minCpu}–${footprint.maxCpu} CPU cores · ${footprint.minRam}–${footprint.maxRam} GiB RAM`],
                    ['Total data volumes', `${footprint.volumes} volumes · ${footprint.minDisk}–${footprint.maxDisk} GiB data disk`],
                    footprint.runtimeMaxDisk > 0 && ['Runtime disks (additional)', `${footprint.runtimeMinDisk}–${footprint.runtimeMaxDisk} GiB across ${footprint.members} runtime instances`],
                    ['Placement', 'Selected namespace/project; physical hosts are unknown until allocation. Zerops does not guarantee host anti-affinity.'],
                    ['Engine configuration', spec.engineConfig?.trim() || 'Provider defaults'],
                    ['Backup storage', spec.backup?.storage ?? 'none'],
                    ['Schedules', (spec.backup?.schedules ?? []).map((s) => `${s.name} (${s.cron}, ${s.type}, keep ${s.retainCount})`).join('; ') || 'none'],
                    ['PITR', spec.backup?.pitr?.enabled ? `enabled, ${spec.backup.pitr.oplogSpanMinutes} min slices` : 'disabled'],
                    spec.dataSource && ['Seed from', `${dataSourceLabel(spec.dataSource, targetNamespace)} ${spec.dataSource.backup ? `backup ${spec.dataSource.backup}` : `at ${spec.dataSource.pitrTime}`}`],
                    ['TLS', `${spec.tls?.mode ?? 'requireTLS'} · ${spec.tls?.ca === 'userProvided' ? 'user-provided certificates' : 'managed CA'}`],
                    ['Exposure', exposureSummary(spec)],
                    ['Monitoring', spec.monitoring?.enabled === false ? 'disabled' : 'enabled'],
                  ]}
                />
                <p className="small muted">These are aggregate autoscaling bounds, not a price estimate or the initial allocation. Billing follows <a href="https://zerops.io/pricing" target="_blank" rel="noreferrer">Zerops pricing</a> for runtime CPU/RAM, runtime disks, data volumes and other enabled resources; backup storage is additional.</p>
                <Callout tone="info" title="What this configuration means">
                  <ul style={{ margin: 0, paddingLeft: 18 }}>{engineConsequences(spec).map((consequence) => <li key={consequence}>{consequence}</li>)}</ul>
                </Callout>
              </div>
            </Card>
          ) : null}

          <div className="wizard-foot">
            <Button disabled={create.isPending} onClick={() => {
              if (stepIndex === 0) { idempotency.reset(); navigate('/clusters'); }
              else goto(STEPS[stepIndex - 1]!.id);
            }}>{stepIndex === 0 ? 'Cancel' : 'Back'}</Button>
            {step === 'review' ? (
              <Button variant="primary" onClick={(e) => submit(e.currentTarget)} loading={create.isPending}>
                Create cluster
              </Button>
            ) : (
              <Button variant="primary" onClick={() => {
                setTouched((t) => ({ ...t, [step]: true }));
                setStep(nextStep(step, validation));
              }}>
                Continue
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/** Drop empty optional envelopes so the server applies its own defaults. */
function cleanSpec(spec: ClusterSpec, targetNamespace: string): ClusterSpec {
  const out: ClusterSpec = structuredClone(spec);
  if (out.backup) {
    if (!out.backup.storage) delete out.backup.storage;
    if (!out.backup.schedules?.length) delete out.backup.schedules;
    if (!out.backup.pitr?.enabled && !out.backup.storage) delete out.backup.pitr;
    if (Object.keys(out.backup).length === 0) delete out.backup;
  }
  if (out.tls) out.tls = cleanTLS(out.tls);
  if (out.exposure && !out.exposure.public) out.exposure = { public: false, ipType: out.exposure.ipType };
  if (!out.engineConfig) delete out.engineConfig;
  if (out.dataSource) out.dataSource = wireDataSource(out.dataSource, targetNamespace);
  else delete out.dataSource;
  return out;
}

/**
 * Source namespace + cluster choice shared by clone and in-place restore.
 * Cross-namespace sources need backups/export on the source cluster, which
 * the server verifies; the hint only sets expectations from the policy the
 * UI can see.
 */
export function SourceClusterPicker({
  targetNamespace,
  namespaces,
  namespace,
  cluster,
  onChange,
  showErrors = false,
}: {
  targetNamespace: string;
  namespaces: string[];
  namespace: string;
  cluster: string;
  onChange: (namespace: string, cluster: string) => void;
  showErrors?: boolean;
}) {
  const auth = useAuth();
  const clusters = useClusters(namespace);
  const cross = namespace !== targetNamespace;
  const exportAllowed = !cross || !cluster || auth.can('backups', 'export', clusterObject(namespace, cluster));
  const options = namespaces.includes(namespace) ? namespaces : [...namespaces, namespace];
  return (
    <div className="stack" style={{ gap: 12 }}>
      <div className="grid-2">
        <SelectField label="Source namespace" value={namespace} onChange={(e) => onChange(e.target.value, '')} hint={cross ? 'Restoring across namespaces requires the backups:export permission on the source cluster.' : undefined}>
          {options.map((n) => (
            <option key={n} value={n}>
              {n}
              {n === targetNamespace ? ' (same namespace)' : ''}
            </option>
          ))}
        </SelectField>
        <SelectField
          label="Source cluster"
          value={cluster}
          onChange={(e) => onChange(namespace, e.target.value)}
          error={clusters.error ? errorMessage(clusters.error) : showErrors && !cluster ? 'Choose a source cluster.' : undefined}
          hint={clusters.data && clusters.data.items.length === 0 ? 'No readable clusters in this namespace.' : undefined}
        >
          <option value="">Choose…</option>
          {clusters.data?.items.map((c) => (
            <option key={c.name} value={c.name}>
              {c.name}
            </option>
          ))}
        </SelectField>
      </div>
      {exportAllowed ? null : (
        <Callout tone="warn">
          Your policy does not grant <code>backups:export</code> on <span className="mono">{clusterObject(namespace, cluster)}</span>; the server will refuse this cross-namespace restore.
        </Callout>
      )}
    </div>
  );
}

export function DataSourceEditor({ value, onChange, targetNamespace, namespaces, showErrors = false }: { value: Schemas['DataSource'] | undefined; onChange: (d: Schemas['DataSource'] | undefined) => void; targetNamespace: string; namespaces: string[]; showErrors?: boolean }) {
  const [mode, setMode] = useState<'backup' | 'pitr'>(value?.pitrTime ? 'pitr' : 'backup');
  const cluster = value?.cluster ?? '';
  // Wizard state always carries the source namespace (see data-source.ts); the
  // fallback only covers values typed as optional, never one this editor built.
  const namespace = value?.namespace || targetNamespace;
  return (
    <div className="stack" style={{ gap: 14 }}>
      <Switch label="Restore data from another cluster into this new cluster" checked={Boolean(value)} onChange={(e) => onChange(e.target.checked ? dataSourceFor(targetNamespace, '') : undefined)} />
      {value ? (
        <>
          <SourceClusterPicker targetNamespace={targetNamespace} namespaces={namespaces} namespace={namespace} cluster={cluster} onChange={(ns, c) => onChange(dataSourceFor(ns, c))} showErrors={showErrors} />
          <ChoiceGroup<'backup' | 'pitr'>
            name="ds-mode"
            legend="Restore point"
            value={mode}
            onChange={(m) => {
              setMode(m);
              onChange(dataSourceFor(namespace, cluster));
            }}
            options={[
              { value: 'backup', title: 'Backup', description: 'A completed backup of the source.' },
              { value: 'pitr', title: 'Point in time', description: 'Any instant inside the source PITR window.' },
            ]}
          />
          {mode === 'backup' ? (
            <BackupPicker namespace={namespace} cluster={cluster} value={value.backup} onChange={(backup) => onChange(dataSourceFor(namespace, cluster, { backup }))} showErrors={showErrors} />
          ) : (
            <PitrPicker namespace={namespace} cluster={cluster} value={value.pitrTime} onChange={(pitrTime) => onChange(dataSourceFor(namespace, cluster, { pitrTime }))} showErrors={showErrors} />
          )}
        </>
      ) : null}
    </div>
  );
}

export function BackupPicker({ namespace, cluster, value, onChange, showErrors = false }: { namespace: string; cluster: string; value: string | undefined; onChange: (b: string) => void; showErrors?: boolean }) {
  const [items, setItems] = useState<Schemas['Backup'][] | null>(null);
  const [err, setErr] = useState<unknown>(null);
  useEffect(() => {
    if (!cluster) return;
    let cancelled = false;
    setItems(null);
    setErr(null);
    unwrap(api.GET('/clusters/{cluster}/backups', { params: { path: { cluster }, query: { namespace, limit: 200 } } }))
      .then((r) => {
        if (!cancelled) setItems(r.items.filter((b) => b.phase === 'Succeeded'));
      })
      .catch((e) => {
        if (!cancelled) setErr(e);
      });
    return () => {
      cancelled = true;
    };
  }, [namespace, cluster]);
  if (!cluster) return <p className="small muted">Choose a source cluster first.</p>;
  if (err) return <ErrorBlock error={err} />;
  if (!items) return <Loading label="Loading backups…" />;
  return (
    <SelectField label="Backup" value={value ?? ''} onChange={(e) => onChange(e.target.value)} hint={items.length === 0 ? 'No completed backups on the source cluster.' : undefined} error={showErrors && !value ? 'Choose a completed backup.' : undefined}>
      <option value="">Choose…</option>
      {items.map((b) => (
        <option key={b.name} value={b.name}>
          {b.name} · {b.type} · {b.completedAt ? new Date(b.completedAt).toLocaleString() : ''}
        </option>
      ))}
    </SelectField>
  );
}

export function PitrPicker({ namespace, cluster, value, onChange, showErrors = false }: { namespace: string; cluster: string; value: string | undefined; onChange: (iso: string) => void; showErrors?: boolean }) {
  const query = useSourcePitrWindow(namespace, cluster);
  const win = query.data;
  if (!cluster) return <p className="small muted">Choose a source cluster first.</p>;
  if (query.error) return <ErrorBlock error={query.error} />;
  if (!win) return <Loading label="Reading PITR window…" />;
  if (!win.available || !win.start || !win.end) return <Callout tone="warn">{win.message ?? 'No continuous PITR window is available for this cluster.'}</Callout>;
  const toLocal = (iso: string) => {
    const d = new Date(iso);
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };
  const pointErrors = validatePitrPoint(value, win);
  return (
    <div className="stack">
      <p className="small muted">
        Restorable window: <strong>{new Date(win.start).toLocaleString()}</strong> → <strong>{new Date(win.end).toLocaleString()}</strong>
      </p>
      <TextField
        label="Point in time (local)"
        type="datetime-local"
        step={1}
        min={toLocal(win.start)}
        max={toLocal(win.end)}
        value={value ? toLocal(value) : ''}
        onChange={(e) => {
          onChange(e.target.value ? new Date(e.target.value).toISOString() : '');
        }}
        error={value || showErrors ? pointErrors[0] : undefined}
      />
      <div className="row">
        <Button size="sm" onClick={() => onChange(win.end!)}>
          Use latest ({new Date(win.end).toLocaleTimeString()})
        </Button>
      </div>
    </div>
  );
}
