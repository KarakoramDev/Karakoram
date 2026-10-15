// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

type Spec = Schemas['ClusterSpec'];

export interface Draft {
  engineReplicas: number;
  engineResources: Schemas['Resources'];
  shards: number;
  configReplicas: number;
  configResources: Schemas['Resources'] | undefined;
  routerReplicas: number;
  routerResources: Schemas['Resources'] | undefined;
  tls: Schemas['TLS'];
  exposure: Schemas['Exposure'];
  monitoring: boolean;
  engineConfig: string;
}

export function draftFrom(spec: Spec): Draft {
  return {
    engineReplicas: spec.components.engine.replicas,
    engineResources: { ...spec.components.engine.resources },
    shards: spec.topology.shards ?? 0,
    configReplicas: spec.components.configServer?.replicas ?? 0,
    configResources: spec.components.configServer ? { ...spec.components.configServer.resources } : undefined,
    routerReplicas: spec.components.router?.replicas ?? 0,
    routerResources: spec.components.router ? { ...spec.components.router.resources } : undefined,
    tls: { mode: spec.tls?.mode ?? 'requireTLS', ca: spec.tls?.ca ?? 'selfSigned' },
    exposure: { public: spec.exposure?.public ?? false, ipType: spec.exposure?.ipType ?? 'IPV6', allowlist: [...(spec.exposure?.allowlist ?? [])] },
    monitoring: spec.monitoring?.enabled ?? true,
    engineConfig: spec.engineConfig ?? '',
  };
}

/** Form values may include defaults that an engine's patch builder deliberately omits. */
export function specFromDraft(spec: Spec, d: Draft): Spec {
  return {
    ...spec,
    topology: { ...spec.topology, ...(spec.topology.type === 'sharded' ? { shards: d.shards } : {}) },
    components: {
      ...spec.components,
      engine: { replicas: d.engineReplicas, resources: d.engineResources },
      ...(d.configResources ? { configServer: { replicas: d.configReplicas, resources: d.configResources } } : {}),
      ...(d.routerResources ? { router: { replicas: d.routerReplicas, resources: d.routerResources } } : {}),
    },
    tls: d.tls,
    exposure: d.exposure,
    monitoring: { enabled: d.monitoring },
    engineConfig: d.engineConfig,
  };
}

/** Build an RFC 7396 merge patch containing only what changed. */
export function buildPatch(spec: Spec, d: Draft): Record<string, unknown> {
  const patch: Record<string, unknown> = {};
  const sharded = spec.topology.type === 'sharded';
  const components: Record<string, unknown> = {};
  const engine: Record<string, unknown> = {};
  if (d.engineReplicas !== spec.components.engine.replicas) engine.replicas = d.engineReplicas;
  if (JSON.stringify(d.engineResources) !== JSON.stringify(spec.components.engine.resources)) engine.resources = d.engineResources;
  if (Object.keys(engine).length) components.engine = engine;
  if (sharded) {
    if (d.shards !== (spec.topology.shards ?? 0)) patch.topology = { shards: d.shards };
    const cfg: Record<string, unknown> = {};
    if (d.configReplicas !== (spec.components.configServer?.replicas ?? 0)) cfg.replicas = d.configReplicas;
    if (d.configResources && JSON.stringify(d.configResources) !== JSON.stringify(spec.components.configServer?.resources)) cfg.resources = d.configResources;
    if (Object.keys(cfg).length) components.configServer = cfg;
    const router: Record<string, unknown> = {};
    if (d.routerReplicas !== (spec.components.router?.replicas ?? 0)) router.replicas = d.routerReplicas;
    if (d.routerResources && JSON.stringify(d.routerResources) !== JSON.stringify(spec.components.router?.resources)) router.resources = d.routerResources;
    if (Object.keys(router).length) components.router = router;
  }
  if (Object.keys(components).length) patch.components = components;

  const tls: Record<string, unknown> = {};
  if (d.tls.mode !== (spec.tls?.mode ?? 'requireTLS')) tls.mode = d.tls.mode;
  if (d.tls.ca !== (spec.tls?.ca ?? 'selfSigned')) tls.ca = d.tls.ca;
  if (d.tls.ca === 'userProvided') {
    if (d.tls.caCertificate?.trim()) tls.caCertificate = d.tls.caCertificate;
    if (d.tls.caPrivateKey?.trim()) tls.caPrivateKey = d.tls.caPrivateKey;
    if (d.tls.certificate?.trim()) tls.certificate = d.tls.certificate;
    if (d.tls.privateKey?.trim()) tls.privateKey = d.tls.privateKey;
  }
  if (Object.keys(tls).length) patch.tls = tls;

  const curExp = spec.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] };
  if (d.exposure.public !== curExp.public || d.exposure.ipType !== (curExp.ipType ?? 'IPV6') || JSON.stringify(d.exposure.allowlist ?? []) !== JSON.stringify(curExp.allowlist ?? [])) {
    patch.exposure = { public: d.exposure.public, ipType: d.exposure.ipType, allowlist: d.exposure.public ? d.exposure.allowlist ?? [] : [], allowAllAddresses: d.exposure.public && Boolean(d.exposure.allowAllAddresses) };
  }
  if (d.monitoring !== (spec.monitoring?.enabled ?? true)) patch.monitoring = { enabled: d.monitoring };
  if (d.engineConfig !== (spec.engineConfig ?? '')) patch.engineConfig = d.engineConfig || null;
  return patch;
}
