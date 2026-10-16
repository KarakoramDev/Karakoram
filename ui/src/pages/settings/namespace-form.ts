// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

export type Backend = Schemas['BackendInfo'];

export interface NamespaceForm {
  name: string;
  projectId: string;
  token: string;
  description: string;
  backend: string;
  config: Record<string, string>;
}

export function selectedBackend(backends: Backend[], kind: string): Backend | undefined {
  return backends.length === 1 ? backends[0] : backends.find((backend) => backend.kind === kind);
}

export function namespaceCreateBody(form: NamespaceForm, backend: Backend): Schemas['NamespaceCreate'] {
  const config = Object.fromEntries(backend.configKeys.flatMap((key) => {
    const value = form.config[key]?.trim();
    return value ? [[key, value]] : [];
  }));
  return {
    name: form.name,
    projectId: form.projectId.trim(),
    token: form.token,
    backend: backend.kind,
    ...(form.description.trim() ? { description: form.description.trim() } : {}),
    ...(Object.keys(config).length ? { config } : {}),
  };
}
