// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import type { Schemas } from '../../api/client';

export const API_MODULES = [
  'text2vec-aws', 'multi2vec-cohere', 'text2vec-cohere', 'text2vec-databricks', 'text2vec-google', 'multi2vec-google', 'text2vec-huggingface', 'text2vec-jinaai', 'multi2vec-jinaai', 'text2vec-mistral', 'text2vec-octoai', 'text2vec-openai', 'generative-deepseek', 'text2vec-digitalocean', 'text2vec-morph', 'text2vec-voyageai', 'multi2vec-voyageai', 'text2vec-weaviate', 'text2multivec-jinaai', 'text2vec-nvidia', 'multi2vec-nvidia', 'multi2vec-twelvelabs', 'multi2multivec-jinaai', 'multi2multivec-weaviate', 'multi2vec-aws', 'generative-anthropic', 'generative-anyscale', 'generative-aws', 'generative-cohere', 'generative-contextualai', 'generative-databricks', 'generative-digitalocean', 'generative-friendliai', 'generative-google', 'generative-meta', 'generative-mistral', 'generative-nvidia', 'generative-octoai', 'generative-openai', 'generative-xai', 'reranker-cohere', 'reranker-contextualai', 'reranker-voyageai', 'reranker-jinaai', 'reranker-nvidia',
] as const;
export const INFERENCE_MODULES = ['text2vec-transformers', 'multi2vec-clip', 'reranker-transformers', 'multi2vec-bind', 'img2vec-neural', 'text2vec-model2vec', 'text2vec-gpt4all', 'ner-transformers', 'qna-transformers', 'sum-transformers', 'text-spellcheck'] as const;
export function options(spec: Schemas['ClusterSpec']) {
  return { modules: [] as string[], inference: {} as Record<string, { url: string }>, replicationMinimumFactor: spec.topology.type === 'cluster' ? 3 : 1, asyncReplication: true, queryDefaultsLimit: 25, diskWarningPercent: 80, diskReadonlyPercent: 90, ...spec.engineOptions };
}
export function normalizeOptions(spec: Schemas['ClusterSpec']): Schemas['ClusterSpec'] {
  return spec.provider === 'weaviate' ? { ...spec, engineOptions: options(spec) } : spec;
}
export function validateSpec(spec: Schemas['ClusterSpec']): string[] {
  const errors: string[] = [];
  const replicas = spec.components.engine.replicas;
  if (!Number.isInteger(replicas) || (spec.topology.type === 'cluster' ? replicas < 3 || replicas > 9 : spec.topology.type !== 'single' || replicas !== 1)) errors.push('Weaviate single requires 1 member; cluster requires 3–9 members.');
  const o = options(spec);
  if (!Number.isInteger(o.replicationMinimumFactor) || o.replicationMinimumFactor < 1 || o.replicationMinimumFactor > replicas) errors.push('Replication minimum factor must be an integer between 1 and the member count.');
  if (!Number.isInteger(o.queryDefaultsLimit) || o.queryDefaultsLimit < 1 || o.queryDefaultsLimit > 10000) errors.push('Query defaults limit must be an integer between 1 and 10000.');
  if (!Number.isInteger(o.diskWarningPercent) || !Number.isInteger(o.diskReadonlyPercent) || o.diskWarningPercent < 1 || o.diskWarningPercent >= o.diskReadonlyPercent || o.diskReadonlyPercent > 95) errors.push('Disk percentages must satisfy 1 ≤ warning < readonly ≤ 95.');
  if (typeof o.asyncReplication !== 'boolean') errors.push('Async replication must be a boolean.');
  if (!Array.isArray(o.modules) || o.modules.some((m) => !(API_MODULES as readonly string[]).includes(m)) || new Set(o.modules).size !== o.modules.length) errors.push('Select unique supported API modules; backup-s3 is managed.');
  if (!o.inference || typeof o.inference !== 'object' || Array.isArray(o.inference)) errors.push('Inference endpoints must be an object.');
  else for (const [module, endpoint] of Object.entries(o.inference)) {
    let valid = (INFERENCE_MODULES as readonly string[]).includes(module);
    try {
      const url = new URL(endpoint.url);
      valid = valid && ['http:', 'https:'].includes(url.protocol) && Boolean(url.hostname) && !url.username && !url.password && !endpoint.url.includes('?') && !endpoint.url.includes('#');
    } catch { valid = false; }
    if (!valid) errors.push(`Inference ${module} requires a supported module and an HTTP(S) URL without credentials, query or fragment.`);
  }
  if (spec.engineConfig) errors.push('Weaviate does not accept raw engine configuration; use engine options.');
  return errors;
}
export function specSummary(spec: Schemas['ClusterSpec']): string {
  return `${spec.components.engine.replicas} Weaviate member${spec.components.engine.replicas === 1 ? '' : 's'} · ${spec.topology.type === 'cluster' ? '3 fixed Raft voters' : 'no redundancy'}`;
}
