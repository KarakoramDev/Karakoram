# 0029 — Elastic distribution licensing boundary

Status: Accepted

## Context

Karakoram is Apache-2.0; the official Elastic binaries are not. An open-source orchestrator or build recipe does not change the license of the deployed product. Elastic distinguishes default distributions, source-code license options and subscription feature tiers ([licensing FAQ](https://www.elastic.co/pricing/faq/licensing)).

## Decision

Keep Karakoram free of Elastic code and do not redistribute Elastic binaries. Download the official default Elasticsearch, Kibana, Logstash and APM Server distributions at deployment time on the user's own infrastructure, preserving license notices and controls. ECK supplies design references only; its ELv2 source is not copied into Karakoram.

Treat default distributions as Elastic-licensed products, with Elasticsearch/Kibana default binaries under Elastic License 2.0 (ELv2). AGPLv3 and SSPL are source-code options for eligible Elasticsearch/Kibana portions, not licenses for the downloaded default binaries and not an Apache-2.0 Elasticsearch distribution. Logstash and APM Server offer OSS builds, but Karakoram uses default distributions for consistent features and security wiring ([Logstash licensing](https://www.elastic.co/docs/reference/logstash/installing-logstash), [APM distributions](https://www.elastic.co/downloads/apm)).

Use Basic-tier features only: security with native/file realms, TLS, API keys, service accounts, snapshot/restore and monitoring APIs. Do not bypass license-key enforcement or activate paid features as part of provisioning. Free subscription entitlement is separate from the product's software license ([self-managed subscriptions](https://www.elastic.co/subscriptions)).

APM templates, ILM policies and ingest pipelines are supplied by Elasticsearch's built-in `apm-data` plugin on the supported versions, not by a bundled Karakoram Elastic artifact. Using these built-in assets or free Basic-tier APIs does not change the default distributions' licensing or the operator's managed-service obligations.

Document the managed-service restriction prominently. ELv2 restricts offering the software to third parties as a hosted/managed service exposing a substantial set of its features. Downloading on user infrastructure and not redistributing binaries does not exempt an operator who resells hosted Elasticsearch/Kibana. Such operators must obtain appropriate rights from Elastic. Use as an application's internal search backend is a different scenario from reselling direct product API/UI access; the actual offering needs legal review ([ELv2 text](https://www.elastic.co/licensing/elastic-license/), [ELv2 FAQ and examples](https://www.elastic.co/licensing/elastic-license/faq)).

Recommend legal review of the exact deployment/commercial model and release licenses. This record is an engineering licensing conclusion, not legal advice. Consider OpenSearch, whose core is Apache-2.0, as a possible later engine rather than relabeling it as Elasticsearch or implying current support ([OpenSearch license](https://github.com/opensearch-project/OpenSearch/blob/main/LICENSE.txt)).

## Consequences

Karakoram's license remains independent of downloaded products, but users/operators must comply with their terms. No redistribution is not a universal managed-service safe harbor. Basic-only provisioning does not remove ELv2 restrictions, and custom settings do not entitle users to paid capabilities. Default builds retain feature parity at the cost of Elastic-license obligations. Commercial resale requires review before deployment.

## Alternatives considered

- **Bundle Elastic binaries or copy ECK implementation:** adds distribution/source-license obligations and is unnecessary for independent orchestration.
- **Build Elasticsearch/Kibana from AGPL/SSPL source:** a different artifact, maintenance and compliance model; not the selected default-distribution workflow.
- **Use Logstash/APM OSS builds:** available, but a different feature/configuration surface from the version-locked default Stack.
- **OpenSearch now:** clearer Apache-2.0 core licensing, but a separate engine with its own security, lifecycle and compatibility contract; deferred rather than substituted for the requested Elastic engine.
