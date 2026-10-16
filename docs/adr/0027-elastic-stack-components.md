# 0027 — Elastic Stack components belong to the cluster

Status: Accepted; shared component mechanics generalized by [ADR 0031](0031-engine-component-model.md)

## Context

Kibana, Logstash and APM Server need compatible Elasticsearch versions, trusted CA material and separate backend identities. ECK represents them as independent resources with association controllers; Karakoram's current requirement is a coupled stack on plain runtime services, not a general graph of linked applications ([ECK associations](https://www.elastic.co/docs/deploy-manage/deploy/cloud-on-k8s/k8s-kibana-es), [Logstash associations](https://www.elastic.co/docs/deploy-manage/deploy/cloud-on-k8s/advanced-configuration-logstash)).

## Decision

Represent optional `kibana`, `logstash` and `apm` under `spec.components.extra`, with per-component resources and replica counts. Zero disables the component. Lock every enabled product to the exact Elasticsearch version; cluster create, upgrade, restore, credential rotation and deletion own their lifecycle. Establish Elasticsearch/security first, then Kibana, Logstash and APM. On upgrade, finish Elasticsearch before components and stop every Kibana instance before changing its version ([Stack installation](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/installing-elasticsearch), [Kibana upgrade](https://www.elastic.co/docs/deploy-manage/upgrade/deployment-or-cluster/kibana)).

The ordered `ComponentSet` and stateless lifecycle helper in ADR 0031 now provide shared defaults, validation, topology metadata and import/deploy/readiness/scale/delete transitions. Elasticsearch retains the security, configuration, version compatibility and safety ordering described in this decision.

Use one stateless service per component with N containers and no Local Storage volume, following the router pattern in [ADR 0007](0007-member-deployment.md). All replicas receive the cluster CA and automatically generated backend credentials: a Kibana service-account token and separate scoped Logstash/APM writer users. Share stable Kibana session, saved-object and reporting encryption keys across its replicas, but let instance identities remain unique ([Kibana load balancing](https://www.elastic.co/docs/deploy-manage/production-guidance/kibana-load-balance-traffic)). Browser and APM-agent credentials are separate from component-to-Elasticsearch identities.

Keep private endpoints by default. `spec.engineOptions.kibana.public` enables Zerops subdomain L7 HTTPS to Kibana's internal HTTP port 5601; set the public base URL and secure cookies. `spec.engineOptions.apm.public` optionally enables the same route to APM intake on 8200. TLS ends at the balancer on these routes; component-to-Elasticsearch traffic remains HTTPS with CA verification. Zerops documents a 50-MB upload cap and a shared central subdomain balancer that is not recommended for production. Custom-domain management is out of scope, so optional subdomain exposure is not a production-ingress guarantee ([public access](https://docs.zerops.io/references/networking/public-access), [Kibana proxy settings](https://www.elastic.co/docs/reference/kibana/configuration-reference/general-settings)).

Expose Elasticsearch HTTP only through the existing optional public TCP policy in [ADR 0017](0017-public-tcp-exposure.md), routed to port 9200 on data/combined nodes with allowlists and end-to-end Elasticsearch TLS. Keep dedicated masters and transport port 9300 private. Public Kibana/APM switches do not expose Elasticsearch or Logstash inputs.

Connection endpoints retain names `elasticsearch`, `kibana` and `apm` for private and public entries; public TCP/subdomain entries carry `public: true`, without a `-public` suffix. Logstash exposes `logstash-beats` (`tcp`, 5044) and `logstash-http` (`http`, 8080). The protected connection response reveals credential entries `elastic`, `apm-secret-token` and the cluster CA; it does not reveal component backend credentials.

Render user Logstash pipelines and inject Elasticsearch hosts, writer credentials and CA path as environment variables rather than embedding secrets in pipeline source. Default inputs are dual-stack Beats on 5044 and HTTP on 8080, both binding `::`, and write data streams. If persistent queues are enabled, store them under `DATA_DIR/karakoram/data` on each container's runtime disk: they survive process restarts, but are lost on redeploy or replacement. They are not replicated, backed up or durable across container lifecycle ([persistent queues](https://www.elastic.co/docs/reference/logstash/persistent-queues)).

Standalone APM Server listens dual-stack on `[::]:8200`. Elasticsearch's built-in `apm-data` plugin installs APM index templates, ILM policies and ingest pipelines on 8.15+; those assets were observed live on 9.5.5. Do not introduce a separate Karakoram APM setup job or mistake asset installation for verified event delivery. The full live three-node/component matrix remains unverified.

## Consequences

A cluster is the stack's ownership/version boundary; users cannot upgrade or attach components independently or share them across clusters. Coupled security wiring reduces setup work but rotation and restore must propagate credentials before components resume. Kibana upgrades have downtime. Small stateless disks constrain Logstash buffering, and container replacement can lose queued events. Public subdomains inherit platform limits; private access remains the preferred default.

## Alternatives considered

- **Separate linked resources like ECK:** permits independent lifecycles and many-to-many associations, but needs additional ownership, version compatibility and credential controllers without a current requirement.
- **Public TCP for Kibana/APM:** would require application-side HTTPS/certificates and browser endpoint management; use the existing platform HTTP route for this optional surface.
- **A persistent volume per Logstash replica:** stronger queue persistence, but changes the stateless service/replica model; not part of this decision.
- **Use `elastic` for every component:** simpler wiring, but turns routine ingestion/service work into superuser access.
