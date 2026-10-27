# 0043 — Per-cluster data MCP gateway

Status: Accepted

## Context

AI agents increasingly expect a [Model Context Protocol](https://modelcontextprotocol.io) (MCP) endpoint for the data inside a database, not only for its lifecycle. Usable open-source servers exist for [MongoDB](https://www.mongodb.com) and [PostgreSQL](https://www.postgresql.org), but most have no inbound authentication, their write gates and confirmations fail open, and none fit Karakoram's identity model. [Elasticsearch](https://www.elastic.co/elasticsearch) and [RabbitMQ](https://www.rabbitmq.com) have no server that is both maintained and safe to gate. Data access is a personal-data decision for the operator, so it must be off by default and narrower than lifecycle access.

## Decision

Serve one MCP endpoint per cluster at `https://<karakoram>/mcp/clusters/{namespace}/{cluster}` on the existing public listener, implemented in `internal/datamcp` with the [Go](https://go.dev) MCP SDK. The gateway is an MCP-aware proxy, not a byte-level reverse proxy:

- **Authentication and authorization.** Every request carries a restricted Karakoram API token. Cookies are rejected, the `Origin` header must be absent or match the public URL, and bodies are capped. Data access requires all of: the cluster has `spec.dataMcp.enabled`; the token restrictions name the cluster (or its namespace) and explicitly list `cluster-data:read` or `cluster-data:write`; and the token owner holds RBAC `cluster-data:read|write` on the cluster. Unrestricted or legacy tokens and browser sessions never receive data access. The `cluster-data` resource is not granted to the built-in viewer or operator roles.
- **Writes need four switches:** cluster `spec.dataMcp.access: readWrite`, a token with `cluster-data:write` and without `readOnly`, owner RBAC `cluster-data:write`, and the installation switch `KARAKORAM_MCP=readwrite`. Missing any one hides write tools.
- **Tool manifests.** Each pinned upstream version has a reviewed manifest mapping tool names to `read`, `write`, `destructive` or `deny`, with optional argument rules (numeric clamps and defaults, allowed or denied values, forbidden arguments). Tools absent from the manifest are denied, so upstream releases cannot add tools silently. `tools/list` is filtered by effective access and `spec.dataMcp.tools.deny`; calls to hidden tools are refused.
- **Confirmation.** Destructive tools (and write tools when `tools.confirmWrites` is set) need a gateway-run confirmation bound to the cluster, token, tool and argument hash. Clients that cannot elicit are refused. Upstream elicitation is never advertised to the upstream.
- **Untrusted data.** Results are bounded and wrapped in a per-call nonce boundary that states the content is data, not instructions.
- **Audit and limits.** Every `tools/call` records an `Audit` event (`resource: cluster-data`) with actor, tool, class, outcome, duration and a SHA-256 of the canonical arguments; argument text only with `spec.dataMcp.auditQueryText`. Calls are rate-limited per token and bounded in concurrency per cluster.

Tools come from an upstream chosen by the engine provider (`engine.DataMcpProvider`):

- **Companion (MongoDB/[PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb): `mongodb-mcp-server`; PostgreSQL and [Supabase](https://supabase.com): `pgedge-postgres-mcp`).** One private stateless service `<cluster>mcp` on [Ubuntu](https://ubuntu.com) 24.04 runs the Karakoram agent with the `datamcp` adapter, which supervises the pinned upstream installed from the native artifact lock ([ADR 0036](0036-native-artifacts.md)). The upstream binds to `127.0.0.1` and requires a per-process local secret; credentials reach it through environment variables, never arguments. The agent exposes it only below `/v1/datamcp/` on its authenticated private HTTPS listener, and the gateway reaches that listener through the existing agent dialer and relay (port 9443, no new relay port). Telemetry, connection switching, Atlas, assistant, export, LLM and embedding features are disabled. With `readWrite`, a second upstream process runs with the read-write identity; the read-only process never holds write credentials.
- **Native (Elasticsearch, RabbitMQ).** Small in-process Go toolsets use the data-access identity over the namespace dialer. RabbitMQ message peeking consumes and requeues messages and is therefore classified `write`; queue purge is destructive. Hidden and system indices are never reachable.

The companion is managed by one engine-neutral controller called from the existing reconciler after provider synchronization and during cluster deletion; providers keep their own state machines. It uses the shared deployment guard and backend admission, ownership markers, the cluster CA for its agent certificate, and a separate durable state record.

The controller stores its import plan, service ID and operation receipts in the sealed cluster secret `datamcp/state`, and publishes member configuration in `datamcp/config`. Neither shares `Cluster.Internal`, which providers may replace wholesale. The frozen import plan is durable before submission; deletion verifies ownership and the recorded service ID, and cluster deletion (including volume-preserving suspension) waits for the companion. The default companion has shared CPU and 0.25–0.5 GiB RAM, overridable by `spec.dataMcp.resources`. Credential/spec changes and certificate renewal advance the published configuration version and request an agent reload. Companion readiness is composed into `status.dataMcp`, never into database membership.

The adapter owns `mcp-ro` and `mcp-rw`, with protected configuration under the node data directory's `karakoram/datamcp/<process>` directories. MongoDB receives credentials and required local HTTP authorization through its environment; pgEdge receives database credentials through a mode-0600 YAML file and validates a SHA-256 token file. Neither receives secrets in command arguments. `/v1/datamcp/ro` and `/v1/datamcp/rw` preserve streaming responses, replace the agent bearer with the process-local bearer, and return a generic error on upstream transport failures. `POST /v1/datamcp/readiness` (also available as `GET`) reports `configVersion`, upstream `version`, and a `processes` map of `{healthy: bool}`; it returns 503 unless every configured process answers an authenticated MCP initialization probe.

Each enabled cluster gets Karakoram-managed database identities `karakoram-mcp-ro` and, only with `readWrite`, `karakoram-mcp-rw`. Grants are least-privilege and scoped by `spec.dataMcp.scope`; read-only access is enforced by the database as well as by the tool list. The identities are system users: generated, sealed, rotatable through the existing rotation operation, never revealed through the API, and removed when no longer needed.

`karactl mcp config --cluster <ns>/<name> [--write]` mints a cluster-scoped restricted token and prints client configuration for the endpoint.

## Consequences

Enabling data access on MongoDB, PostgreSQL or Supabase adds one small companion service (two upstream processes with writes). Every upstream version bump needs a manifest review and conformance run. Prompt injection from stored data cannot be prevented; the controls are the read-only identity, per-cluster scope, confirmation and the absence of credential tools. Agent-generated queries can still load production; limits rely on caps in the upstream, role timeouts and gateway rate limits. Elicitation and resources from upstreams are not proxied.

## Alternatives considered

- **Expose vendor servers directly:** a different auth scheme per engine (or none), a new public surface per cluster, and fail-open confirmation.
- **One aggregated endpoint for all clusters:** tool-count explosion and cross-cluster exfiltration in a single agent turn; per-cluster consent is impossible.
- **Run upstreams inside the control plane:** concentrates every cluster's credentials and memory in one process; Node and Python children cannot use the namespace dialer.
- **Google MCP Toolbox as one multi-database server:** no RabbitMQ, no read-only enforcement, a 301 MiB binary and weaker MongoDB and Elasticsearch tools.
- **Community Elasticsearch and RabbitMQ servers:** need a Python runtime bundle and argument rewriting; their read-only modes either hide essential tools or can be bypassed.

## References

[Control-plane MCP](0040-agent-interface-mcp.md), [component model](0031-engine-component-model.md), [native artifacts](0036-native-artifacts.md), [relay](0018-multi-project-namespaces-relay.md), [secrets and users](0012-secrets-users-tls.md), [data MCP guide](../data-mcp.md).
