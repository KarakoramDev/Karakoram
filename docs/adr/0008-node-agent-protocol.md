# 0008 — Pull configuration and a bounded authenticated node agent

Status: Accepted

## Context

Zerops has no public container-exec API. MongoDB administration can use the wire protocol, but local files, process supervision and PBM physical restore require a process inside the member. Control-plane unavailability must not prevent an existing member from restarting.

## Decision

Run `karakoram agent` as the member's supervisor. Pull `GET /internal/agent/v1/config` from the control plane's private `https://karakoram:9443` in namespace `default`, or `https://<relay hostname>:9443` in another namespace (ADR 0018). The relay forwards bytes to the existing internal TLS listener; TLS remains end-to-end agent-to-control-plane, verified against the pinned internal CA. Reissue the internal server leaf under the same CA when a required relay hostname SAN is missing. Authenticate with a random per-member Bearer token, storing its hash plus an encrypted recoverable copy in control-plane state. Tokens authorize their assigned namespace/cluster/member only; they are not public API credentials.

Return versioned `MemberConfig`: role, replset, config overrides, keyfile, engine TLS material, agent server certificate/key, required local system credentials, PBM/process intent and maintenance mode. Cache it at `{DATA_DIR}/karakoram/config.json` with mode 0600; pull every 60 seconds and on reload. Render protected configuration files and supervise mongod/mongos/pbm-agent, forwarding SIGTERM for clean shutdown. Mongos without a data volume cannot rely on cache persistence across redeploys and must refetch configuration then.

For control-plane-to-agent HTTPS on private port 9443, verify the agent certificate and require the same member token. Limit commands to status, reload, named process start/stop/restart, maintenance, idempotent localhost bootstrap, whitelisted PBM actions, bounded logs and metrics. Never expose a shell or arbitrary exec endpoint. Maintenance explicitly disables automatic mongod restart during physical restore.

## Consequences

The agent supplies the node-local seam missing from Zerops, while the control plane retains MongoDB topology authority. Cached member configuration contains secrets and requires filesystem protection. Initial bootstrap/redeploy without a cache needs the control plane. Per-member token/certificate rotation must coordinate both directions; a private network alone is not authentication. Unknown configuration versions must not silently produce divergent files.

## Alternatives considered

- **VPN plus SSH orchestration:** requires external connectivity and broad remote-shell authority.
- **Startup scripts only:** cannot safely coordinate ongoing restore, rotation and supervision.
- **Generic remote exec agent:** unnecessary command authority and a larger attack surface.

## Multi-engine runners

`KARAKORAM_ENGINE` selects the node runner; empty or `psmdb` retains the built-in MongoDB/PBM path. Other runners implement the in-process `agent.Engine` interface and receive a `Node` toolkit for atomic owner-only files under `DATA_DIR/karakoram`, whitelisted supervision, configuration, logs and local safety holds. `MemberConfig.engine` identifies the runner (empty means `psmdb`); `engineSpec` is an engine-owned JSON document. Configuration must match both member identity and engine. The shared loop retains cached startup, five-second first-pull retries, sixty-second refresh, pinned TLS, bearer authentication and bounded child shutdown. Maintenance and explicit process-stop holds persist independently in `controls.json`; configuration pulls cannot release them or restart children during maintenance. Generic status, reload, process, maintenance, logs and Prometheus metrics routes are shared; runner routes are confined to `/v1/<engine>/` behind the same authentication. MongoDB bootstrap and PBM endpoints remain PSMDB-only. Readiness forwards additional arguments to the selected runner; unknown engines fail explicitly. Runners are compiled into the executable, not remote-exec plugins.

Engine route actions serialize safety-hold checks and process transitions with generic controls and configuration application. The toolkit passed to `Apply` is scoped to that synchronous call and exposes its pending configuration; status publishes the new applied configuration/certificate/version only after the protected cache write succeeds. Generic CPU counters carry process labels; monitoring retains independent per-target/process baselines and sums reset-aware rates into the bounded member CPU percentage.

`Node.WithLock(ctx, callback)` lets a bounded engine command serialize its entire operation with Apply and generic controls, rather than only its supervisor actions. Lock acquisition observes context cancellation; callbacks bound their own work with that context. The callback receives a scoped toolkit supporting nested Actions/WithLock calls without recursive locking. Engine-private locks are acquired inside this callback, preserving the control-lock-before-engine-lock ordering.

Engine-owned client actions use a separate three-minute default HTTP deadline (`ClientOptions.EngineTimeout`), while ordinary agent requests retain their shorter timeout. Both clients share pinned TLS, member authentication and redirect refusal. The caller's context can shorten either deadline. Engine and action names are validated before constructing the bounded `/v1/<engine>/<action>` route.
