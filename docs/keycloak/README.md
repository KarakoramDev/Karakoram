# Keycloak

Karakoram runs the official **[Keycloak](https://www.keycloak.org/) 26.8.1** native distribution on [Ubuntu](https://ubuntu.com/) 24.04 with `openjdk-21-jre-headless`. The archive URL, SHA-256, byte size, Apache-2.0 license and source revision are pinned in the native artifact lock. [Keycloak's supported configurations](https://www.keycloak.org/server/supported-configurations) include [OpenJDK](https://openjdk.org/) 21 and [PostgreSQL](https://www.postgresql.org/) 14–18.

Keycloak is an independent engine driver, not a PostgreSQL cluster variant. Create and make ready a `postgres` source first, then select `keycloak`, the `single` topology and one engine replica. The UI's Basics step accepts the source cluster and namespace. Engine driver-specific options are:

```json
{
  "database": {
    "cluster": "identitydb",
    "namespace": "default",
    "name": "keycloak"
  },
  "hostname": "https://login.example.com",
  "publicHttps": true,
  "proxyHeaders": "xforwarded"
}
```

`database.cluster` is required; omitted namespace uses the consumer namespace and omitted database name uses `keycloak`. The binding source and database are immutable after creation. Unknown fields, password fields, raw engine configuration, extra components, direct public TCP exposure and consumer backup schedules are rejected. Choose at least one CPU, 1.5 GiB RAM and 1 GiB runtime disk. The runtime's filesystem is disposable: authoritative realms, identities, sessions and configuration are in the bound PostgreSQL database.

## Database and credential boundary

Before deploying any runtime, the control plane ensures a binding with a dedicated non-superuser owning login and database. Only that login's password, resolved in-process through `pgbinding.Service.Password`, reaches the consumer. Source administrator credentials never reach Keycloak. JDBC uses the binding writer endpoint with `sslmode=verify-full` and an owner-only CA file. A source that cannot supply verify-full trust is rejected.

The managed JDBC URL sets `preferQueryMode=simple`. ProxySQL 3.0.11 rejects
named cursor portals and mixed text/binary result-column formats. Keycloak
26.8.1 configures Hibernate's statement fetch size to 64, which makes pgjdbc
use named portals inside transactions even if binary transfer or server-side
preparation is disabled. Simple mode avoids both protocol limitations, including
explicit JDBC fetch sizes; `defaultRowFetchSize=0` alone cannot override those.
pgjdbc still safely encodes JDBC `PreparedStatement` parameters, but does not
use server-side prepared statements or incremental cursor fetching. Results are
buffered in memory, so large queries can consume more member memory.
TLS hostname verification and the binding's writer endpoint remain unchanged;
the same configuration also works with direct singleton sources.

The same datasource supplies Liquibase, Hibernate and the default Infinispan
JDBC-ping discovery SQL; simple mode also covers discovery's prepared statements.
The managed scheduler uses Keycloak's basic timer, not a Quartz JDBC job store.
This is not support for custom extensions that require `LISTEN` or other
unsupported proxy features. During PostgreSQL primary failover, the writer
endpoint remains stable, but in-flight requests can fail until the JDBC pool
replaces failed connections. Verify new authentication and persisted realm/user
operations after recovery, not merely an already-cached session. See the
[binding failover boundary](../postgres/bindings.md#protocol-compatibility-and-primary-failover).

The setting is rendered by the member agent, not the control plane. Existing
consumers need a newly deployed agent; ordinary restart and `upgrade-agents`
are not a supported rollout for this provider. Follow the
[v0.4.1 consumer rollout procedure](../upgrade.md#keycloak-and-temporal-postgresql-proxy-compatibility),
including its data-retention warnings.

The member configuration is sealed at rest, including the generated temporary `admin` password under `keycloak/admin`. It is not returned through ordinary cluster reads. Member pulls are restricted to the allocated member identity and use the existing authenticated agent channel. On the runtime, database and bootstrap credentials live only in mode-0600 files under `<data-dir>/karakoram/keycloak/`; they are not placed in process arguments or environment variables. The optimized distribution is built with `kc.sh build --db=postgres --health-enabled=true` before `start --optimized`.

Each reconciliation resolves the binding again. Credential generation, endpoint, CA, hostname or proxy-header changes republish the sealed configuration and trigger an agent reload. The agent starts the configured runtime on first apply and stops then starts it when configuration changes, unless maintenance, an operator stop hold or the desired process list prevents startup. There is only one runtime, so rotation interrupts authentication. Applied configuration versions and management health must both match before the cluster is ready.

The desired generation is acknowledged after the sole live agent accepts its
configuration and runtime resources/routing have converged. Periodic health polls
continue without keeping that generation pending. Missing agents, stale applied
configuration and agent-reported configuration errors do not acknowledge it;
native management health still independently controls Ready.

## Routing and health

Application HTTP listens on private port **8080** for a trusted TLS-terminating ingress. Management HTTP on **127.0.0.1:9000** is not exposed by the runtime's port declarations. Native readiness calls `/health/ready` and requires HTTP 200 with status `UP`. Agent health is independently protected by cluster-scoped TLS on port 9443.

`hostname` accepts an explicit HTTP(S) origin. Omitting it disables strict hostname checks for private development only. `publicHttps` requires an explicit HTTPS origin and `forwarded` or `xforwarded` proxy headers; the infrastructure provider must support public HTTPS subdomain routing. Configure DNS and the ingress so the origin matches that routing. **Only trust an ingress that overwrites the selected headers**; otherwise clients can forge their external address/scheme. A private HTTP endpoint is not an Internet-facing production endpoint.

Create, replace and patch admission, including dry runs, reject `publicHttps: true`
when the namespace provider does not advertise `publicHttps`. This includes [Nomad](https://developer.hashicorp.com/nomad)
and [Dokploy](https://dokploy.com/) namespaces without configured HTTPS routing.

Exactly **one replica** is supported. Multi-instance JDBC-ping discovery has not been qualified for this engine driver; advertising three replicas without that proof would imply unverified availability. Runtime maintenance, resource changes and credential rotation cause downtime even when the source PostgreSQL cluster is highly available.

Resource reconciliation compares values, including an explicit `startCpu`, not
the identity of the decoded optional value. An unchanged resource specification
does not submit another deployment. After upgrading the control plane, a failed
redundant resize from an older build is retired only when its target equals the
previously accepted resources and the owned runtime still reports those resources.
The control plane then resumes agent/configuration and native readiness observation;
retiring the receipt alone does not mark the cluster Ready. Real failed resource
changes remain failures. This repair requires only a control-plane upgrade, not
replacement of an already serving Keycloak agent. Historical failed create
operations remain historical failures even if background convergence later recovers.

## Establishing an administrator and recovery

There is deliberately no new credential-reveal API: existing reveals are engine driver-specific, and Keycloak does not implement the database-user/role data-plane contract merely to expose a password. Use Keycloak's supported offline recovery command with the rendered database configuration:

1. Stop the Karakoram member agent/runtime using the infrastructure's maintenance controls. Prevent automatic restart while recovering. All Keycloak nodes must remain stopped.
2. As the runtime owner, set `JAVA_HOME` to `/usr/lib/jvm/java-21-openjdk-amd64`, then run:

   ```sh
   /opt/karakoram/keycloak-26.8.1/keycloak-26.8.1/bin/kc.sh \
     --config-file=<data-dir>/karakoram/keycloak/keycloak.conf \
     bootstrap-admin user --optimized
   ```

   Enter a distinct temporary administrator name and password at the interactive prompt; do not put passwords in command arguments, logs or shell history. Keep the existing protected file and CA in place so recovery uses exactly the same verified database connection.
3. Restart the runtime and sign in at the configured origin's `/admin/`. Create a permanent administrator with strong authentication. Remove both the recovery user and the original temporary `admin` account.

Startup bootstrap settings only create an administrator when the master realm is first initialized; changing them does not reset an existing account. See [upstream administrator recovery](https://www.keycloak.org/server/bootstrap-admin-recovery), including service-account recovery when realm policy prevents password login.

## Backups and deletion

Back up, restore and upgrade the **source PostgreSQL cluster**, not the disposable consumer runtime. A source restore affects every consumer sharing it; coordinate an outage and recovery across those consumers. A source cannot be deleted while the binding exists.

Consumer deletion first disables managed public routing and deletes the owned runtime, observing completion before releasing the binding. The binding identity is derived from the consumer and source specification, so deletion also releases a partially provisioned binding when the first binding attempt failed. Normal deletion drops the bound database and role. `keepVolumes` maps to binding `keepData`: preserve the database and disable the login (`NOLOGIN`). It does not preserve a Keycloak runtime volume. Do not treat deletion retention as a backup.

## Native qualification

The opt-in `TestNativeKeycloak` test in `internal/agent/keycloak` starts and stops its own native PostgreSQL and Keycloak processes. Set `KARAKORAM_KEYCLOAK_NATIVE=1`, `KEYCLOAK_HOME`, `JAVA_HOME` and `POSTGRES_HOME` to verified, unpacked distributions. The test uses the production `Render` function without changing the generated settings, verifies management readiness, obtains a bootstrap administrator token without logging it, and checks least-privilege role attributes and TLS sessions. It uses application port 8080 and loopback management port 9000; run it in an isolated environment where those ports are free. Multi-instance discovery is intentionally outside this qualification.
