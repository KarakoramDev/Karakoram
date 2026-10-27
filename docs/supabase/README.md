<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Supabase

The `supabase` engine driver combines the [PostgreSQL core](../postgres/README.md) with modular native [Supabase](https://supabase.com/) services. This brief guide describes interfaces and limits, not deployment acceptance or complete parity with every upstream hosting feature.

Opt-in [cluster data MCP](../data-mcp.md) uses the PostgreSQL [pgEdge](https://www.pgedge.com/) companion and dedicated non-BYPASSRLS roles, not Supabase service credentials. Database/schema scope and PostgreSQL PUBLIC-grant limitations apply; access is disabled by default.

## At a glance

| Setting | Value |
| --- | --- |
| Database version | PostgreSQL **17 only**, default Percona 17.11.1; `spec.version` is the database version |
| Data topology | `single` (1), `ha` (3 or 5); core ProxySQL client endpoint |
| Default preset | `minimal`: gateway, Auth, PostgREST, plus database and proxy |
| Components | Separate stateless services with replicas/resources in `components.extra` |
| API entrypoint | Envoy, private HTTP 8000; optional provider-managed public HTTPS |
| Database access | [PostgreSQL native access API](../postgres/access-and-tls.md); internal services use a local raw TCP leader forwarder, not ProxySQL |
| Recovery | [pgBackRest/PITR](../postgres/backups.md) for database data, not separate S3 objects or control-plane secrets |

Component versions are locked to the Karakoram release. The current lock supplies every component in `minimal`, `standard` and `full`, including Studio 2026.10.12-bc27d2661edd, Edge Runtime 1.77.4 and [Supavisor](https://github.com/supabase/supavisor) 2.9.13. Standard/full require registered Storage configuration, and development builds need an artifact mirror or explicit `KARAKORAM_ARTIFACT_RELEASE` containing the exact locked source-built services and extension bundle. See [components and artifact availability](components.md).

The internal leader forwarder uses the provider's declared private IPv4 or IPv6 family for database connections and [Patroni](https://patroni.readthedocs.io/) probes, matching the PostgreSQL member's client-access rules. It does not fall back to a different address family when a member hostname resolves to both.

## Create

Inspect `karactl providers get supabase -o json`. Save `supabase.yaml`:

```yaml
name: appapi
spec:
  provider: supabase
  version: 17.11.1
  topology:
    type: single
  components:
    engine:
      replicas: 1
  engineOptions:
    siteUrl: https://app.example.com
    additionalRedirectUrls: []
    auth:
      disableSignup: false
      autoconfirm: false
      jwtExpiry: 3600
    rest:
      schemas: [public]
    gateway:
      publicHttps: false
```

```sh
karactl -n NS clusters create -f supabase.yaml --wait
karactl -n NS clusters status appapi -o json
karactl -n NS supabase credentials appapi
```

Omitted extra components receive the minimal defaults, including one proxy. TLS defaults to `requireTLS`. Public gateway HTTPS requires provider `publicHttps`; with it disabled, API URL is private `http://<cluster>gw:8000`. The management relay is not external application routing. Studio, when enabled, shares the gateway URL and requires dashboard basic authentication.

`supabase credentials` masks sensitive fields; `--show-secrets` explicitly reveals them and requires `cluster-credentials:read`. Browser credential reveal has the same boundary. Do not put the service-role/secret key, dashboard password or JWT secret into browser application code. Anon/publishable keys still require correctly configured database grants and row-level security.

## Options and limits

Options are a strict object. The PostgreSQL `synchronousMode`, `parameters` and `proxy` options remain available. Supabase adds `siteUrl`, `additionalRedirectUrls`, `auth`, `rest`, `storage`, `gateway`, `studio`, `extensions` and `functions`; see [component configuration](components.md) and [operations](operations.md). Passwords are not option fields; SMTP uses a write-only API.

- PostgreSQL 16/18 and cross-major database upgrades are rejected.
- Application/component versions cannot be selected independently of the release lock.
- Components and extensions without qualified locked bytes are rejected, even if build recipes exist. A mirror cannot add a missing lock entry.
- Separate database members do not establish host separation; auxiliary replicas are not a service-level HA guarantee.
- Database restores/clones are not complete application/object-store recovery. Preserve control-plane state/master key and protect [S3](https://aws.amazon.com/s3/) objects separately.
- Public application database clients still inherit [ProxySQL protocol limits](../postgres/access-and-tls.md#proxy-compatibility); the internal leader forwarder does not remove those limits.

See [ADR 0035](../adr/0035-supabase-engine.md) and [native artifact operations](../native-artifacts.md).
