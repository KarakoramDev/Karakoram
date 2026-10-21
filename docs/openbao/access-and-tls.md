<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# OpenBao access and TLS

## API access

OpenBao's customer API is HTTPS on **8200**. Use the advertised endpoints and cluster CA; authenticate with OpenBao tokens and policies, not a Karakoram API token. Karakoram's connection response contains endpoints and CA but **no root or management credentials** and no system user. The private cluster/forwarding listener on **8201** uses OpenBao-managed TLS. Never expose it, private recovery port 8202, member control port 9443, or the loopback metrics listener on 8210 publicly.

```sh
karactl -n NS clusters connection secrets -o json
karactl -n NS clusters ca secrets > openbao-ca.pem
export BAO_ADDR=https://secrets.example.com:8200
export BAO_CACERT="$PWD/openbao-ca.pem"
bao status
```

Replace the illustrative address with an advertised, client-reachable endpoint whose hostname matches the certificate. Exporting the CA requires `cluster-credentials:read`. Provision least-privilege native OpenBao policies/tokens for applications; do not use the initial root token for routine requests.

## TLS and exposure

Only `spec.tls.mode: requireTLS` is accepted; neither `preferTLS` nor `disabled` is supported. CA sources are `selfSigned` (default) and `userProvided`; use the [shared supplied-PKI contract](../clusters.md). Client connections verify the cluster CA and server hostname; do not use `BAO_SKIP_VERIFY` to work around a routing/SAN problem. Listener TLS is at least TLS 1.2.

Private endpoints are the default. Public API exposure is opt-in through `spec.exposure.public` with a nonempty CIDR `allowlist` and `ipType` (`IPV4` or default `IPV6`). The [public-exposure acknowledgement rules](../security.md#public-exposure-allowlists) also apply. Public routing is not proof of reachability: test allowed and denied clients independently. A namespace relay is management connectivity, not an application proxy.

TLS rotation reloads listener certificate files via SIGHUP, one member at a time, without restarting or re-unsealing OpenBao. See [maintenance](operations.md#rolling-maintenance).

### Private control-plane recovery

OpenBao 2.7.1 disables the deprecated, unauthenticated `sys/generate-root/*` workflow by default. Its authenticated replacement cannot recover access when all native tokens are gone. Karakoram enables the shares-based workflow only on a dedicated private **8202** listener protected by mandatory mutual TLS. API and metrics listeners keep it disabled; legacy rekey is not enabled. The control plane verifies the member's cluster CA and hostname and presents a client certificate from an independent, per-cluster recovery CA. Ordinary member certificates, absent client certificates and another cluster's identity are rejected at TLS.

Only that recovery CA's public certificate reaches member configuration. Its signing key and the recovery client certificate/private key stay encrypted in the control-plane SecretStore; agents never receive the client key or shares. Port 8202 is not a customer endpoint, public mapping or generic service proxy. An authenticated namespace relay may carry the opaque mTLS connection to an exact private member target. There is no ordinary-API fallback.

The recovery listener remains available for unattended management-token expiry and snapshot recovery. Possession of the recovery client identity permits attempt initiation/cancellation, so its compromise can deny service to an in-flight generation attempt; it does not replace the required native share threshold. Protect control-plane secret-store/master-key access and private management connectivity. This is an intentional constrained opt-in to the [upstream deprecated workflow](https://openbao.org/community/deprecation/unauthed-generate-root), not a customer authentication mechanism.


## Reveal and store recovery material offline

Karakoram stores initialization material encrypted in its secret store: Shamir shares, or recovery shares for auto-unseal, threshold, seal type and initial root token. The control plane also holds a separate periodic management token, which is never published as a customer credential. Loss of the installation master key can prevent automatic unseal and recovery. Back up consistent control-plane state and that key separately; reveal shares and store them securely offline.

```sh
karactl -n NS openbao unseal-keys reveal secrets --confirm secrets -o json
```

Equivalent API request, relative to `/api/v1`:

```http
POST /clusters/secrets/openbao/unseal-keys/reveal?namespace=NS
Content-Type: application/json

{"confirm":"secrets"}
```

This requires **`openbao-unseal-keys:reveal`** on `NS/secrets`, not merely cluster read or `cluster-credentials:read`. Only administrators receive it by default. The response has `Cache-Control: no-store`; successful disclosure is audited as `OpenBaoUnsealKeysRevealed` without shares/token in the audit body. Treat terminal scrollback, shell redirection and screenshots as secret disclosure. UI reveal fetches explicitly without query-cache storage. Recovery shares do not manually unseal an auto-unseal server; retain access to the original transit key or KMS key as well.

## Managed database connections

Connections are opt-in and target a **Ready**, TLS-enabled Karakoram `postgres`, `psmdb` replica-set, or `rabbitmq` cluster in the **same namespace**. Supabase and sharded MongoDB are not accepted targets. Every OpenBao member must reach the target's private endpoints. PostgreSQL storage bindings are unrelated to this feature.

```sh
karactl -n NS openbao database-connections create secrets \
  --name orders --target ordersdb --role reader:readonly \
  --role writer:readwrite --default-ttl 1h --max-ttl 24h
karactl -n NS openbao database-connections list secrets -o json
karactl -n NS openbao database-connections get secrets orders -o json
```

The corresponding POST `/clusters/secrets/openbao/database-connections?namespace=NS` body is:

```json
{
  "name": "orders",
  "targetCluster": "ordersdb",
  "roles": [
    {"name": "reader", "access": "readonly", "defaultTtlSeconds": 3600, "maxTtlSeconds": 86400},
    {"name": "writer", "access": "readwrite", "defaultTtlSeconds": 3600, "maxTtlSeconds": 86400}
  ]
}
```

Connection and role names match `^[a-z][a-z0-9-]{0,30}$`; there are 1–16 roles per connection. TTL defaults are 3600/86400 seconds; maximum must be at least default and at most 31536000 seconds. `database` is forbidden for PostgreSQL; for MongoDB it selects one database (empty means all databases), and for RabbitMQ it selects a vhost (default `/`). All RabbitMQ roles in one connection **must share the same default and maximum TTL pair**, because OpenBao sets leases per RabbitMQ mount.

Creation requires `clusters:update` on OpenBao plus `database-users:create` on the target. Karakoram creates a dedicated target administrative identity and stores its bootstrap credentials sealed. PostgreSQL uses `LOGIN CREATEROLE` and ADMIN OPTION on `pg_read_all_data`/`pg_write_all_data`, without superuser; MongoDB uses `userAdminAnyDatabase` on `admin`; RabbitMQ uses an administrator tag and permissions on requested vhosts. Target CAs trigger a serialized rolling process-trust restart before plugin configuration. A returned record can still be `pending`; wait for `ready` before issuance. Records contain no passwords.

Karakoram configures roles; **OpenBao issues and leases the actual credentials**. Use each returned role's `credentialsPath`, with a suitably authorized native token:

```sh
bao read database/creds/orders_reader
bao read database/creds/orders_writer
```

PostgreSQL/MongoDB use `database/creds/<connection>_<role>`; the underscore separator cannot occur in either name, so hyphenated connection and role names remain unambiguous. RabbitMQ uses `rabbitmq-<connection>/creds/<role>`; each unique connection name has its own mount. Create native policy grants for those paths yourself. PostgreSQL/MongoDB rotate the dedicated root password inside OpenBao; the retained bootstrap password is intentionally stale afterward.

```sh
karactl -n NS openbao database-connections delete secrets orders --yes
```

Deletion requires `clusters:update` on OpenBao and `database-users:delete` on the target. It synchronously revokes outstanding dynamic leases while the plugin still has its connection and role definitions, then removes native connection configuration and finally the target identity. Revocation failure blocks cleanup rather than discarding the credentials needed to revoke users. Target-cluster deletion is blocked while connections reference it. Native PostgreSQL, MongoDB and RabbitMQ development proofs cover actual credential issuance and revocation; RabbitMQ additionally checks TLS AMQP readonly/readwrite and vhost isolation. These bounded native checks do not establish live-backend acceptance. See [verification](../verification.md).
