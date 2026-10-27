<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Neo4j access and TLS

[Overview](README.md) · [Backups](backups.md) · [Operations](operations.md)

## Listeners and trust

| Listener | Port | Scope |
| --- | --- | --- |
| Bolt | 7687 | TLS required; private by default, opt-in public TCP |
| HTTPS Query API / browser | 7473 | Private, cluster-CA TLS |
| Member control | 9443 | Private authenticated management |

Plain HTTP is disabled. Public exposure publishes **Bolt only**, not the Query API or browser. Configure address-family-appropriate CIDR allowlists and check backend enforcement in [public exposure](../security.md#public-exposure-allowlists); backends without CIDR enforcement need the documented explicit all-address acknowledgement and an external firewall. Avoid exposing an administrative graph database directly to the Internet.

Connection summaries are password-free. Download the CA and explicitly reveal the managed credential only when needed:

```sh
karactl -n NS clusters connection graphdb
karactl -n NS clusters ca graphdb > graphdb-ca.pem
karactl -n NS clusters connection graphdb --reveal
```

CA export and reveal require `cluster-credentials:read`. Reveal responses have `Cache-Control: no-store`; do not log or share their output. Use a separate native user for application credential lifecycle, while recognizing that Community still grants it administrative rights.

The advertised URI is `neo4j+s://HOST:7687`. A private cluster CA must be installed in the **client's** trust store; `+s` does not make a private CA publicly trusted. Verify the endpoint hostname/IP against the certificate. `neo4j` schemes enable routing discovery, even for a singleton: returned private addresses must be reachable. For a public endpoint or other direct singleton connection, use `bolt+s://HOST:7687` to avoid routing to an unreachable private address. `bolt+s` still requires certificate validation; it is not a trust workaround.

For Cypher Shell, first add the cluster CA to the Java trust store used by that shell, then connect without putting a password in command-line arguments:

```sh
cypher-shell -a 'bolt+s://HOST:7687' -u appuser -d neo4j
```

The shell prompts for the password. See the [official Cypher Shell reference](https://neo4j.com/docs/operations-manual/current/cypher-shell/) for Java/client setup. For drivers, use their documented custom-CA/trust-store configuration; APIs vary by language. Some drivers prohibit combining `+s` with explicit encryption/trusted-certificate options and instead require a `bolt://` or `neo4j://` URI plus explicitly enabled TLS and custom CA verification. Do not switch to an unencrypted URI without those TLS settings. See [official driver connection semantics](https://neo4j.com/docs/python-manual/current/connect-advanced/) and the [Python driver configuration reference](https://neo4j.com/docs/api/python-driver/current/api.html#driver-configuration).

`neo4j+ssc` / `bolt+ssc` encrypt without verifying the server certificate. They are not substitutes for trusting the cluster CA and should not be used for production connections.

The private Query API is `POST https://HOST:7473/db/neo4j/query/v2` with Basic authentication and the cluster CA trusted by the HTTPS client. The server does not require a client certificate. Do not publish HTTPS via an unmanaged proxy and assume Karakoram's Bolt allowlist protects it.

## Managed administrator

`neo4j` is the managed administrator, hidden from native-user lists and rejected by native-user mutation routes. Its password is sealed in the control-plane secret store. Rotate it through the managed system-user operation, not direct Cypher or the application-user API:

```sh
karactl -n NS system-users list graphdb
karactl -n NS system-users rotate graphdb neo4j --wait
```

Rotation changes the native password before committing the new managed secret; retries accept a password already applied. Do not independently change the managed password: rejected authentication produces the `CredentialMismatch` condition described in [operations](operations.md#troubleshooting).

## Native users

**Every Community user can read, write and administer the whole server.** There are no limited roles, per-database grants or read-only users. Network policy and credential handling remain essential.

The cluster's native **Users** tab lists users and password-change state, creates users, sets/generates passwords and confirms deletion. Leave the password field empty to generate one; save the one-time reveal before dismissing it. The managed administrator belongs in **System users**, not this tab. The connection panel's **How to connect (Bolt / [Neo4j](https://neo4j.com/) drivers)** section explains CA trust and private/public listener scope; download the CA through credential reveal.

```sh
karactl -n NS neo4j users list graphdb
karactl -n NS neo4j users get graphdb appuser
karactl -n NS neo4j users create graphdb appuser
karactl -n NS neo4j users password graphdb appuser --password-stdin
karactl -n NS neo4j users delete graphdb appuser
```

Omitting a password on create or password change generates one, returned in that response. `--password-stdin` supplies a password without a command-line secret. Store generated passwords securely; ordinary reads return only `name` and `passwordChangeRequired`.

The API paths below are relative to `/api/v1/clusters/{cluster}`:

| Method/path | Body | Result |
| --- | --- | --- |
| `GET /neo4j/users` | — | `{items: [{name, passwordChangeRequired}]}` |
| `POST /neo4j/users` | `{name, password?}` | 201 `{user, generatedPassword?}` |
| `GET /neo4j/users/{user}` | — | `{name, passwordChangeRequired}` |
| `DELETE /neo4j/users/{user}` | — | 204 |
| `POST /neo4j/users/{user}/password` | Optional `{password?}` | `{generatedPassword?}` |

Native routes use `database-users` permissions; every mutation additionally requires `clusters:update`. Cookie mutations require CSRF protection; mutations support `Idempotency-Key`. Password responses use `Cache-Control: no-store`. The generic [MongoDB](https://www.mongodb.com/)-shaped `/database-users` and `/database-roles` routes are not Neo4j access APIs.

Management MCP exposes `neo4j_users_list` and `neo4j_user_get` as reads, and `neo4j_user_create`, `neo4j_user_delete` and `neo4j_user_password_set` as destructive operations requiring confirmation. Create and password changes are secret-bearing operations: restricted API tokens must permit credential reveal as well as the required permissions, even when supplying a password explicitly. Neo4j does not support the cluster Data MCP gateway.

Creating an existing user returns 409, changing or deleting a missing user returns 404, and invalid names or passwords return 400. The managed `neo4j` administrator is hidden and cannot be changed through these routes.

Names start with an ASCII letter and contain at most 64 letters, digits, underscores, dots or hyphens. Case-insensitive managed/reserved names include `neo4j`, `neo4j_*`, and prefixes `system`, `kk_`, `karakoram`.

## Managed hardening

The server disables usage reports, fleet discovery broadcasts, Fleet Manager, X-Forwarded processing and file-URL `LOAD CSV`; strict configuration validation is enabled. These are managed policy, not user-overridable settings. The exact [settings allowlist](README.md#memory-and-options) permits query behavior/logging only. TLS rotation reissues the leaf certificate and restarts the singleton: plan an outage and reconnect clients afterward.
