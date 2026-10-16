<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Access and TLS

Configure broker trust, application identities and native management access separately from [Karakoram accounts/RBAC](../access-control.md).

## TLS modes and supplied PKI

| Mode | AMQP / optional Stream | Private management | Erlang distribution |
| --- | --- | --- | --- |
| `disabled` | Plaintext | HTTP | IPv6 TCP |
| `preferTLS` | Plaintext and TLS | HTTP and HTTPS | IPv6 TCP |
| `requireTLS` (default) | TLS only | HTTPS only | Peer-verified IPv6 TLS |

See [listener ports](README.md). AMQP TLS supports password authentication without client certificates; verify the CA and hostname. `disabled ↔ preferTLS` can roll in place; crossing the `requireTLS` boundary requires a new cluster, [definitions restore](backups.md) and separate message migration. CA source (`selfSigned`/`userProvided`) is immutable.

For `userProvided`, supply `spec.tls.caCertificate` and signing `caPrivateKey` as PEM. The matching CA key must be RSA ≥2048 bits or ECDSA P-256/P-384/P-521; a certificate without its signing key is insufficient. Karakoram issues leaves if no supplied leaf pair is present. Optional `certificate`/`privateKey` must match; include intermediates in the certificate chain. The leaf must be currently valid, chain to the supplied CA, permit both server and client authentication, and cover **every broker service hostname**; with public exposure it must also cover the **selected project public IP** in its SANs. RabbitMQ does not require a supplied leaf's SANs to include `127.0.0.1`. Replace expiring supplied leaves/keys; `TLSCertificate` reports expiry. Leaf rotation does not rotate the CA, Erlang cookie or control-plane master key.

**OTP advisory:** pinned OTP 27.3.4.17 has TLS-client verification bypass [CVE-2026-89422 / GHSA-rgxr-4g4w-j875](https://github.com/erlang/otp/security/advisories/GHSA-rgxr-4g4w-j875). Generated distribution/CLI TLS clients and Osiris replication client/server force TLS 1.2, retaining peer verification; public AMQPS, Stream and HTTPS server versions are unchanged. A packaged catalog fix ≥27.3.4.18 is required before lifting this restriction. User-supplied Shovel/Federation runtime parameters need independently safe outbound TLS settings.

## Public access

`spec.exposure.public` requires project IPv6 or dedicated IPv4 and the shared [public exposure allowlist and acknowledgement rules](../security.md#public-exposure-allowlists). Each broker receives a distinct public TCP port targeting AMQPS, or AMQP when TLS is disabled. Use returned hosts/ports, not assumed external ports or private service URIs. Test allowed **and denied** source access and certificate coverage externally.

`engineOptions.managementUI.public: true` separately exposes the native **UI and HTTP API** on public HTTPS subdomains. It accepts connections without the AMQP CIDR restriction and authenticates RabbitMQ users, not Karakoram sessions/RBAC. Zerops terminates HTTPS and forwards plaintext HTTP privately to 15672. Only `preferTLS`/`disabled` permit this; `requireTLS` rejects it because certificate-verified HTTPS backend routing is unavailable. Keep management private unless that network surface and private plaintext hop are acceptable. Native administrators/project-platform access are trusted infrastructure authority; never publish the internal-only ports in the [ports table](README.md).

## Access API, CLI and UI

**Cluster → Access** manages six collections. They apply directly through the Management API, not durable declarative topology resources. MongoDB database-user/role endpoints do not apply; other providers return HTTP 409.

All collections live under `/api/v1/clusters/{cluster}/rabbitmq/`, with `?namespace=NS`:

| Collection / CLI group | `get`, `update`, `delete` arguments after `<cluster>` | Write payload |
| --- | --- | --- |
| `vhosts` | `<name>` | `name`, `tags`; optional `defaultQueueType` (`classic`, `quorum`, `stream`), `description`, `limits: {maxConnections, maxQueues}`; `-1` = unlimited |
| `users` | `<name>` | `name`, `tags`; optional `password` **or** `passwordHash` |
| `permissions` | `<vhost> <user>` | `vhost`, `user`, `configure`, `write`, `read` |
| `topic-permissions` | `<vhost> <user>` | `vhost`, `user`, `exchange`, `write`, `read` |
| `policies`, `operator-policies` | `<vhost> <name>` | `vhost`, `name`, `pattern`, `applyTo`, nonnegative `priority`, object `definition` |

Policy `applyTo`: `all`, `queues`, `exchanges`, `classic_queues`, `quorum_queues`, `streams`. Each group has `list/get/create/update/delete`; `list/create` take only `<cluster>`. Create/update require `-f FILE` or `-f -` (API-shaped YAML/JSON); update body identities must match path arguments. Lists have no `--limit`/`--cursor`; output supports `-o table|json|yaml` and namespace selection `-n`. Collections/tags return arrays even when empty. Unknown request fields return `invalid_request`; `definition` remains open.

```sh
karactl -n NS rabbitmq vhosts get brokers /
karactl -n NS rabbitmq permissions update brokers / application -f permission.yaml
karactl -n NS rabbitmq policies get brokers / retention
karactl -n NS rabbitmq operator-policies delete brokers / safety --yes
```

Raw HTTP encodes names **once**: `/` becomes `%2F`, e.g. `/api/v1/clusters/brokers/rabbitmq/vhosts/%2F?namespace=NS`, never `%252F`. CLI/UI/typed clients take raw names. Reads/create/update/delete require corresponding `database-users` actions on `<namespace>/<cluster>`.

Patterns use RabbitMQ PCRE (including lookarounds/backreferences), at most 4096 bytes, forwarded unchanged. Empty permission/topic patterns deny access; policy patterns must be nonempty. Invalid regexes/definition keys return fixed field errors without native diagnostics/pattern echo; missing grant identities name `user,vhost`, reason `not_found`. Correct the payload or create identities before retrying.

**Warning:** vhost deletion deletes its queues/messages; topic-permission deletion removes **all exchanges' grants for the vhost/user pair**. Review policy effects before applying. Delete prompts require the final argument, not cluster name; noninteractive callers need `--yes` or piped `y`/`yes`; empty input sends nothing.

POST/PUT support [Idempotency-Key retries](../api.md#safe-retries-with-idempotency-key), not DELETE. Access UI retains a body-bound key for unchanged retries; edits start a new key. Matching retries replay status/body, including generated passwords, for 24 hours; changed bodies return 409. Default/omitted namespace share scope; other namespaces/object paths are independent. CLI has no idempotency-key flag.

Vhost audit targets include `<cluster>/<vhost>` under `rabbitmq-vhosts`:

```sh
karactl -n NS events list --cluster brokers --resource rabbitmq-vhosts
```

## Least-privilege application access

Tags grant management authority, **not messaging permissions**. Valid tags are `management`, `policymaker`, `monitoring`, `administrator`, `impersonator`; applications should use `tags: []` and scoped grants.

Save three files:

```yaml
# vhost.yaml
name: orders
tags: []
defaultQueueType: quorum
description: Orders application
limits:
  maxConnections: 20
  maxQueues: 1
```

```yaml
# user.yaml
name: orders-app
tags: []
```

```yaml
# permission.yaml
vhost: orders
user: orders-app
configure: '^orders$'
write: '^(orders|amq\.default)$'
read: '^orders$'
```

From a trusted operator environment with [authenticated karactl](../cli.md) and `jq`:

```sh
umask 077
karactl -n NS rabbitmq vhosts create brokers -f vhost.yaml
karactl -n NS rabbitmq users create brokers -f user.yaml -o json > created-user.json
karactl -n NS rabbitmq permissions create brokers -f permission.yaml
jq -er '.generatedPassword' created-user.json
karactl -n NS rabbitmq permissions get brokers orders orders-app
karactl -n NS clusters ca brokers > ca.pem
karactl -n NS clusters connection brokers -o json
```

Omitting both password fields generates a password returned once (UI also displays it once); ordinary reads expose neither passwords nor hashes, and updates omitting both preserve it. Store the password privately in the application service's runtime secrets, then remove `created-user.json`. Hand off only app credentials, vhost, broker endpoints and CA—not a Karakoram token. Mount/copy `ca.pem` where the deployed app can read it. Shell exports alone are not persistent deployment configuration.

The anchored grants allow only queue `orders` and publishing through default exchange `amq.default`; `write: '^orders$'` alone fails default-exchange publishing. No arbitrary named exchange, other queue or other vhost is granted.

## Connect an application over TLS

Password-free connection status gives per-broker `amqp`/`amqps`/`management` endpoints, not a shared load balancer/cluster URI; `preferTLS` can list both schemes. Revealing through Connection details or `clusters connection brokers --reveal -o json` needs `cluster-credentials:read` and returns **admin**, not an app identity: entries contain `username`, `password`, credential-bearing `uri`, `database` (vhost, normally `/`), plus response `caCertificate`. Do not distribute these admin credentials to workloads.

Run the following with Python 3/Pika inside the namespace's project for private DNS. Set `AMQP_HOST` to one returned TLS broker hostname, `AMQP_PORT` to its returned port, and `AMQP_PASSWORD` from the app secret; install Pika in a virtual environment. No client certificate/key is needed.

```python
import os
import ssl
import pika

host = os.environ['AMQP_HOST']
context = ssl.create_default_context(cafile='ca.pem')
parameters = pika.ConnectionParameters(
    host=host, port=int(os.environ['AMQP_PORT']), virtual_host='orders',
    credentials=pika.PlainCredentials('orders-app', os.environ['AMQP_PASSWORD']),
    ssl_options=pika.SSLOptions(context, host), heartbeat=30,
    blocked_connection_timeout=30, socket_timeout=10,
)
connection = pika.BlockingConnection(parameters)
try:
    channel = connection.channel()
    channel.queue_declare(queue='orders', durable=True,
                          arguments={'x-queue-type': 'quorum'})
    channel.confirm_delivery()
    channel.basic_publish(exchange='', routing_key='orders', body=b'hello over AMQPS',
                          properties=pika.BasicProperties(delivery_mode=2), mandatory=True)
    method, properties, body = channel.basic_get(queue='orders', auto_ack=False)
    if method is None:
        raise RuntimeError('No message received')
    channel.basic_ack(method.delivery_tag)
    print(body.decode())
finally:
    connection.close()
```

Use an otherwise unused queue/vhost for this test; it leaves an empty queue. Keep CA/hostname verification enabled. Production clients need multiple broker endpoints and bounded reconnect backoff; Pika blocking connections do not automatically recover established connections. Recreate channels/declarations/consumers/confirms; deduplicate retries of unconfirmed publishes and acknowledge only after processing.

## Delegating user management and system authority

`database-users/create` and `/update` on the cluster allow empty or `management` tags only. Any `administrator`, `policymaker`, `monitoring` or `impersonator` tag additionally requires `clusters/update` on that same object—even when retaining an existing tag. Explicit deny wins; neither another cluster's grant nor `clusters/update` alone suffices.

```csv
p, role:user-manager, database-users, create, default/brokers, allow
p, role:user-manager, database-users, update, default/brokers, allow
```

Add `p, role:user-manager, clusters, update, default/brokers, allow` only for trusted cluster administrators; it also permits ordinary cluster updates. Native management/definitions import are separate administrative surfaces outside this delegation boundary.

Reserved users **karakoram/admin/monitor/guest** and their vhost/topic grants cannot be created/changed/deleted through access routes regardless of RBAC. Bootstrap creates `karakoram` (control-plane administrator) and `admin` (customer administrator), both administrator-tagged with full `/` grants, and removes `guest`. `monitor` has a stored credential but is not bootstrap-created; loopback monitoring does not use it. Only `karakoram`/`admin` rotate via `karactl -n NS system-users rotate brokers admin` (or `karakoram`).

The random shared Erlang cookie is sealed in SecretStore and rendered 0400; it grants node/CLI authority, not AMQP login. Never export it or pass it in command arguments. Node-control identity/tokens, broker credentials and Karakoram sessions are separate trust paths.
