<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Database users, TLS and access

Karakoram accounts/policies authorize the **control plane**; MongoDB users/roles authorize the **database**. Creating one does not create the other. Use [RBAC/auth](rbac-and-auth.md) for local/OIDC accounts and [security](../security.md) for trust boundaries.

## Application users and custom roles

In a cluster's **Users** tab, create application identities scoped to their authentication database and role references. For example, save this API-shaped file as `app-user.yaml` to grant a workload read/write access only to `appdb`:

```yaml
username: appwriter
database: appdb
roles:
  - role: readWrite
    db: appdb
```

```sh
karactl db-users create demo -f app-user.yaml
karactl db-users list demo
karactl db-users get demo appdb appwriter
```

If creation omits `password`, the server generates one and returns `generatedPassword` **once**. Capture it securely at creation; subsequent reads cannot recover an application password. A supplied password is write-only. Update with `db-users set CLUSTER DATABASE USER -f FILE`; omit the password to retain it. Delete with `db-users delete CLUSTER DATABASE USER`.

Every supplied password, on creation or password change, must contain **at least eight Unicode characters**. Omit the field to generate a password on creation or retain it on update; an explicitly empty value is rejected. MongoDB SCRAM authentication uses **SASLprep (Unicode 3.2)** normalization: ordinary punctuation, accented Latin and supported CJK characters are allowed, but emoji, unassigned/prohibited characters, controls and invalid bidirectional text are rejected before contacting MongoDB. Invalid passwords return HTTP 400 with `details.field: password` and reason `out_of_range` (minimum) or `invalid` (character/profile rule); error messages never include the password.

Custom roles use MongoDB privilege resource/action documents and optional inherited role references:

```yaml
name: appreader
database: appdb
roles: []
privileges:
  - resource:
      db: appdb
      collection: orders
    actions: [find]
```

```sh
karactl db-roles create demo -f app-role.yaml
karactl db-roles list demo
karactl db-roles get demo appdb appreader
```

`db-roles set` updates inherited roles/privileges; `db-roles delete` removes a custom role. Role resources `cluster`/`anyResource` are not interchangeable with database/collection resources. MongoDB built-in roles are referenced, not managed as editable custom roles. Provider-reserved accounts/roles are protected. Application user/role operations require `database-users` permissions in Karakoram; do not grant broad database system roles to applications for convenience.

Deleting a custom role that is still assigned to any user or inherited by another custom role returns **409** with reason `invalid_state`, naming the dependent identities. Remove those role references explicitly using user/role updates, then retry the same DELETE; there is no force-delete parameter. This prevents MongoDB's native `dropRole` behavior from silently stripping user assignments.

## System users and rotation

Each cluster has independently generated passwords for these reserved accounts in `admin`:

| Account | Roles and purpose |
| --- | --- |
| `clusterAdmin` | `clusterAdmin@admin`: topology/cluster administration |
| `userAdmin` | `userAdminAnyDatabase@admin`: users/roles and restored-user reconciliation |
| `clusterMonitor` | `clusterMonitor@admin`, `read@local`: engine/oplog monitoring |
| `backup` | `backup`, `restore`, `clusterMonitor`, `readWrite`, `pbmAnyAction` on admin: PBM |
| `databaseAdmin` | `readWriteAnyDatabase`, `readAnyDatabase`, `dbAdminAnyDatabase`, `backup`, `restore`, `clusterMonitor` on admin: database administration |

PBM's custom `pbmAnyAction` grants `anyAction` on `anyResource`. The backup account is deliberately powerful, **not** read-only or narrowly least-privileged. Distinct credentials limit routine reuse but do not make that role narrow.

```sh
karactl system-users list demo
karactl system-users rotate demo backup
karactl operations cluster demo
```

Rotation is a durable operation: passwords are changed per set and local backup/monitor credentials reloaded member by member. Follow the operation to success. Do not manually reset these users behind the provider; it must retain usable credentials. System-user listings do not reveal passwords. Connection reveal currently returns **databaseAdmin only**, not every system account or any application password.

After the rotation operation succeeds, system-user listings return that account's `lastRotatedAt` in UTC. The timestamp is durable provider state and survives control-plane restarts; a failed or still-running rotation does not advance it.

## Keyfile

The provider generates a cluster keyfile for intra-cluster authentication; the agent renders `DATA_DIR/karakoram/keyfile` as **0400** under the same OS user as mongod/PBM. It is separate from TLS certificates and application authentication. It is not returned by public cluster reads, and there is no public keyfile-rotation CLI command. Preserve encrypted cluster secrets through state recovery; never share the keyfile as an application credential.

## TLS modes and generated CA

The current implementation defaults to **`requireTLS`**, with a per-cluster self-generated CA. Modes are:

- `requireTLS`: all database client/cluster traffic must use TLS. Password-authenticated clients **do not need a client certificate**: the rendered configuration sets `allowConnectionsWithoutCertificates: true`. They still need a trusted server CA and valid MongoDB authentication.
- `preferTLS`: members prefer encrypted communication but clients may connect in plaintext. This is a compatibility transition, **not** mandatory encryption.
- `disabled`: database TLS is off; authentication/keyfile remain. This does not disable the separately secured node/control-plane agent protocol.

Download the public CA through authorized connection details and configure your driver/client to validate it. Do not use insecure certificate-verification flags instead. Generated member certificates cover their private hostnames, localhost IPs and known project public IPs. Internal control-plane PKI and cluster database PKI are distinct.

Change modes with a spec patch:

```yaml
spec:
  tls:
    mode: requireTLS
    ca: selfSigned
```

```sh
karactl clusters patch demo -f tls-mode.yaml --wait
karactl clusters rotate-tls demo --wait
```

Mode changes use cluster-wide rolling passes, waiting for each restarted member to acknowledge its configuration and catch up before disrupting the next member. Enabling TLS follows `disabled → allowTLS → preferTLS → requireTLS` (stopping at the requested mode); disabling it follows the reverse sequence. The internal `allowTLS` bridge accepts TLS and plaintext but sends plaintext to other members, while `preferTLS` sends TLS. Every member must complete a pass before the next begins: directly mixing `disabled` and `preferTLS` can break replication and prevent a safe primary stepdown. `allowTLS` is not a public spec option. See MongoDB's [rolling TLS upgrade procedure](https://www.mongodb.com/docs/v8.0/tutorial/upgrade-cluster-to-ssl/).

`rotate-tls` rotates leaf certificates member by member under the existing cluster CA; it is not an operator master-key rotation or a CA-source migration. CA source **and existing CA certificate/key material** are immutable after creation: PUT/PATCH proposing different CA material returns 409; resubmitting the identical CA pair is accepted. See the [security ADR](../adr/0012-secrets-users-tls.md) for the TLS rationale.

Controller-issued member and agent leaves renew within 30 days of expiry. Renewed bundles are staged privately, then published one member at a time after its disruption safety check; the next member waits until the agent has applied the configuration and database replication has recovered.

## User-provided PKI

For `tls.ca: userProvided`, supply a **CA certificate and signing private key** via write-only `caCertificate`/`caPrivateKey` fields. The CA/key must match and be a certificate-signing CA, using RSA **≥2048 bits** or ECDSA **P-256, P-384 or P-521**. Karakoram needs signing authority to issue member and agent certificates; importing a public CA bundle alone is insufficient. Use a dedicated cluster CA, not a broadly trusted organization's root key.

Optionally supply `certificate` and `privateKey` as a matching PEM server/member pair. The provided leaf/chain must verify under the supplied CA, be currently valid, support **serverAuth and clientAuth**, and cover each current member hostname plus IP SAN `127.0.0.1` for PBM. When exposed publicly, it must also cover the selected project public IP. A single supplied leaf is applied to relevant members, so its SANs must cover all of them. New monotonically indexed members may require replacing the supplied bundle with expanded SAN coverage before scaling/replacement. Private keys are encrypted in state and omitted from ordinary reads.

Supplied member leaves are never automatically renewed or replaced. Within 30 days of expiry, cluster status reports `TLSCertificate=False` with reason `ExpiringSoon` (or `Expired` after expiry); replace the supplied certificate/private-key pair before it expires. Agent leaves and controller-issued member leaves under a user-provided CA still renew automatically.

PEM inputs are supplied in a protected API-shaped create/patch file, using YAML block scalars. Certificate and private-key fields must be supplied as pairs. Do not paste private material into terminal history or shared screenshots. TLS leaf rotation issues certificates under the current CA; do not assume it preserves an externally supplied leaf unchanged.

## Connection details

Cluster status contains private hosts, replicaSet (for replica sets), a **password-free** URI and provisioned public endpoints. In the Overview tab, **Reveal credentials**, or:

```sh
karactl clusters connection demo --reveal
```

The endpoint requires `read` on **`cluster-credentials`**, separate from reading `clusters`, and sends `Cache-Control: no-store`. Treat its databaseAdmin password/credential-bearing URI as a secret. This is an administrative credential, not the application's normal identity. Application clients should use their own user's auth database and password, URL-encode credentials, and set TLS with the downloaded CA. Sharded clients connect to mongos; replica-set clients use discovered members and `replicaSet=rs0`.

Select the cluster's namespace (`karactl -n prod clusters connection orders
--reveal` or `?namespace=prod`). Private hosts belong to **that namespace's
project**, not necessarily the control-plane project. The relay carries
management only and is not an application connection endpoint; workloads need
network access to the database project or deliberate public TCP exposure.
Credential authorization is on `cluster-credentials, read, prod/orders`.

### Connect an application user with TLS

This recipe uses the scoped `appwriter@appdb` user above, not the revealed
administrative account. Keep the cluster at `requireTLS` and wait for **Ready**
with healthy members and matching desired/observed generations before connecting.
You need `karactl` configured for your installation, `jq`, `mongosh`, and Python 3
with PyMongo installed (`python3 -m pip install pymongo` in your application's
environment).

**Private hostnames resolve only inside the selected namespace's Zerops
project.** Run the client commands from an application service in that project.
The simplest way to follow the complete recipe is to run it there with an
authenticated `karactl`. You can instead create the user/export connection
material on your workstation, then securely transfer `ca.pem`, the password-free
host list and the application password to the application service. A successful
public control-plane login does not make private database DNS reachable from
your workstation. For outside-project access, first read
[public exposure and discovery](#public-exposure-and-discovery).

1. Create the application user from `app-user.yaml`, capturing the generated
   password rather than copying an administrative credential:

   ```sh
   # Select the namespace containing the database; use it on every command.
   NAMESPACE=default
   CLUSTER=demo
   APP_PASSWORD=$(karactl -n "$NAMESPACE" db-users create "$CLUSTER" \
     -f app-user.yaml -o json | jq -er .generatedPassword)
   ```

   Store this one-time password in a secret manager. If the user already exists,
   load its saved password instead of creating it again; GET cannot recover it.
   Keep shell tracing disabled and do not commit secrets or credential-bearing
   URIs.

2. Export the public cluster CA as a real multiline PEM file:

   ```sh
   karactl -n "$NAMESPACE" clusters ca "$CLUSTER" > ca.pem
   ```

   The JSON alternative uses the top-level `caCertificate` field:

   ```sh
   karactl -n "$NAMESPACE" clusters connection "$CLUSTER" --reveal -o json \
     | jq -r .caCertificate > ca.pem
   ```

   Or use the API directly, with the installation origin and a Karakoram bearer
   token loaded securely:

   ```sh
   curl --fail-with-body --silent --show-error \
     -H "Authorization: Bearer $KARAKORAM_TOKEN" \
     "$KARAKORAM_SERVER/api/v1/clusters/$CLUSTER/connection?namespace=$NAMESPACE" \
     | jq -r .caCertificate > ca.pem
   ```

   Both paths require `cluster-credentials:read`. Connection reveal also returns
   administrative secrets: do not retain its full JSON in logs or application
   configuration. Do not copy a human-readable table cell into a PEM file.

3. Read only the password-free connection summary, URL-encode the application
   password, and build an application URI:

   ```sh
   SUMMARY=$(karactl -n "$NAMESPACE" clusters connection "$CLUSTER" --reveal -o json \
     | jq -c .connection)
   MONGO_HOSTS=$(printf '%s' "$SUMMARY" | jq -er '.hosts | join(",")')
   REPLICA_SET=$(printf '%s' "$SUMMARY" | jq -r '.replicaSet // empty')
   ENCODED_PASSWORD=$(printf '%s' "$APP_PASSWORD" | jq -sRr @uri)
   URI_OPTIONS='authSource=appdb&tls=true'
   if [ -n "$REPLICA_SET" ]; then
     URI_OPTIONS="$URI_OPTIONS&replicaSet=$REPLICA_SET"
   fi
   export MONGODB_URI="mongodb://appwriter:$ENCODED_PASSWORD@$MONGO_HOSTS/appdb?$URI_OPTIONS"
   ```

   `.connection.hosts` contains private replica-set members or, for sharding,
   mongos routers. The replica-set summary supplies `replicaSet` (`rs0`);
   sharded summaries omit it, so the same recipe does not add that option for
   mongos. `authSource=appdb` matches where this user was created, not `admin`.
   Treat `MONGODB_URI` as a secret.

4. From the application service in the database project, verify a TLS-authenticated
   write with mongosh:

   ```sh
   mongosh "$MONGODB_URI" --tls --tlsCAFile ca.pem \
     --eval 'db.orders.insertOne({example: "tls connection"}); db.orders.findOne()'
   ```

5. Connect from PyMongo using the same CA and application identity:

   ```python
   import os
   from pymongo import MongoClient

   with MongoClient(os.environ["MONGODB_URI"], tls=True, tlsCAFile="ca.pem") as client:
       client.admin.command("ping")
       print(client.appdb.orders.find_one())
   ```

   Keep CA/hostname verification enabled. DNS failures mean the client is not
   resolving the project's private hosts; authentication failures require
   checking the saved application password, `authSource` and role grants. A TLS
   trust error requires the CA for this cluster, not an insecure verification
   flag. Replica-set public seed connectivity alone does not establish working
   external discovery/failover; see the caveat below.

## Public exposure and discovery

Private-only is the default. The agent explicitly renders both `net.bindIpAll: true` and `net.ipv6: true` for dual-stack MongoDB listeners; private IPv4/IPv6 and supported public IPv6 routing therefore use the same managed listener. `bindIpAll` alone does not enable MongoDB IPv6. Both settings are provider/agent-owned and cannot be overridden through engineConfig. Public MongoDB TCP requires project IPv6 or a **dedicated** public IPv4, not shared IPv4 or the HTTP subdomain. You must supply a nonempty CIDR allowlist:

```yaml
spec:
  exposure:
    public: true
    ipType: IPV6
    allowlist:
      - "2001:db8:1234::/64"
```

The documentation prefix above is an example, not a usable client network. Replace it with the smallest real authorized client CIDR. Apply via `clusters patch -f FILE --wait`. Public member ports are distinct because members share the project public IP. Sharded exposure publishes mongos only; config/shard members remain private. Disabling exposure removes owned public routes. Keep TLS required and verify actual allowed and denied client reachability.

Every allowlist entry must be a valid CIDR of the configured `ipType`: IPv6 for `IPV6`, IPv4 for `IPV4`. A family mismatch is rejected before public routing is changed. Entries covering every address (`::/0`, `0.0.0.0/0`, including noncanonical equivalents) are rejected unless you explicitly send **`spec.exposure.allowAllAddresses: true`** (default false). This acknowledgement does not bypass family validation, TLS or authentication. Prefer a restricted range; the web UI asks for confirmation when an all-address range is entered. An error names the offending entry, such as `spec.exposure.allowlist.0`.


**Use layered access controls.** An IP allowlist is defense in depth, not a substitute for authentication. Prefer private access; when public access is needed, retain `requireTLS` and strong credentials and verify allowed and denied client access for your deployment. See [networking limitations](../limitations.md#access-and-networking).

**Replica-set split-horizon caveat:** Karakoram does not configure a complete external DNS/SNI split-horizon scheme. A reachable public seed still advertises private member hostnames in normal MongoDB discovery; external clients must be able to resolve/reach the discovered addresses with their correct per-member ports and certificate validation. Merely mapping names to the shared IP does not fix distinct public port mappings. `directConnection=true` can target one publicly routed member for diagnostics, but it bypasses normal replica-set discovery/failover and is not an external HA connection solution.

Treat `publicHosts` as routing information, not proof of externally usable replica-set failover. A complete external discovery solution requires coordinated advertised horizons/DNS/ports and TLS/SNI-aware clients; this is a known [limitation](../limitations.md). Public routers for sharded clusters do not have this replica-set client discovery requirement. Never expose agent port 9443 or broad allowlist ranges merely to work around a connection error.

In public endpoint summaries, show the discovery caveat alongside the seed address: **Public seeds advertise private member hostnames. External clients must resolve and reach the discovered members; `directConnection=true` is for diagnostics only, not HA/failover.** This caveat applies to MongoDB replica sets, not sharded-cluster mongos endpoints.

## Reading placement

Host fingerprints are best-effort observations, not guaranteed anti-affinity. Co-location warnings only group members with the same **known** fingerprint. Members grouped as `unknown` have not reported a usable fingerprint: **placement unknown until members report host fingerprints**, not evidence that they share a physical host.
