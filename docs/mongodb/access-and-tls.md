<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Users, roles and TLS

Manage MongoDB identities and database transport; [Karakoram accounts](../access-control.md) authorize the control plane separately.

## Application users and custom roles

Use **Users → Add user** or an API-shaped `app-user.yaml`:

```yaml
username: appwriter
database: appdb
roles:
  - role: readWrite
    db: appdb
```

```sh
karactl -n default db-users create demo -f app-user.yaml
karactl -n default db-users list demo
karactl -n default db-users get demo appdb appwriter
```

Omit `password` to receive `generatedPassword` **once**; save it before closing the UI success dialog. Subsequent reads cannot recover it. `db-users set CLUSTER DATABASE USER -f FILE` changes roles/password; omission retains the password. `db-users delete CLUSTER DATABASE USER` removes the identity. UI empty-password fields mean generation on create, retention on edit; supplied passwords are write-only.

Supplied passwords need ≥8 Unicode characters and MongoDB SCRAM's SASLprep (Unicode 3.2) profile. Punctuation, accented Latin and supported CJK are accepted; emoji, prohibited/unassigned characters, controls and invalid bidirectional text are rejected. Explicit empty passwords return 400 with `details.field: password`, reason `out_of_range` for length or `invalid` for profile violations, without echoing secrets.

Custom roles accept inherited role references and MongoDB resource/action documents, for example `app-role.yaml`:

```yaml
name: appreader
database: appdb
roles: []
privileges:
  - resource: {db: appdb, collection: orders}
    actions: [find]
```

Use `db-roles create demo -f app-role.yaml`, `list demo`, `get demo appdb appreader`, `set demo appdb appreader -f FILE` or `delete demo appdb appreader`. Built-in roles are referenced, not editable custom roles; reserved identities are protected. `cluster`/`anyResource` resources are not database/collection substitutes. These operations require Karakoram `database-users` permissions; grant workloads only required database privileges.

Deleting an assigned/inherited role returns **409**, reason `invalid_state`, naming dependents. Remove references with user/role updates, then retry DELETE; no force-delete exists. The UI lists known dependents but does not revoke them, and the server rechecks stale UI data.

## System users and rotation

Reserved `admin` accounts have independent generated passwords:

| Account | Roles on `admin` unless noted |
| --- | --- |
| `clusterAdmin` | `clusterAdmin`: topology administration |
| `userAdmin` | `userAdminAnyDatabase`: users/roles and restored-user reconciliation |
| `clusterMonitor` | `clusterMonitor`, `read@local`: monitoring |
| `backup` | `backup`, `restore`, `clusterMonitor`, `readWrite`, custom `pbmAnyAction` |
| `databaseAdmin` | `readWriteAnyDatabase`, `readAnyDatabase`, `dbAdminAnyDatabase`, `backup`, `restore`, `clusterMonitor` |

**Warning:** `pbmAnyAction` grants `anyAction` on `anyResource`; PBM is not read-only. Database administration and user administration also carry broad/escalation authority. Never use these system accounts in applications.

```sh
karactl -n default system-users list demo
karactl -n default system-users rotate demo backup --yes
karactl -n default operations cluster demo
```

Rotation changes credentials per set and reloads local backup/monitor credentials member by member. Follow the durable operation; do not manually reset managed passwords. Listings reveal no passwords. Successful rotation advances durable UTC `lastRotatedAt` (**Last rotated**); absence means no recorded success, and failed/running rotation does not advance it.

The separate generated intra-cluster keyfile is owner-only **0400**, not an application credential or TLS key. Public reads do not return it; no public keyfile-rotation command exists. Preserve encrypted cluster secrets during [state recovery](../operations.md#control-plane-state-backup-and-recovery).

## TLS modes and generated CA

| Mode | Database traffic |
| --- | --- |
| `requireTLS` (default) | TLS required; password clients need the trusted CA, not client certificates (`allowConnectionsWithoutCertificates: true`) |
| `preferTLS` | Members send TLS; plaintext clients remain allowed |
| `disabled` | No database TLS; authentication/keyfile and separately protected agent transport remain |

The generated cluster CA uses ECDSA P-256, distinct from installation internal PKI. Member leaves cover private hostnames, localhost IPs and known project public IPs. Trust this CA; never disable hostname/certificate verification.

Patch `spec.tls: {mode: requireTLS, ca: selfSigned}` with `clusters patch demo -f FILE --wait`. Changes use rolling passes, waiting for configuration acknowledgement and replication catch-up between members/passes. Enabling traverses `disabled → allowTLS → preferTLS → requireTLS`, stopping at the requested mode; disabling reverses it. Internal `allowTLS` accepts both but sends plaintext; it is not a public option. Directly mixing disabled/preferTLS can break replication/stepdown.

`karactl clusters rotate-tls demo --wait --yes` rotates leaves under the existing CA, not the master key or CA source. CA source **and stored signing certificate/key are immutable**: changed material returns **409**; identical resubmission is accepted. Controller-issued member/agent leaves renew within 30 days of expiry, staged one member at a time behind disruption/replication checks.

## User-provided PKI

For `tls.ca: userProvided`, provide paired write-only `caCertificate`/`caPrivateKey` PEM fields: matching signing CA/key, RSA ≥2048 or ECDSA P-256/P-384/P-521, with CA/signing constraints. A public CA bundle alone is insufficient. Use a dedicated cluster CA, not an organizational root.

Optional paired `certificate`/`privateKey` must match, be currently valid, verify under that CA for **serverAuth and clientAuth**, and cover every current member hostname plus **IP SAN `127.0.0.1` for PBM** and the selected public IP when exposed. One supplied leaf serves relevant members; new monotonic hostnames may need expanded SANs before replacement/scaling. Private keys are encrypted and omitted from ordinary reads.

Supplied member leaves do not auto-renew: within 30 days status reports `TLSCertificate=False`, reason `ExpiringSoon`, then `Expired`. Replace the pair before expiry. Agent/controller-issued leaves still renew under a supplied CA. Explicit leaf rotation issues under the current CA; do not assume it preserves a supplied leaf unchanged.

Use protected create/patch files and YAML block scalars, not terminal history/screenshots. Existing userProvided clusters may omit CA inputs to retain stored material. The editor's ability to enter another CA source does not override immutable-source validation.

## Connection details and application recipe

Ordinary status provides private hosts, replica-set name, password-free URI and provisioned public endpoints. **Reveal credentials** / `clusters connection demo --reveal` requires `cluster-credentials/read` on `<namespace>/<cluster>`, returns `Cache-Control: no-store`, and reveals **databaseAdmin only**, never application passwords. Do not use that URI in an app or retain full reveal JSON in logs.

Provision from a trusted **operator workstation**, with [authenticated karactl](../cli.md) and `jq`; the application needs no Karakoram token or CLI. Wait for [healthy observed generation](replica-sets.md).

1. Create the scoped user above and capture its one-time password with tracing disabled:

   ```sh
   set +x
   NAMESPACE=default
   CLUSTER=demo
   APP_PASSWORD=$(karactl -n "$NAMESPACE" db-users create "$CLUSTER" \
     -f app-user.yaml -o json | jq -er .generatedPassword)
   karactl -n "$NAMESPACE" clusters ca "$CLUSTER" > ca.pem
   ```

   CA export requires credential-read permission and uses the reveal endpoint; do not retain its other fields. For an existing user, load the saved password instead of recreating it.

2. Read hosts/replica-set from **non-reveal status**, then URI-encode the application password:

   ```sh
   SUMMARY=$(karactl -n "$NAMESPACE" clusters status "$CLUSTER" -o json \
     | jq -c .connection)
   HOSTS=$(printf '%s' "$SUMMARY" | jq -er '.hosts | join(",")')
   RS=$(printf '%s' "$SUMMARY" | jq -r '.replicaSet // empty')
   PASSWORD=$(printf '%s' "$APP_PASSWORD" | jq -sRr @uri)
   OPTIONS='authSource=appdb&tls=true'
   if [ -n "$RS" ]; then OPTIONS="$OPTIONS&replicaSet=$RS"; fi
   MONGODB_URI="mongodb://appwriter:$PASSWORD@$HOSTS/appdb?$OPTIONS"
   ```

   Replica sets supply `rs0`; mongos summaries omit it. Encode usernames too if changed to characters requiring escaping.

3. Persist `MONGODB_URI` and, if stored separately, the application password as **Zerops application-service secret environment variables**, not source/pipeline variables. Ship the public multiline `ca.pem` in the app's deployment/configuration to a readable fixed path, such as `/var/www/ca.pem`. Restart/redeploy the application to load updated secrets. A shell `export` affects only a test shell; it is not persistent service configuration.
4. Run clients **inside the database namespace's project network**, or use a deliberately validated public access plan:

   ```sh
   mongosh "$MONGODB_URI" --tls --tlsCAFile /var/www/ca.pem \
     --eval 'db.orders.insertOne({example: "tls"}); db.orders.findOne()'
   ```

   ```python
   import os
   from pymongo import MongoClient
   with MongoClient(os.environ["MONGODB_URI"], tlsCAFile="/var/www/ca.pem") as client:
       client.admin.command("ping")
       print(client.appdb.orders.find_one())
   ```

**UI alternative:** **Users → Add user**, save the generated password, then use ordinary **Overview** hosts/replica-set. Reveal only to **Download ca.pem** / **Copy CA**; if clipboard fails, download or select the visible PEM preserving line breaks. **How to connect (MongoDB)** snippets start from an administrative URI: replace identity, password and `authSource` with the scoped user's values, then perform the same secret/CA handoff above.

## Public exposure and discovery

Private-only is default. Public MongoDB TCP needs project IPv6 (default `IPV6`) or dedicated IPv4, not shared IPv4/HTTP subdomains; clients need the corresponding connectivity. Patch exposure, for example:

```yaml
spec:
  exposure:
    public: true
    ipType: IPV6
    allowlist: ["2001:db8:1234::/64"]
```

Replace this documentation CIDR with authorized client ranges; follow the shared [public exposure allowlist and acknowledgement rules](../security.md#public-exposure-allowlists). The UI clears confirmation when settings change.

Managed listeners explicitly enable `bindIpAll` and IPv6; neither is overridable. Members share the project public IP with distinct mapped ports. Disabling exposure removes owned routes. Routing synchronization is project-wide; Ready/`isSynced` is not proof of external reachability. Keep `requireTLS`, narrow CIDRs and verify allowed/denied access; never publish agent port 9443.

**Warning:** Public replica-set seeds still advertise private member names/ports. No automatic external DNS/SNI split-horizon solution is configured; DNS-to-shared-IP alone does not fix per-member port mappings. External clients must resolve/reach discovered endpoints with valid TLS. `directConnection=true` is diagnostic, not HA/failover. Treat `publicHosts` as routes, not validated external discovery. [Sharded](sharding.md) mongos endpoints do not require external discovery of shard members.

## Reading placement

`clusters placement` fingerprints are best-effort. Co-location warnings group matching **known** fingerprints; `unknown` means placement is unobserved, not co-location. Distinct fingerprints do not certify host/zone separation; see [platform limitations](../limitations.md).
