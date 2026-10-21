<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Access and TLS

[Overview](README.md) · [Configuration](configuration.md) · [Backups](backups.md)

Both `mysql` and `pxc` use **MySQL access** in the UI, `karactl mysql` in the CLI and `/api/v1/clusters/{cluster}/mysql/...` in the REST API. These are native MySQL accounts, databases and database-level grants—not MongoDB-shaped `db-users` or arbitrary SQL execution.

## Application identities

Create a database, create a dedicated application user, then grant only the required privileges on that database. Accounts are `'name'@'%'` and require TLS. Names and privileges are validated; reserved schemas and system identities are hidden and cannot be modified here. Global/administrative grants are not exposed. Database character set and collation are creation-time settings, not alter operations.

Example request bodies for `-f`:

```json
{"name":"app","maxUserConnections":20,"locked":false}
```

```json
{"name":"appdb","characterSet":"utf8mb4","collation":"utf8mb4_0900_ai_ci"}
```

```json
{"user":"app","database":"appdb","privileges":["SELECT","INSERT","UPDATE","DELETE"]}
```

```sh
karactl -n NS mysql databases create CLUSTER -f database.json
karactl -n NS mysql users create CLUSTER -f user.json
karactl -n NS mysql grants grant CLUSTER -f grant.json
karactl -n NS mysql users list CLUSTER -o json
karactl -n NS mysql grants list CLUSTER --user app --database appdb -o json
```

Capture the generated password once into an approved secret store. Do not log creation/rotation responses or put passwords in command arguments or URIs in shell history. Manage proxy-routable credentials through this interface: accounts created directly in SQL are **not** automatically synchronized into ProxySQL. `routableThroughProxy` reports managed credential ownership, not end-to-end connectivity or a grant to every database.

The UI supports account locking, maximum user connections, database/grant management and permission-gated password actions. All native MySQL access uses the `database-users` RBAC resource; read access does not authorize mutation. See [access control](../access-control.md) for policy syntax. System-account reveal requires the separate `cluster-credentials:read` permission and is not an application credential workflow.

## Password rotation

Creation and rotation return server-generated passwords once. Rotation may retain the previous password using `retainCurrent: true` in the request body. Deploy the new secret to all clients, verify it through the application endpoint, then discard the old password. The UI exposes **Rotate password**, **Keep current password until discarded** and **Discard old password**. A retained password is still valid until explicitly discarded; do not treat rotation alone as revocation.

Native mutations resolve the current writer and synchronize managed credentials to the proxies. If an outcome is unknown, inspect the account/operation state before another mutation; transport failure does not prove that account DDL failed. MySQL account DDL implicitly commits, and password changes are not a transactional rollback mechanism. Recovery also has [account-snapshot caveats](backups.md#accounts-after-recovery).

Access requests and new-member initialization remain fenced during active recovery; a completed reformation receipt does not keep those fences active. Delete and suspend reject new access mutations, but first resolve any previously sealed account intent before removing runtimes. An applied SQL receipt finishes credential publication and proxy acknowledgement without repeating SQL. An ambiguous result replays the same request identity against the native journal; only a definitive rejection restores the previous sealed indexes. Unavailable connectivity keeps the intent and runtimes intact until resolution, rather than discarding potentially committed account changes.

Restored Group Replication followers receive destructive clone permission only for their own outstanding restore receipt. Seed formation does not revoke that permission prematurely: each follower keeps it until its clone completes, then a new configuration revision revokes it before joining. Ordinary fresh-member initialization and retained completed clone receipts do not grant permission to overwrite an existing identity.

A pending PXC shared-traffic TLS rotation also fences access. Cleanup supersedes that rotation only after all runtimes have disappeared: suspension finishes publishing the common pending traffic CA and leaf certificate/key bundle before preserving volumes; deletion removes all active and pending traffic CA/leaf secrets. A persisted activation checkpoint makes interrupted pending-secret removal replayable without changing certificates under running members.

## Connect

```sh
karactl -n NS clusters connection CLUSTER -o json
karactl -n NS clusters ca CLUSTER > ca.pem
mysql --host=APPLICATION_ENDPOINT --port=3306 --user=app --password \
  --database=appdb --ssl-mode=VERIFY_CA --ssl-ca=ca.pem
```

Use the endpoint from connection status, not a data member chosen by index. The CLI prompts for the password. PXC and Group Replication use the proxy; direct connection is supported for a proxy-free MySQL singleton. Connection summaries contain no password. The default authentication plugin is `caching_sha2_password`; secure transport is required and legacy `mysql_native_password` is not enabled.

`VERIFY_CA` validates the certificate authority; use `VERIFY_IDENTITY` when the endpoint certificate and driver support hostname verification. Do not disable certificate checking merely because encryption succeeds. Download the CA through the authorized workflow, distribute it to clients and rehearse CA rotation. Driver URI TLS options differ: explicitly configure CA verification, percent-encode credentials/database names and bracket IPv6 addresses when using a URI.

Private endpoints require network access to the cluster. Public exposure is opt-in on 3306 with the configured allowlist; independently verify allowed and denied external clients. Reader port 3307 is private. TLS and an allowlist are complementary controls, not substitutes for least-privilege identities.

During failover, reconnect with bounded, idempotent application retry logic. A connection loss or timeout can occur after a commit; blindly retrying a non-idempotent write can duplicate its effect. Reader routing is not a causal-consistency guarantee; see [routing](configuration.md#application-routing).

## PXC cluster-traffic trust boundary

PXC uses a **dedicated traffic CA** for its shared Galera/SST traffic certificate and private key, separate from SQL and agent trust. SQL and agent leaves cannot authenticate replication. Member-specific SQL leaves still do not provide per-member replication identities: compromise of the shared traffic key affects the cluster-wide replication/state-transfer trust boundary, not just one member. Treat disclosure as a cluster-traffic credential compromise, including unauthorized participation or state-transfer access wherever that traffic credential remains trusted.

Restrict traffic-key access to the intended data members and protect the sealed secret store and its encryption key. Managed traffic rotation stops all data members, replaces **both the dedicated traffic CA and shared leaf/key**, and uses evidence-based recovery. Reformed members trust the new root and reject the old traffic credential. Ordinary member addition does not rotate that identity or require a TLS outage. See [offline rotation](operations.md#restart-configuration-and-scale).

Issuing a new leaf under the same trusted CA would **not by itself revoke** a stolen, still-valid certificate; replacing traffic trust is therefore part of managed rotation. Fence the compromised member and remove its access to secrets and private networking before distributing replacement material, or it could obtain the new credential. Verify old/untrusted credentials can no longer join or transfer data before reopening traffic. This is cluster-wide trust rollover, not independent per-member revocation; no CRL/OCSP facility is implied.

The managed manual TLS mode requires encryption **and peer verification** on Galera/IST and SST. `pxc_encrypt_cluster_traffic=OFF` only disables upstream automatic certificate rewriting; it must never be interpreted as permitting plaintext or accepting a wrong CA. Native acceptance must reject wrong-CA traffic and SST attempts while ordinary CA-verified SQL access remains independently tested.

Server-side FILE import/export is unavailable by design: `secure_file_priv=NULL` and the managed administrative account has no `FILE` privilege, protecting traffic keys and other runtime files from SQL file access. Every actual server startup enforces that privilege policy independently of password receipts, including existing/restored accounts, without re-keying unchanged passwords. Startup restores the Group Replication write fence before admitting clients. This does not disable native XtraBackup backup/recovery.

### Replication ports and network restriction

PXC data members listen on Galera group communication `4567/tcp` and `4567/udp`, IST `4568/tcp` and SST `4444/tcp`. Karakoram never publishes these ports; only data members of the same cluster need to reach them. TLS peer verification rejects unauthorized peers but cannot stop an untrusted host on the same network from opening connections (see [hostile traffic](#hostile-cluster-traffic)). None of the registered backends offers Karakoram a private per-port or per-cluster network policy, so member-only reachability is **documented, not enforced**:

| Backend | What Karakoram enforces | Operator action for member-only reachability |
| --- | --- | --- |
| Nomad | Nothing beyond private listeners: members join the target's CNI network (`network`, default `karakoram`) shared by every job using it; the backend renders no per-port rules and no Consul service-mesh intentions. | Run PXC in a namespace whose CNI network carries only that cluster, or apply host/CNI firewall rules allowing `4567`, `4568` and `4444` only between its data members. |
| Dokploy (Swarm) | Nothing beyond private listeners: members attach to the target's attachable overlay (`network`, default per Dokploy environment) shared by every cluster in that environment and by the control plane; Swarm overlays have no per-port ACL. | Use a dedicated Dokploy environment or `network` for the PXC cluster and attach no other workloads to it. |
| Zerops | Nothing beyond private listeners: every service in a Zerops project shares the project private network; Zerops firewall allowlists apply only to public port routing. | Place the PXC cluster in a dedicated project when other services in the project are not trusted. |

Verify the chosen restriction from a non-member runtime: connections to `4567`, `4568` and `4444` should fail at the network layer, not only at TLS.

### Hostile cluster traffic

Peer verification is an admission control, not a denial-of-service boundary. Karakoram makes **no availability claim** for a member whose replication ports are reachable by hostile clients.

Observed with PXC 8.4.11-11.1 and 8.0.46-38.1: a TLS client presenting an untrusted certificate (signed by the SQL CA, or self-signed) connected to a joining member's IST port while a legitimate IST was running. The joiner sent the expected certificate alert and admitted no data, but its `mysqld` then aborted (`terminate called after throwing an instance of 'gu::Exception'`, `mysqld got signal 6`). Upstream reset that member's `grastate.dat` to the zero UUID with `seqno: -1` and `safe_to_bootstrap: 0`, the documented state after an interrupted transfer. The remaining quorum kept every row and GTID. Upstream issue trackers contain no matching report; related fixed issues are PXC-4167 (uncaught accept exception, signal 6), PXC-4631 (zero-UUID reset after a failed IST) and PXC-4845, plus PXC-3741 (IST hang after network loss).

Recovery uses the ordinary product path, whether the member is stopped by an operator first or restarted by its supervisor:

- The member accepts upstream's exact reset only under its own cluster and member receipt, in the pinned history (a control-plane re-formation of that history may raise the formation epoch), and only when its last launch never reached Synced. Any other state identity stays fatal. Galera writes the same tuple when it evicts a Synced member for an inconsistency. That case also stays fatal and needs explicit recovery or replacement, not an automatic rejoin with inconsistent data.
- Relaunch waits 130 seconds after a launch that never reached Synced. Upstream's donor keeps retrying the joiner's SST address (30 attempts, bounded by the rendered `donor-timeout=3`). A joiner started inside that window could consume the stale stream of the aborted request and wait indefinitely for an IST that no donor sends.
- The member then rejoins by IST or SST and re-establishes its identity only once it is Synced in the same history.

Repeated hostile connections can repeat the abort and keep a member out of the cluster indefinitely. Restricting the replication ports to cluster members is the only mitigation (see above).

