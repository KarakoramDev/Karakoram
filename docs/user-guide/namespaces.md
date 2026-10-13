<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Namespaces

A namespace is one existing Zerops project managed by this control plane, with its own project-scoped infrastructure token. Namespace `default` is the control plane's project: its token comes from `KARAKORAM_ZEROPS_TOKEN`, networking is direct and no relay is needed. Other projects use an outbound relay deployed by Karakoram. This is management routing and RBAC scope, not hostile tenant isolation or an application VPN. See [architecture](../architecture.md#namespaces-and-relay-routing), [security](../security.md#relay-threat-model) and [limitations](../limitations.md#namespaces-and-relays).

## Names and selection

Namespace names match `^[a-z][a-z0-9-]{0,30}[a-z0-9]$`: 2–32 lowercase letters, digits or hyphens, beginning with a letter and ending with a letter/digit. `default` is reserved. One project may belong to only one namespace, including the control plane's project. Names/project identity cannot be changed; only description can be edited. Cluster and backup-storage names are unique **within** a namespace, so `dev/orders` and `prod/orders` are separate objects with separate secrets and history.

CLI single-object/create/mutation commands resolve namespace as **`--namespace/-n` > saved context namespace > `default`**. Collection commands without an explicit `-n` list all authorized namespaces; with `-n` they filter. Save a default for object commands with `karactl config set-context production --namespace prod`. Responses carry `namespace`, and unfiltered tables identify it.

`karactl namespaces list` follows every authorized result page in all output formats.

The UI namespace switcher offers **All namespaces** and authorized namespace names with health indicators. Selection is carried in `?namespace=` and a non-secret localStorage preference; cluster detail routes include the namespace. Choose an explicit namespace when creating a cluster/storage. A shared link does not bypass authorization. Reading namespace metadata is separate from reading its clusters.

The UI switcher, namespace settings list and namespace selectors follow every authorized result page, rather than stopping at the first page.

## Add, validate and rotate

Create a Zerops token with **Custom access per project → Full access** for the additional project only. Organization-wide authority and project-creation rights are unnecessary. The control-plane public HTTPS origin must be reachable from the project, with WebSocket Upgrade allowed at `/relay/v1/connect`. Allow capacity for the relay and a free relay hostname (normally `karakoramrelay`). See [installation prerequisites](../install.md#adding-a-namespace).

Use **Settings → Namespaces → Add namespace**, enter name/project/token/optional description and select **Validate and add**. The server validates token/project before saving; relay deployment then proceeds asynchronously. The token field is write-only and is not returned by namespace reads.

```sh
# The token file must be private and untracked; '-' reads stdin.
karactl namespaces create --name dev --project-id YOUR_PROJECT_ID \
  --description 'Development databases' --token-file namespace-token.txt
karactl namespaces list
karactl namespaces get dev -o yaml
karactl namespaces validate dev
karactl namespaces update dev --description 'Development and test'
karactl namespaces rotate-token dev --token-file replacement-token.txt
karactl -n dev clusters list
```

Do not place the token in an argument value, committed file or shell trace. Creation rejects invalid credentials/project scope, a duplicate project or an occupied relay hostname with **422**; an existing namespace name returns **409**. Missing/invalid request shape may return **400**. A valid token is not proof that the relay is connected: wait for relay `Ready` and `connected: true`.

Re-validation returns updated health; an invalid stored token is represented by `token.valid: false` and `lastError`, not necessarily an HTTP error. Automatic checks refresh token health about every five minutes. Rotation validates access to the **same project**, persists the replacement and changes the Backend's token without restarting the relay/session. Revoke the old Zerops token only after successful rotation. For `default`, API rotation returns **409**: update the control-plane service secret and restart instead.

Validation confirms project identity and service-list access; it does not probe every import/deploy/secret mutation. A token that can only read the project may pass validation and then fail relay deployment. Supply the required Full access scope and inspect both token and relay health.

## Relay health

Karakoram deploys one private Ubuntu runtime running `karakoram relay`, with TCP 9443 and no public access. It dials out over WSS, inner mTLS 1.3 and smux to the control plane. Remote agents pull through `https://<relay hostname>:9443` with end-to-end internal TLS; database/agent commands travel in the opposite direction. Backup data goes directly from members to S3.

The derived relay hostname normally appends `relay` to the control-plane
hostname. If that exceeds 25 characters, it uses the first sixteen characters,
four hexadecimal SHA-256 characters and `relay`. An explicit
`KARAKORAM_RELAY_HOSTNAME` applies to new enrollments; existing namespaces keep
their recorded relay hostname. Agent URLs and internal TLS SANs use that
recorded name. Transport bounds are 1 MiB per WebSocket message and 256 active
data streams per session (control excluded), not a limit on total streamed
management payload.

Namespace detail shows:

| Field | Meaning |
| --- | --- |
| `phase` | Namespace `Pending`, `Ready`, `Error` or `Deleting` |
| `token.source`, `valid` | `env` for default or `stored`; last validation result |
| `token.lastValidatedAt`, `lastError`, `rotatedAt` | Validation/rotation timestamps and diagnostic state; no token/fingerprint |
| `relay.hostname`, `serviceId` | Private service identity |
| `relay.phase` | `NotRequired` for default; otherwise `Pending`, `Deploying`, `Ready` or `Error` |
| `relay.connected`, `connectedSince`, `lastSeenAt` | Authenticated hub session and most recent observed activity |
| `relay.version`, `desiredVersion` | Reported and desired executable version; digest mismatch triggers redeployment |
| `relay.certNotAfter` | Enrolled client certificate expiry; renewed within thirty days |
| `relay.reconnects`, `lastError` | Hub-observed reconnect count and deployment/session diagnostic |

A relay's platform readiness command probes its local private listener only; it does **not** prove tunnel connectivity. Inspect namespace health instead. Reconnect counters are process-local hub observations, not a durable lifetime SLA. Lifecycle events include `NamespaceCreated`, `NamespaceValidated`, `NamespaceValidationFailed`, `NamespaceTokenRotated`, `RelayDeploying`, `RelayConnected`, `RelayDisconnected`, `RelayTargetRefused`, `NamespaceDeleting` and `NamespaceDeleted`.

Namespace create/update/validation/token rotation/deletion and relay lifecycle
events retain both the namespace and actor. User-triggered changes identify the
initiating account or API token; controller lifecycle work uses system actors.
Audit visibility is separate from namespace metadata: `audit, read, dev/**`
allows audit records in dev, not global installation activity or other namespaces.
Actor/resource filters intersect with the namespace on lists and streams:

```sh
karactl -n dev events list --actor alice --resource namespaces
karactl -n dev events watch --resource namespaces
```

## Backup storages and cross-namespace recovery

Register storage in the cluster's namespace (`karactl -n dev storages create -f FILE`). Cluster backup settings and schedules refer to names in that same namespace. Identically named storages in different namespaces need not describe the same bucket or credentials. Member access to S3 is direct; control-plane storage verification runs from the control-plane network, not through the relay.

New backup-storage names must not contain `:` or `/`. Existing `default` storages whose names contain these characters remain readable and updatable; the creation restriction does not rename or invalidate them.

For a snapshot from `prod/orders`, restore into an existing compatible `dev/orderscopy`:

```sh
karactl -n dev restores create orderscopy recover \
  --source-namespace prod --source-cluster orders --backup baseline
```

For a new cluster, submit a complete create file whose `spec.dataSource` contains:

```yaml
dataSource:
  namespace: prod
  cluster: orders
  backup: baseline
```

Then use `karactl -n dev clusters create orderscopy -f FILE --wait`. Source namespace defaults to the target namespace when omitted; in-place REST restore uses `sourceNamespace`, while cluster creation uses `spec.dataSource.namespace`. The live-source convenience clone supports `--source-namespace` too. Recovery uses the source backup's recorded storage, then restores the target's isolated backup configuration; ensure target members can reach that S3 destination. All normal topology/version, snapshot/PITR and protected recovery-identity checks still apply. See [backups and restore](backups-and-restore.md#cross-namespace-recovery).

Authorization requires `backups/read` on the source and `restores/create` on an existing target or `clusters/create` on a new target, **plus `backups/export` on the source** when namespaces differ. Export alone grants neither target mutation nor backup read. Admin/operator have export by default; viewer does not.

## RBAC examples

Add these rules to a complete policy retaining administrator access:

```csv
# Full development authority, read-only staging.
p, user:alice, *, *, dev/**
p, user:alice, *, read, stage/**
# Production reads, explicitly excluding credential reveal.
p, user:alice, *, read, prod/**
p, user:alice, cluster-credentials, read, prod/**, deny
# Credential reveal and namespace metadata are separate grants.
p, user:bob, cluster-credentials, read, prod/orders
p, user:bob, namespaces, read, prod
# Audit visibility is separate from namespace metadata.
p, user:alice, audit, read, dev/**
p, user:alice, namespaces, read, dev
# Permit source export; backup read and target mutation still required.
p, user:bob, backups, export, prod/**
```

`dev/**` matches the namespace itself and its objects; `dev/*` does not span additional `/` separators. Audit uses `<namespace>/<cluster>` for cluster records, `<namespace>` for namespace-level records and global `*` for installation records. Existing object `*` normalizes to `**`, and legacy bare cluster and audit patterns normalize to `default/<pattern>`; use `dev/**` rather than bare `dev` for namespace-wide audit. Bare `namespaces` objects remain namespace names. Namespace creation requires `namespaces/create` on global `*`, not merely `dev/**`. See [RBAC policy format](rbac-and-auth.md#policy-format).

On **fresh installations**, the built-in operator and viewer roles include `namespaces, read, **` for namespace metadata across all namespaces; neither grants namespace creation, updates or deletion. Upgraded installations keep their stored policy, and normalization does not add these grants. To let those roles see namespaces, add the following to the existing complete policy:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants, the UI shows only `default`; existing cluster permissions do not imply namespace metadata access. Narrower namespace-read rules may be used instead. See [upgrade policy guidance](../upgrade.md#existing-single-project-installations).

## Remove

```sh
karactl namespaces delete dev
karactl namespaces list
```

Deletion returns **202** and removes the owned relay before deleting namespace credentials/state; it does not delete the Zerops project. **409** guards deletion while any cluster (including deleting), backup storage, retained backup record or pending orphaned-volume record remains. Remove those resources with their normal lifecycle commands and wait for cleanup. `default` cannot be deleted. The UI removal dialog explains the guard. RBAC policies are **not** removed automatically: review obsolete grants before reusing a namespace name.

Deletion revokes the namespace's hub session and pending admissions, preventing
stale relay state from routing a reused name into the old project. Review the
previous policy grants before reuse; credential/session revocation does not
remove authorization rules. See [relay security](../security.md#relay-threat-model).

After deletion completes, registering the same name creates a new namespace
with a fresh creation timestamp, description and relay enrollment. Reconciliation
checkpoints and sealed credentials from the removed registration cannot overwrite
the replacement; relay provisioning starts again from `Pending`.

## Troubleshooting

- **Token invalid:** re-validate, check project-scoped Full access and rotate with a replacement for the same project. Updating a Karakoram API token does not fix the infrastructure token.
- **Relay Pending/Deploying/Error:** inspect `relay.lastError`, project capacity, owned service identity and Zerops deployment/runtime logs. Do not replace enrollment secrets manually or adopt an unrelated service occupying the hostname.
- **Relay disconnected:** verify the configured public HTTPS origin and certificate, outbound reachability and WebSocket forwarding; inspect connection/last-seen/reconnect fields. The tunnel reconnects automatically. Database replication may still be healthy.
- **Target refused:** the `RelayTargetRefused` event (at most one per minute per namespace) names the target and the relay's reason. While members are created or removed, `target hostname could not be resolved` and `target TCP connection failed` are expected, and `target is not in namespace policy` can appear once before the relay receives the updated policy. Persistent refusals need a check of the generated member/router hostnames. Only current policy targets on 27017/9443 are allowed. Do not broaden policy to reach `core`, IP literals or arbitrary services.
- **Agents cannot pull:** check relay session, private listener, internal CA/relay hostname SAN and assigned member token. Never disable TLS verification to bypass a mismatch.
- **403 recovery:** check source backup read/export and target mutation objects, including explicit denies. **409 removal:** inspect retained backups, storages and orphan inventory in that namespace, not only the cluster list.

See [namespace operations](../operations.md#namespace-and-relay-operations) for outage and upgrade handling.
