<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Nomad backend (experimental)

The `nomad` backend runs Karakoram clusters on a [HashiCorp Nomad](https://developer.hashicorp.com/nomad) cluster that you operate. It is **experimental**, with no production track record. Zerops remains the reference backend. The runtime and scheduling behavior described here was exercised on Nomad **2.0.7**; do not assume that an untested Nomad upgrade preserves it. Design rationale: [ADR 0033](adr/0033-nomad-backend.md).

[PostgreSQL](postgres/README.md) and [Supabase](supabase/README.md) use native preparation/supervision under this backend and remain experimental here; this is not an engine acceptance claim. Allow egress to Percona repositories, locked native artifacts/operator mirror and registered S3 storage. `native_artifact_mirror` configures the shipped control-plane job's artifact mirror. Supabase `gateway.publicHttps` is rejected because this backend has no managed HTTPS capability; keep the private gateway and supply application ingress outside that option. Public PostgreSQL TCP needs the existing operator load balancer/firewall arrangement and allow-all acknowledgement.

## How it maps

|Karakoram|Nomad|
|---|---|
|Namespace target (`projectId`)|One existing Nomad namespace; its name is the target ID, not the Karakoram namespace name|
|Member runtime|One service job named after the member hostname, normally one allocation with an `exec` task; no Docker or `raw_exec`|
|Member DNS name|All registrations of own-namespace Nomad native services (`provider = "nomad"`, allocation address) rendered into each allocation's watched `/etc/hosts`, including its own job name|
|Persistent volume|A dynamic host volume using the built-in `mkdir` plugin; a separate Karakoram identity survives deletion of the runtime|
|Placement|Karakoram selects a node when creating the volume; its runtime follows that node. Replica-set/cluster volumes are spread across distinct eligible nodes while enough nodes exist.|
|Runtime image|An operator-provided, read-only Ubuntu 24.04 minbase rootfs; Nomad copies it into each allocation before engine preparation|
|Deployment|Nomad downloads the `karakoram` archive with an exact SHA-256 checksum; the allocation runs the engine's normal prepare commands|
|State and secrets|Non-secret service records in Nomad Variables `karakoram/services/<hostname>`; environment and secrets in `nomad/jobs/<hostname>`, rendered under `secrets/`|
|Public TCP|Static host ports forwarded by CNI `portmap`; an operator load balancer routes each public port to the member's node|
|Public HTTPS|Not provided by the backend; the operator supplies HTTPS/WebSocket ingress for the control plane|

The runtime layout is `/opt/karakoram` for binaries and `/var/lib/karakoram/volumes/<volume-hostname>` for mounted data. Engine preparation runs as `karakoram` with passwordless sudo. A short root entrypoint bounds capabilities, takes the volume's lifetime writer lock, waits for its own service hostname, and restores sudo's setuid permission in the private copy for prepare. After prepare and binary installation it removes the sudoers grant and resets sudo to mode 0755 **before** sourcing secrets and dropping to uid/gid **10001**. Every start recreates the prepare-only sudo grant, including restarts that reuse a prepare marker. Engine data must remain on the member volume, not the disposable allocation rootfs.

## Capabilities

|Capability|Nomad|Notes|
|---|---|---|
|`publicTcp`|yes|Static host ports; `publicIPv4`/`publicIPv6` must reach an operator-managed load balancer that forwards each port to the correct node. This is not a routing mesh.|
|`tcpAllowlist`|no|Public TCP requires an allowlist covering the whole address family and explicit `allowAllAddresses: true`. Enforce narrower access in your firewall/load balancer.|
|`publicHttps`|no|No managed domains or certificates for Kibana/RabbitMQ management; an external proxy does not enable the backend capability.|
|`placement`|yes|Authoritative Nomad node per member (`placementGroup` = node name, falling back to node ID).|
|`logs`|yes|Nomad stdout/stderr; no native per-line timestamps. See [limitations](#operations-and-limitations).|
|`dedicatedCpu`|yes|Nomad `cores` reservations; requires a whole number of at least one core. Shared CPU uses Nomad CPU units.|
|`horizontalScaling`|yes|Stateless components scale by allocation count; a volume-bearing runtime always has one allocation.|
|`diskGrowth`, `volumeQuota`|no|The `mkdir` plugin ignores requested capacity. No enforced quota or supported growth operation.|
|`scopedTokens`|no|Namespace ACLs authorize writes, but Nomad 2.0.7 leaks other namespaces' host-volume list metadata and native-service metadata; see [ACL policy](#acl-policy).|
|`exec`, `metrics`|no|Nomad has native allocation exec/stats APIs, but Karakoram does not expose or consume them under the backend contract ([ADR 0030](adr/0030-backend-neutral-infrastructure-contract.md)).|
|`envChangeRestarts`|no|Variable changes apply on the next restart, not immediately.|

## Requirements

- Linux amd64 clients, Ubuntu **24.04** runtime rootfs, Nomad `exec` driver and cgroups. Nomad **1.10 or newer** is the feature floor for dynamic host volumes; **2.0.7 is the only tested version**, not a promise of compatibility with all versions between 1.10 and 2.0.7 or with later releases. Validate other versions before use.
- Dedicated Nomad clients and a cluster whose **every `submit-job` principal is trusted**, preferably a dedicated cluster. In the tested Nomad 2.0.7 configuration, any root exec job scheduled on these clients gets an unbounded root capability set unless its own entrypoint limits it. A malicious submitter can bypass Karakoram's entrypoint and compromise the host; a node pool and `allow_caps` do **not** prevent this. `chroot_env` and root username policy are client-wide; do not adapt a general-purpose pool to these jobs.
- Nomad ACLs and native service discovery enabled; one existing Nomad namespace **dedicated exclusively to Karakoram (no other jobs or services)** and a client ACL token per target. Every namespace-native service is trusted for `/etc/hosts`: a foreign registration can hijack a member hostname and redirect engine traffic. Runtime import rejects an existing Nomad job without a Karakoram record, but this is not a replacement for namespace exclusivity. The built-in dynamic host-volume `mkdir` plugin needs no external executable.
- The read-only rootfs below, mounted on a **different filesystem/device from Nomad's allocation/data directory**. Nomad otherwise hardlinks source files, allowing writes in an allocation to affect the source. A read-only directory on the same filesystem is not sufficient.
- Routed CNI bridge networking: one non-overlapping subnet per node, routes between all nodes and from the control plane to all allocation subnets, working allocation DNS, and outbound HTTPS for Ubuntu/Percona/RabbitMQ/Elastic package sources and artifacts. Members also need access to configured S3 endpoints.
- Enough node disk for persistent data, rootfs copies, package caches, extracted archives and allocations awaiting garbage collection. Requested volume sizes do not reserve space.
- Client `max_kill_timeout` at least **120 s**, so the backend's 120 s shutdown grace period is not truncated.

### Build and mount the node rootfs

Run as root on each dedicated client, or distribute an identically built, patched and versioned image to them. The following example builds a fresh 2 GiB ext4 image; choose paths that do not already contain an image or mounted filesystem:

```sh
apt-get update
apt-get install -y debootstrap e2fsprogs util-linux
install -d -m 0755 /var/lib/karakoram-images /opt/karakoram-rootfs
truncate -s 2G /var/lib/karakoram-images/ubuntu-noble.ext4
mkfs.ext4 -F /var/lib/karakoram-images/ubuntu-noble.ext4
mount -o loop /var/lib/karakoram-images/ubuntu-noble.ext4 /opt/karakoram-rootfs
debootstrap --arch=amd64 --variant=minbase noble /opt/karakoram-rootfs https://archive.ubuntu.com/ubuntu/

# Do not start distro services inside the allocation rootfs.
printf '#!/bin/sh\nexit 101\n' > /opt/karakoram-rootfs/usr/sbin/policy-rc.d
chmod 0755 /opt/karakoram-rootfs/usr/sbin/policy-rc.d
chroot /opt/karakoram-rootfs /bin/sh -ec '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates sudo curl gnupg util-linux numactl
  groupadd --gid 10001 karakoram
  useradd --uid 10001 --gid 10001 --create-home --shell /bin/bash karakoram
  install -d -m 0755 /etc/sudoers.d
  printf "%s\n" "karakoram ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/karakoram
  chmod 0440 /etc/sudoers.d/karakoram
  apt-get clean
'
umount /opt/karakoram-rootfs
mount -o loop,ro /var/lib/karakoram-images/ubuntu-noble.ext4 /opt/karakoram-rootfs
```

Persist the read-only mount in `/etc/fstab`:

```fstab
/var/lib/karakoram-images/ubuntu-noble.ext4 /opt/karakoram-rootfs ext4 loop,ro 0 0
```

The loop-mounted ext4 source and Nomad's data directory must report different device IDs (`stat -c '%d %n' /opt/karakoram-rootfs /var/lib/nomad`, substituting your actual Nomad `data_dir`). Keep the source read-only for the lifetime of every client. Rebuild/patch images under a maintenance procedure rather than modifying a live source. Nomad copies strip setuid bits; the generated entrypoint restores sudo only in the allocation's private copy.

Example dedicated-client configuration (merge with your TLS, server addresses and other client settings):

```hcl
client {
  enabled          = true
  node_pool        = "karakoram"
  max_kill_timeout = "120s"
  cni_path         = "/opt/cni/bin"
  cni_config_dir   = "/opt/cni/config"
  chroot_env = {
    "/opt/karakoram-rootfs" = "/"
  }
  options = {
    # Exclude exec from the stock root-username check; raw_exec stays disabled.
    "user.checked_drivers" = "raw_exec"
  }
}

plugin "exec" {
  config {
    allow_caps = [
      "audit_write", "chown", "dac_override", "fowner", "fsetid",
      "setgid", "setpcap", "setuid", "sys_chroot"
    ]
  }
}
```

The backend requests those nine capabilities and bounds them with `setpriv` **before** preparation. In our tests Nomad 2.0.7 root `exec` tasks received an unbounded root capability set despite `cap_drop`/`cap_add` and the client `allow_caps` setting. The allowlist rejects disallowed capability **requests**, but is **not a mitigation against a malicious root job**; only Karakoram's own entrypoint imposes its kernel bound. All principals able to submit jobs that can reach these clients must be trusted, across all namespaces. A node pool is not an ACL boundary; use a dedicated trusted cluster or separately enforce equivalent placement governance. Do not enable `raw_exec` or grant `SYS_ADMIN` to make package installation work.

For Elasticsearch, persist `vm.max_map_count=262144` in `/etc/sysctl.d/90-karakoram.conf` and apply with `sysctl --system`. Enable `net.ipv4.ip_forward=1` for the routed network. THP and NUMA policy are host-level operator settings, not per-allocation tuning.

Add a `systemctl edit nomad` drop-in, adjusting the mount path if needed:

```ini
[Unit]
RequiresMountsFor=/opt/karakoram-rootfs

[Service]
LimitNOFILE=1048576
LimitNPROC=infinity
LimitMEMLOCK=infinity
TasksMax=infinity
Delegate=yes
```

Run `systemctl daemon-reload` and restart the client in a maintenance window after installing the mount, CNI and client configuration. Preserve Nomad's supported service shutdown configuration; do not kill database allocations incidentally during node maintenance.

### Routed CNI network

Install the standard CNI `loopback`, `bridge`, `host-local` and `portmap` binaries in `cni_path`. The custom conflist must **invoke `loopback`**, not merely have its binary installed: a fresh allocation needs interface `lo` up for local engine communication. Each node needs a conflist with the **same network name** and a **different subnet**. For example, `/opt/cni/config/karakoram.conflist` on the first client:

```json
{
  "cniVersion": "1.0.0",
  "name": "karakoram",
  "plugins": [
    {"type": "loopback"},
    {
      "type": "bridge",
      "bridge": "kk-alloc",
      "isGateway": true,
      "ipMasq": false,
      "ipam": {
        "type": "host-local",
        "dataDir": "/run/cni/networks",
        "ranges": [[{"subnet": "10.240.1.0/24", "gateway": "10.240.1.1"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      },
      "dns": {"nameservers": ["192.0.2.53"]}
    },
    {
      "type": "portmap",
      "capabilities": {"portMappings": true},
      "snat": true
    }
  ]
}
```

Check that `lo` is **UP in a fresh allocation** after installing this configuration. A custom bridge/portmap chain without `loopback` left it down in our tests, causing RabbitMQ's Erlang distribution startup to fail. Fix the operator CNI chain and create a fresh allocation; do not grant tasks `NET_ADMIN` or add an application-side loopback workaround.

Keep host-local IPAM's `dataDir` on **tmpfs**, such as `/run/cni/networks` above, so allocation bookkeeping is cleared on node reboot. In the live node-loss test, persistent host-local state survived a host OOM/reboot and the restored allocation failed with `duplicate allocation is not allowed`, leaving its job dead. `/run` must actually be tmpfs on the client; ensure the CNI path exists/is writable when allocations start. This is networking bookkeeping, not database storage: persistent engine data remains on its host volume.

Use `10.240.2.0/24`, `10.240.3.0/24`, etc. on other clients, or choose another non-overlapping private range. Replace `192.0.2.53` with a DNS resolver **reachable from allocations**; it is an example address, not a public resolver. Put `dns` on the bridge plugin as shown. The host's loopback resolver (such as `127.0.0.53`) is not reachable inside the allocation network namespace. DNS here resolves package/S3/external names; member discovery uses Nomad native services and `/etc/hosts`, not Consul DNS.

On every node, route each remote allocation subnet through the private address of its owning node, for example:

```sh
ip route replace 10.240.2.0/24 via <second-node-private-ip>
ip route replace 10.240.3.0/24 via <third-node-private-ip>
```

Install equivalent routes on the control-plane host and any private reverse proxy, and persist them with your network configuration. **Restrict routes and forwarding to allocation subnets to the nodes, control plane and authorized load balancer/proxy**; do not route the whole VPC or public clients into them. Allocation IPs expose engine and agent listeners to anything with a permitted route. Allow only required forwarding and return traffic in host/network firewalls. Provide upstream routes or egress masquerading for outbound internet access; if using NAT, exclude inter-allocation/private routed traffic. `ipMasq: false` deliberately avoids hiding allocation addresses from peers. Verify return routes, DNS and package downloads before admitting workloads.

All allocations can listen on the same engine ports because each has its own IP. Member jobs and the supplied control-plane job declare **no private/agent host port**: their native service uses numeric allocation port 9443 without a network port label (supported in the 2.0.7 lab). CNI `portmap` forwards **every declared host port**, including dynamic ports, not just those named public. Enforce node-firewall rules for the configured dynamic port range (Nomad's default is **20000–32000**) and all static published ports; deny untrusted ingress and permit published ports only from the intended LB/proxy sources. Check forwarded/DNAT traffic too, rather than relying solely on local-input rules. The control-plane HTTP host port is disabled by default.

The watched hosts template includes **all registrations**, even multiple registrations of the job's own name; it has no explicit `NOMAD_ALLOC_IP_*` self line and no readiness check that could block discovery during engine bootstrap. Group registration preceded first render in the lab. After capability bounding and any writer fence, each entrypoint polls `getent hosts "$NOMAD_JOB_ID"` every second for up to **120 s** before prepare/exec; on timeout it emits a warning on stderr and continues. `change_mode = "noop"` keeps hosts current without restarting the task.

## ACL policy

Use a client token, not a management token, for each target. The Nomad namespace must contain only Karakoram jobs/services, because every native service in it can supply trusted hosts entries. An operator must create that namespace and attach a policy like this (replace `databases` and `PREFIX`):

```hcl
namespace "databases" {
  capabilities = [
    "list-jobs", "read-job", "submit-job", "read-logs",
    "alloc-lifecycle", "host-volume-create", "host-volume-read", "host-volume-delete"
  ]
  variables {
    path "karakoram/*" {
      capabilities = ["read", "list", "write", "destroy"]
    }
    path "nomad/jobs/*" {
      capabilities = ["read", "list", "write", "destroy"]
    }
    path "nomad/jobs/karakoram" {
      capabilities = ["deny"]
    }
  }
}
node {
  policy = "read"
}
host_volume "kk-PREFIX-*" {
  policy = "write"
}
```

`PREFIX` is the **first eight lowercase hex characters of SHA-256 of the exact Nomad namespace name**, with no trailing newline. Compute the full allowed name prefix with:

```sh
namespace=databases
printf '%s' "$namespace" | sha256sum | cut -c1-8 | xargs printf 'kk-%s-\n'
```

A physical member volume is named `kk-<hash>-<hostname>`; use that prefix plus `*` in the `host_volume` rule, not `*` for every host volume. The namespace host-volume capabilities authorize creation/deletion; the separate `host_volume` write rule is required to **mount** it. No `read-fs`, `alloc-exec`, `dispatch-job`, agent or operator grants are needed: the backend consumes logs, not filesystem cat/list or exec endpoints. The exact Variables deny for `nomad/jobs/karakoram` takes precedence over the wildcard, protecting the control plane's master key, admin password, ACL token and TLS material; its own workload identity still renders its templates. Register the control-plane job and provision its volume with an **operator token**, not this restricted client token. Deliberately omit a `host_volume "karakoram-data"` mount grant. Node read is cluster-wide; the token can explicitly select a node when creating a volume, so a Karakoram `nodePool` setting is not an ACL placement boundary.

**Control-plane storage boundary.** Omitting its mount grant does **not** protect a dynamic control-plane volume from deletion: `host-volume-delete` authorizes deletion of any unclaimed dynamic host volume in the target namespace, including `karakoram-data` during a stop-first upgrade. The supplied dynamic-volume example assumes a trusted backend token. To keep SQLite/PKI storage outside this API, provision a static client-configured `host_volume` on only the chosen client, pin the control-plane job there, and keep its mount grant operator-only; static host volumes are not deleted through the dynamic-volume API. Do not give the backend token to untrusted users.

**Not hostile-tenant isolation.** In Nomad 2.0.7 a namespace-scoped token cannot write other namespaces' jobs, volumes or Variables, but wildcard host-volume listing leaks other namespaces' volume names/IDs/node IDs/capacities. Default workload identities can also query native-service metadata across namespaces. Karakoram uses exact-namespace requests and templates only query their own namespace; these precautions do not repair the upstream metadata isolation. This is why `scopedTokens` is false. Use a dedicated cluster/topology when those metadata are sensitive.

## Target settings

Set `KARAKORAM_BACKEND=nomad` for the default namespace. Additional namespaces use the same non-secret keys in their immutable `config` object and supply the ACL token through the write-only token input.

|Config key|Environment variable (default namespace)|Meaning|
|---|---|---|
|`address`|`KARAKORAM_NOMAD_ADDR`|Nomad API origin, e.g. `https://nomad.example.com:4646`; no path, credentials, query or fragment|
|`projectId` (namespace field)|`KARAKORAM_NOMAD_NAMESPACE`|Existing Nomad namespace name; required|
|—|`KARAKORAM_NOMAD_TOKEN`|Nomad ACL token secret ID; required|
|`region`|`KARAKORAM_NOMAD_REGION`|Optional Nomad region; omitted uses Nomad's default region|
|`datacenters`|`KARAKORAM_NOMAD_DATACENTERS`|Comma-separated datacenter names (or `*`); default `*`|
|`nodePool`|`KARAKORAM_NOMAD_NODE_POOL`|Dedicated client node pool; code default `default`, but configure your prepared pool explicitly|
|`network`|`KARAKORAM_NOMAD_NETWORK`|CNI conflist name; default `karakoram` (jobs use `cni/karakoram`)|
|`publicIPv4`|`KARAKORAM_NOMAD_PUBLIC_IPV4`|IPv4 address clients use for public TCP, normally the operator load balancer|
|`publicIPv6`|`KARAKORAM_NOMAD_PUBLIC_IPV6`|IPv6 address clients use for public TCP; requires matching operator ingress support|

These settings are **operator-wide**, not accepted namespace config keys:

|Environment variable|Meaning|
|---|---|
|`KARAKORAM_NOMAD_ALLOWED_ADDRS`|Comma-separated approved API origins; see URL restriction below|
|`KARAKORAM_NOMAD_CACERT`|PEM CA bundle file for Nomad API TLS; if set, replaces the system trust roots for this client|
|`KARAKORAM_NOMAD_CLIENT_CERT`|PEM mTLS client certificate file; must be paired with the key|
|`KARAKORAM_NOMAD_CLIENT_KEY`|PEM mTLS client private-key file|
|`KARAKORAM_NOMAD_TLS_SERVER_NAME`|TLS server name override for Nomad certificate verification|
|`KARAKORAM_NOMAD_ARTIFACT_URL`|HTTP(S) archive URL reachable by the control plane and Nomad clients; needed for non-release builds|

**URL restriction.** A target's API origin receives its ACL token. The control plane accepts the operator's `KARAKORAM_NOMAD_ADDR` and entries in `KARAKORAM_NOMAD_ALLOWED_ADDRS`. Arbitrary HTTPS to DNS names/public IP literals is accepted **only when both the allowlist and operator mTLS client certificate/key settings are unset/empty**. Configuring either client identity setting makes origin approval restrictive even without an allowlist: only `KARAKORAM_NOMAD_ADDR` is then approved, or no origin if it too is unset. An explicit embedding `WithAllowedAddrs` option is also restrictive. Loopback, private, link-local and shared-address literals and `localhost` require explicit approval; plain HTTP always requires it. Set an explicit allowlist in production, including for private DNS names: the implicit policy checks literals, not DNS resolution/rebinding, and is not a network egress boundary. This restriction prevents implicitly offering the shared operator client certificate to an arbitrary public HTTPS origin. API redirects are never followed. The artifact URL is separate operator-controlled configuration, not governed by this API-origin allowlist.

**mTLS.** Prefer HTTPS with Nomad HTTP TLS and ACLs enabled. Karakoram requires TLS 1.2 or newer. Mount CA/certificate/key files into the control-plane allocation and use their paths in the variables above. They are read when the backend factory is constructed, so restart the control plane after rotating them. The same TLS configuration applies to every Nomad target; namespace users cannot upload another client certificate or disable certificate verification. This is separate from Karakoram's own internal management mTLS.

## Artifact delivery

For a release build, the default archive is the official `karakoram-<version>-linux-amd64.tar.gz` GitHub release asset for the control plane's version. A non-release/local build **must** set `KARAKORAM_NOMAD_ARTIFACT_URL` to an archive containing that exact build's `karakoram` binary as a regular file at archive root:

```sh
tar -czf karakoram-nomad.tar.gz karakoram
# Publish this file at an operator-controlled URL; do not put credentials in it.
```

Before registering a deployment, the control plane downloads the archive, compares its binary with its own deployment artifact, and computes the whole archive's SHA-256. Nomad's `artifact` stanza downloads and verifies that exact archive before extraction. A mismatched binary is rejected. Keep the URL reachable from both the control plane and every client, and keep previously referenced archives available for restarts/new allocations. No artifact server is embedded in Karakoram; use an existing HTTPS/object-storage origin. TLS material configured for the Nomad API does not configure artifact downloads. Prefer HTTPS; HTTP is supported for explicitly operator-managed distribution.

## Install the control plane on Nomad

Use the supplied [control-plane job](../deploy/nomad/karakoram.nomad.hcl), [volume specification](../deploy/nomad/karakoram-volume.hcl) and [ACL policy](../deploy/nomad/karakoram-policy.hcl). Run the commands below from the repository root with the Nomad CLI configured for your cluster's API TLS and an **operator** credential. The control plane itself receives only the client token created below.

The examples default to the **Nomad namespace `karakoram`**, which backs Karakoram's logical **`default`** namespace. These names need not match. The control-plane job must run in that same Nomad namespace as its default members: its native-service hosts template cannot discover members in a different namespace. It has one allocation, a sticky single-node-single-writer volume mounted at `/var/lib/karakoram`, a lifetime volume lock, rescheduling pinned to the volume's node (exponential 30 s–1 h delays), and 12 h disconnect tolerance.

1. Prepare the dedicated clients, read-only rootfs, routed CNI, DNS, TLS and shutdown limits above. Create the target Nomad namespace and apply the supplied policy:

   ```sh
   nomad namespace apply -description='Karakoram default target' karakoram
   nomad acl policy apply karakoram deploy/nomad/karakoram-policy.hcl
   nomad acl token create -name=karakoram-control-plane -policy=karakoram
   ```

   Keep the returned **Secret ID** private; it becomes the `KARAKORAM_NOMAD_TOKEN` item in the protected Nomad Variable, never a jobspec `env` literal or CLI `-var` argument. The supplied policy allows only member volume mounts `kk-5932d69c-*` (the hash of `karakoram`), denies the exact `nomad/jobs/karakoram` Variables path, and has no control-plane volume mount grant. If changing the Nomad namespace, edit both the namespace and computed hash prefix in the policy. Continue using the **operator credential** to create the control-plane volume, write its protected Variable and register/update its job.

2. Edit the literal `node_pool` in `deploy/nomad/karakoram-volume.hcl` to your prepared pool (for example `karakoram`); the supplied value `default` is not a recommendation to use unprepared nodes. Set `node_id` to a particular eligible node if you require deterministic placement. Create the volume **once**:

   ```sh
   nomad volume create deploy/nomad/karakoram-volume.hcl
   nomad volume status -namespace=karakoram -type=host
   ```

   Wait until `karakoram-data` is ready. If using another namespace/name, edit the literal volume specification too. Volume and ACL files are **not HCL2 jobspecs**: job `-var`/`-var-file` settings do not substitute their values. The node holding this volume also holds SQLite and sealed internal PKI; never delete/recreate it as part of a binary update.

3. Create a mode-0600, untracked JSON file, for example `private-control-plane.nv.json`, containing the required Nomad Variable items:

   ```json
   {
     "Items": {
       "KARAKORAM_MASTER_KEY": "<48 random characters; keep a separate backup>",
       "KARAKORAM_ADMIN_PASSWORD": "<initial admin password>",
       "KARAKORAM_NOMAD_TOKEN": "<client token Secret ID>",
       "NOMAD_CA_PEM": "<CA certificate PEM, with JSON-escaped newlines>",
       "NOMAD_CLIENT_CERT_PEM": "<client certificate PEM, with JSON-escaped newlines>",
       "NOMAD_CLIENT_KEY_PEM": "<client private-key PEM, with JSON-escaped newlines>"
     }
   }
   ```

   Store it under the job's exact Variables path:

   ```sh
   chmod 0600 private-control-plane.nv.json
   nomad var put -namespace=karakoram -out=none \
     nomad/jobs/karakoram @private-control-plane.nv.json
   ```

   The job renders only `KARAKORAM_*` items into the environment file; TLS PEM values are rendered **only as files under `secrets/`**, readable by uid/gid 10001, never exported into the process environment. These PEM item names belong to the example job, while the backend uses `KARAKORAM_NOMAD_CACERT`/`CLIENT_CERT`/`CLIENT_KEY` file paths. All six items are required by this mTLS example. Use a trusted secret-storage workflow for this file and remove temporary plaintext copies after loading it.

4. Publish the exact release/local archive as described in [Artifact delivery](#artifact-delivery). Create an untracked **non-secret** HCL variables file, for example `control-plane.vars.hcl`:

   ```hcl
   namespace             = "karakoram"
   datacenters           = ["*"]
   node_pool             = "karakoram"
   network               = "karakoram"
   volume_name           = "karakoram-data"
   artifact_url          = "https://artifacts.example.com/karakoram.tar.gz"
   artifact_checksum     = "sha256:<archive SHA-256>"
   nomad_address         = "https://nomad.example.com:4646"
   nomad_tls_server_name = "server.global.nomad"
   public_url            = "https://karakoram.example.com"
   public_port           = 0 # Default: proxy routes directly to allocation:8080.
   public_ipv4           = "203.0.113.10"
   ```

   Compute `artifact_checksum` from the exact published archive (`sha256sum karakoram.tar.gz`); verify official release authenticity using the [installation guidance](install.md), or use your own trusted local build. `artifact_url`, `artifact_checksum`, `nomad_address` and `public_url` are required job inputs. Set the TLS server name to the name in your Nomad certificate, not necessarily the API hostname. Optional inputs include `region`, `public_ipv6`, `backup_endpoint_allowlist`, `cpu_mhz` (default 1000) and `memory_mb` (default 1024). Omit public addresses if you do not expose member TCP ports. The example explicitly restricts `KARAKORAM_NOMAD_ALLOWED_ADDRS` to `nomad_address`; edit its operator policy before managing an additional Nomad API origin.

5. Register the job:

   ```sh
   nomad job run -var-file=control-plane.vars.hcl deploy/nomad/karakoram.nomad.hcl
   nomad job status -namespace=karakoram karakoram
   ```

   The job sets `KARAKORAM_BACKEND=nomad`, its default target to the job's Nomad namespace, `KARAKORAM_DATA_DIR=/var/lib/karakoram`, and `KARAKORAM_ADVERTISE_URL=https://karakoram:9443`. Default members must resolve/reach that private name over the routed CNI network.

6. Route your HTTPS reverse proxy directly to the routed allocation IP on port **8080**. If you explicitly opt into host publication with `public_port = 8080` (or another nonzero port), route to that port on the **volume-owning node only**. This is plaintext HTTP carrying admin/API/relay traffic: firewall either path to **only your LB/proxy sources**, never arbitrary clients. The default `public_port = 0` omits the host-port block entirely. Forward WebSocket upgrades and streaming responses. Do **not** expose private port 9443 publicly. Sign in at `public_url` as `admin`, complete the required initial password change and sign in again before creating API tokens or using `karactl login` ([API bootstrap](api.md#curl-bootstrap), [CLI](cli.md#login-and-contexts)).

Back up the volume and `KARAKORAM_MASTER_KEY` separately ([Backups and storage](backups.md)). **Upgrades:** publish the new archive and update its URL/checksum, then run `nomad job stop -namespace=karakoram karakoram` **without `-purge`**, wait for every allocation to become terminal, and register the updated job with the same volume. Do not rely on `update.max_parallel = 1` to imply stop-first ordering for SQLite. Keep one writer and the lifetime lock; never launch a second control plane against the same data. Variable/TLS changes use `change_mode = "noop"` and need a deliberate restart.

## Managing Nomad from another control plane

Additional namespaces of kind `nomad` use an outbound [relay](namespaces.md), deployed in that target Nomad namespace with the same rootfs and routed CNI prerequisites. The relay connects to the control plane's public HTTPS URL, which must support WebSocket upgrades and have a certificate trusted by the Ubuntu rootfs CA bundle. Example:

```sh
karactl namespaces create --name databases --backend nomad --project-id databases \
  --config address=https://nomad.example.com:4646 \
  --config nodePool=karakoram --config network=karakoram \
  --config publicIPv4=203.0.113.10 --token-file nomad-token.txt
```

The control plane must approve that API origin and have the required operator-wide mTLS credentials. Do not put token files in source control. A Karakoram namespace name and its Nomad namespace target may differ; compute the volume ACL prefix from the **Nomad** name.

## Operator load balancers

**Public TCP.** `portmap` forwards a reserved host port only on the node running the allocation. Configure the target's public address as a stable load-balancer address, and route each advertised public port to that member's **actual node and host port**. Sending it to an arbitrary node fails; there is no Swarm-style routing mesh. Update routes when replacing a member. Karakoram does not configure or reconcile this external load balancer. Apply client source filtering at the LB and restrict **every published node port to only the LB/proxy sources** in the node's forwarding firewall; backend allowlists are not enforced. Also protect the configured dynamic host-port range against accidental publication. For public IPv6, provide a working IPv6 listener and suitable forwarding to the prepared allocation network; merely setting `publicIPv6` does not configure networking.

**HTTPS, SSE and WebSocket.** Terminate public HTTPS for the control plane at your reverse proxy and forward to its HTTP listener on port **8080**. UI live updates use **SSE** at `/api/v1/events/stream`; preserve streaming responses without buffering. The relay carrier uses **WebSocket** at `/relay/v1/connect`; preserve its upgrade and bidirectional binary traffic. Both passed through an operator HAProxy in testing. The control-plane private TLS listener on **9443** must be reachable from default-namespace members over the routed network but must not be exposed as the browser/API listener. Set `KARAKORAM_PUBLIC_URL` to the external HTTPS origin and `KARAKORAM_ADVERTISE_URL` to the private member-reachable TLS address. Member public HTTPS remains unsupported by the backend even if you manually operate a separate proxy.

## Operations and limitations

- **Node loss pins recovery to the original node.** A volume-bearing member is never automatically rebuilt on a different node: sticky single-node-single-writer storage pins replacements to its volume's node. Members and the supplied SQLite control-plane job enable unlimited failed-allocation rescheduling with exponential delays from **30 s to 1 h**, while `disconnect` retains `lost_after = "12h"`, `replace = false`, `reconcile = "keep_original"`. This is not an unconditional recovery guarantee: [Nomad still applies the deployment progress deadline](https://developer.hashicorp.com/nomad/docs/job-specification/reschedule#unlimited), configured here as **20 minutes**; a never-healthy deployment can exhaust that window and need Start/Restart. Once a deployment is healthy, later task failures can retry without an attempt limit. The volume's lifetime `flock` serializes old/replacement writers. Wait for the node to recover, or explicitly replace the member through Karakoram to rebuild elsewhere from healthy peers/backups. If no allocation is running despite desired running state, **Restart** recreates the saved job with stop → terminal → register; **Start** can likewise recover a dead or absent job. Restart of a running allocation still uses Nomad's allocation restart endpoint. Recovery requires a healthy node/rootfs/CNI and available capacity.
- **Single-writer fencing is required.** Sticky `single-node-single-writer` claims alone did not prevent old and new allocations overlapping after node-loss reconciliation in our tests. The entrypoint holds an exclusive `flock` on the member volume across preparation and the final execution; a contender waits rather than writing. Do not remove the lock file or bypass the entrypoint while a writer may exist.
- **Replacement needs headroom.** A replacement is added before its predecessor is removed. Placement ignores the replaced volume when spreading the new one, so if every eligible node is already used, the replacement may share its predecessor's node temporarily. Reserve CPU, memory, disk and host-port headroom for both, or add a node/free capacity first.
- **Destructive changes restart the member.** Deployment, resource, scale and public-port changes use explicit stop → wait for terminal allocations → register, not an assumed stop-first rolling update. Allow the full 120 s shutdown timeout plus scheduling and preparation time. Allocation completion proves task state, not engine readiness; Karakoram's normal engine health checks remain authoritative.
- **Purged running jobs are derived-object repairs.** GetService/ListServices restore an absent deployed job from its durable Variables record, preserving the service ID, deployment, environment and public ports. Repair waits for completed lifecycle operations and a **5 second registration grace**, uses Variables CAS plus absent-only job registration, and never restores desired-stopped jobs. Existing jobs must match the record ID/kind, namespace and owner marker; mismatches fail explicitly rather than being adopted. A data member is registered only while its original host-volume ID/name/node still match and the volume is ready; missing or changed storage reports **FAILED** for engine replacement, never silently creates a new volume. Stateful jobs explicitly constrain placement to the volume's recorded node.
- **Restoration cleanup belongs to the superseding lifecycle operation.** Repair persists its registration claim with Variables CAS before submitting an absent-only job, with a **10 second submission deadline**. Stop, Delete and replacement/retirement keep that durable claim and cannot finalize their terminal stop/purge phase until **60 seconds after the claim**. Delete retains both service and environment Variables during this fence; Stop retains its desired-stopped record and reports in progress rather than stopped. After the fence, operation polling re-observes the hostname, refuses foreign ownership, stops any owned job that landed late (including a restore-tagged job), and waits for terminal allocations before purging/completing. The obligation survives lost responses, caller cancellation and control-plane/backend reconstruction; failed cleanup is retried from the retained operation record. The restorer never issues a compensating stop or purge, so it cannot delete a successor.
- **Bounded-delivery assumption:** a restoration request must be delivered and committed by Nomad within the 60 second claim window, including server work that continues after the 10 second client deadline. The deadline starts before the claim is saved, leaving margin for server commit latency; clocks across control-plane processes must be synchronized. This is not a fence against an arbitrarily delayed server commit or an external writer deliberately recreating jobs after cleanup.
- **Multi-instance lifecycle exclusion.** Before stopping, purging, registering or advancing a lifecycle operation, each poll CAS-acquires a persisted lease with a random backend-instance owner and a **120 second expiry**. The complete mutation attempt has a **10 second deadline**, and no destructive request starts without at least that much lease remaining. Attempts track whether an issued mutation (including Variables CAS/phase writes) has an unacknowledged outcome. Fully acknowledged attempts, read-only failures and definitive **4xx mutation rejections** release only the exact acquired lease from fresh durable state, without persisting unsaved failed-attempt changes. Only **unknown mutation outcomes** (transport failures, deadlines/cancellation and 5xx responses to writes) retain delivery quarantine. Another attempt (including the original instance after an uncertain mutation) waits until lease expiry **plus a 60 second bounded-delivery margin** before taking over. New lifecycle operations honor the same exclusion. Ownership is checked before the initial stop; stops re-register the exact observed jobspec with `Stop=true` and an enforced job modify-index, so a changed job is not stopped. Purge requires an observed stopped job, terminal allocations and the lease; a stopping-phase poll never stops/purges its own operation's registered successor. Older records without leases remain readable and acquire one on their next mutation. These guarantees require synchronized clocks and delivery/commit of in-flight requests within the stated margin; arbitrary external writers are not serialized by a Karakoram lease.
- **Running-allocation Restart uses the same exclusion.** It acquires a durable service-level restart claim before selecting allocations, refuses held/quarantined lifecycle claims, and bounds every allocation restart request by the attempt deadline and remaining lease. It still restarts existing allocations without replacing their job or the caller's pending operation, including older deployed records without a pending handle. Successful or definitively rejected writes and read-only failures release the claim; an unknown restart-write outcome retains exclusion until safe takeover.
- Public-port methods return once the route is saved and its stop-before-start change is initiated; they do not wait for preparation. Service/container observations and operation polling advance the persisted restart, including after a control-plane restart. A later lifecycle change uses the saved ports and supersedes the pending port operation.
- **Placement blockage is pending, not terminal failure.** An evaluation that is complete but has `FailedTGAllocs`/a blocked evaluation waits for node/capacity recovery, with its placement diagnostic, until the **15 minute startup window measured from replacement registration** expires. Waiting for an old allocation to become terminal is not bounded by that startup window: a disconnected node can hold the stop fence pending. Deadline expiry or failed tasks can fail the registered replacement's operation, but **do not stop/deregister that job**: blocked evaluations and rescheduling remain active within Nomad's deployment policy. A late running allocation of the current job version makes service status **RUNNING** even when its historical operation remains failed; obsolete-version allocations do not. A subsequent Start, Restart, Deploy or changed public-port request supersedes the failure through stop → terminal → register. Retrying an unchanged **failed port operation** also restarts it; an idempotent port call does not restart a member solely because an unrelated historical operation failed.
- **Rootfs copies cost disk and time.** In our tests, new-allocation preparation used approximately **25 s / 333 MiB for RabbitMQ**, **100 s / 1.6 GiB for PSMDB + PBM**, and **95 s / 1.4 GiB for Elasticsearch**. These figures include installed packages/caches, exclude scheduling/artifact/template delays, and are not performance promises. A new allocation repeats preparation; a task restart in the same allocation can reuse its rootfs and prepare marker. Monitor garbage collection and transient extraction space as well as persistent data.
- **Disk requests are not enforced.** The built-in `mkdir` plugin returns no capacity quota; minimum/maximum sizes are metadata, not reservations. Disk growth and volume quotas are false. A custom capacity-enforcing plugin would need a separately supported lifecycle and does not enable these capabilities today.
- **Logs have no per-line timestamps.** Nomad returns plain stdout/stderr. Karakoram labels lines with the task's latest start time; `since` excludes only runs with a known finish time before that bound, not ongoing runs or individual messages. Unreadable terminal or disconnected-allocation logs are skipped, and oversized tails are truncated. Log retention is bounded by Nomad rotation/garbage collection.
- Environment and secret edits are stored immediately but apply at the next restart (`envChangeRestarts: false`).
- `keepVolumes` retains the independent Variables record and host volume; resuming uses the original node. Do not delete Variables/job/volume objects manually while Karakoram owns them. Durable operation handles track evaluations/allocations and survive control-plane restarts.
- **RabbitMQ suspension/deletion can stall on broker drain.** A three-broker quorum-queue test removed the first runtime, then remained `Deleting` because the next broker's drain repeatedly returned HTTP 503. The failure occurred in the engine's backend-neutral serial cleanup, before deleting the next Nomad runtime; a causal relationship to disappearing deleted-peer DNS was not established. Automatic suspension required an operator recovery preserving all volumes. Resume retained the same volume identities and restored all verified messages. A later full delete also needed recovery and its operation exhausted retries even though the final resource inventory was empty. Preserve storage and investigate broker/quorum health if this occurs; do not treat an accepted delete as completed suspension or bypass the writer fence. Check both operation outcome and resource inventory.
- Invalid or foreign service records do not break ordinary listings/imports. Volume deletion fails with a conflict while an undecodable record could hide a runtime's volume reference; repair such manually modified records before deleting storage.
- Namespace-scoped ACL tokens do not prevent the metadata leaks described above; native allocation exec/stats availability does not imply Karakoram `exec`/`metrics` support.
- Elasticsearch's S3 client trusts only public CAs; use a publicly trusted S3 endpoint for snapshots. For operator-approved private/nonstandard-port S3 endpoints, configure `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` ([Backups and storage](backups.md)).

## Nomad licence

Nomad Community Edition has used **BUSL 1.1 since Nomad 1.7**. Karakoram does not include or redistribute Nomad. Check [HashiCorp's licence terms](https://www.hashicorp.com/en/license-faq) for your use; this guide is not legal advice.
