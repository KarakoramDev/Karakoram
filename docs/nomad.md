<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Nomad provider

The `nomad` provider runs Karakoram clusters on a [HashiCorp Nomad](https://developer.hashicorp.com/nomad) cluster that you operate. It is **experimental**, with no production track record. [Zerops](https://zerops.io/) remains the reference provider. The runtime and scheduling behavior described here was exercised on Nomad **2.0.7**; do not assume that an untested Nomad upgrade preserves it. Design rationale: [ADR 0033](adr/0033-nomad-backend.md).

[PostgreSQL](postgres/README.md) and [Supabase](supabase/README.md) use native preparation/supervision under this provider and have not been accepted here. Allow egress to [Percona](https://www.percona.com/) repositories, locked native artifacts/operator mirror and registered [S3](https://aws.amazon.com/s3/) storage. `native_artifact_mirror` configures the shipped control-plane job's artifact mirror. Supabase `gateway.publicHttps` is rejected because this provider has no managed HTTPS capability; keep the private gateway and supply application ingress outside that option. Public PostgreSQL TCP needs the existing operator load balancer/firewall arrangement and allow-all acknowledgement.

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
|Public HTTPS|Not provided by the provider; the operator supplies HTTPS/WebSocket ingress for the control plane|

The runtime layout is `/opt/karakoram` for binaries and `/var/lib/karakoram/volumes/<volume-hostname>` for mounted data. Engine preparation runs as `karakoram` with passwordless sudo. A short root entrypoint bounds capabilities, takes the volume's lifetime writer lock, waits for its own service hostname, and restores sudo's setuid permission in the private copy for prepare. After prepare and binary installation it removes the sudoers grant and resets sudo to mode 0755 **before** sourcing secrets and dropping to uid/gid **10001**. Every start recreates the prepare-only sudo grant, including restarts that reuse a prepare marker. Engine data must remain on the member volume, not the disposable allocation rootfs.

The exec task inherits the client's OS/UTS hostname, which can differ from the
Nomad node name and the member service name. Its allocation-private, watched
`/etc/hosts` also maps Nomad's `attr.unique.hostname` to `127.0.1.1`, unless that
name already has a native-service address (service addresses take precedence).
This avoids repeated external hostname lookups by sudo during preparation even
when the client hostname is absent from allocation DNS. Discovery updates retain
the local alias; neither node `/etc/hosts` nor shared DNS is modified. Keep the
client OS hostname stable while Nomad is running so its fingerprint matches the
inherited UTS hostname.

Each replacement still copies the rootfs and installs the pinned engine packages
in its own allocation; there is no shared prepared-runtime cache. Repository,
download and package-install time therefore still contributes to replacement
latency. The hostname mapping applies to newly rendered job specifications;
upgrading the control plane alone does not rewrite running jobs or their templates.

### Platform-care policy

[Nomad](https://developer.hashicorp.com/nomad) job registration submits desired scheduler state, producing evaluations and
deployments; it does **not** enqueue a server-side image-build pipeline. Artifact
download and native engine preparation run on the selected allocation clients under
Nomad placement/resource policies. A [Dokploy](https://dokploy.com)-style global build-slot semaphore would
therefore throttle scheduling rather than protect a shared build server, and is not
added here.

Per-runtime safety remains enforced by durable operation phases, Variables CAS,
registration delivery fences and exact job-modify-index checks. Registration resumes
by observing the operation's existing job/evaluation instead of registering it again.
Failed allocation/deployment health or the startup deadline durably fails the operation;
ordinary polling and missing-job restoration do not automatically restart a failed
operation. Operator-requested retry/restart is distinct from an automatic deployment
retry loop. Nomad's own task restart/reschedule and deployment-revert policies remain
scheduler responsibilities, not new Karakoram job submissions.

Corrected resource bounds can supersede an initial placement-blocked deployment.
[PostgreSQL](https://www.postgresql.org/) (including [Supabase](https://supabase.com/)), [MongoDB](https://www.mongodb.com/), [RabbitMQ](https://www.rabbitmq.com/),
[Elasticsearch](https://www.elastic.co/elasticsearch) and Supabase component reconciliation submit the latest bounds
before waiting for the obsolete rollout. The replacement keeps the provider's
ordinary registration, ownership, single-writer and deployment-health fences.
The provider conditionally supersedes the exact initial receipt, not an unrelated
successor operation. Already-initialized/previously-ready volume members,
observation errors and initial-deployment allocation history retain the normal
lifecycle path.

For stateless non-relay jobs, an unchanged owned job and its current blocked
evaluation may authorize non-destructive correction after Nomad garbage-collects
the registration evaluation, provided that blocked evaluation directly names the
saved registration receipt as `PreviousEval`. This preserves canary-first rollout
without a provider-issued predecessor stop; Nomad replaces remaining predecessors
destructively after promotion. It is not proof that the task never executed:
volume correction and destructive unplaced-member retirement still require their
original complete scheduling evidence and fail closed when it is missing.

For a never-ready volume member, stopping its old job is the scheduling fence;
allocation history is re-read after that fence and before replacement registration.
This is not an atomic check-and-stop: an allocation placed in the intervening
window can be stopped during initial bootstrap. If any allocation appears,
`UnplacedResizeDeferred` refuses replacement registration and terminates with an
actionable deployment-exhaustion diagnostic. The provider and engine driver retain that
terminal evidence across polling/restarts, retire the obsolete receipt and preserve
previous applied bounds; ordinary reconciliation cannot silently redeploy it.
Inspect that bootstrap allocation, then explicitly re-arm with
`karactl clusters retry-deployments CLUSTER --namespace NS`.
Re-arm reconciles the latest capacity before retrying the initial deployment.
Stateless specification updates do not issue a provider stop; Nomad retires
predecessors during the canary-first rollout.
This shortcut never authorizes stopping an unreachable previously-serving writer.

A PostgreSQL proxy count reduction can supersede an unfinished larger-count
rollout, including one blocked on public host-port placement. The control plane
submits the smaller count without waiting for the obsolete count to become
healthy, retains the already-published surge-safe connection budget, and keeps
the previous operation handle if Nomad rejects the superseding mutation. A
concurrent public-port revocation remains part of the replacement specification.
Count increases still require their normal all-instance configuration barrier.
This does not treat a running allocation with desired status `stop` as terminal:
an unreachable live proxy continues to fail readiness and data-retirement
barriers closed. A Nomad client/executor stuck in `Killing` beyond its kill timeout
requires client-side diagnosis; a count change is not proof that it exited.

### Public exposure and proxy rollout timing

Stateless specification changes register a new version of the existing owned job;
they do not stop the whole proxy job first. Only the serving-checked canary is
start-first; promotion is followed by serial stop-before-start replacements.
Each new allocation copies the rootfs, downloads the pinned artifact and runs
native preparation.
The prepare marker is allocation-local, so a replacement must prepare again.
Serving TCP checks run every 2 seconds and must remain healthy for 10 seconds.
Ready additionally requires the deployment and every live proxy instance to
finish applying its published configuration and expose a working writer route.
First successful traffic can therefore substantially precede fresh Ready.

Count-only changes (including increases and cancelling an unplaceable scale-up
back to the serving count) do not require canary capacity. The provider compares
the current owned job with the jobs embedded in healthy serving allocations,
ignoring only count and rollout policy. Increases require the existing count
healthy with that exact specification; reductions require the desired retained
count healthy. It preserves serving Meta and registers `Canary=0` with
`AutoPromote=false`: Nomad adds only the deficit or terminates only the surplus.
Polling also repairs an already-running scale-back operation registered with a
blocked canary before this policy applied. Image, configuration, resource and
public-port changes retain canary-first safety; healthy allocations running an
older specification cannot acknowledge an unready replacement.
Sparse desired jobs are compared with native canonicalized jobs without treating
Nomad's implicit SIGTERM, service-discovery and minimum-version constraints as
placement-specification removals. Only those exact server-injected constraint
shapes are defaults; removing any other placement constraint still requires the
ordinary specification rollout.

A terminal proxy rollout failure is reported as `ProxyRolloutFailed`, not an
indefinite pending rollout. PostgreSQL can request one new, retain-only scale
operation when the exact owned desired specification is already natively healthy
at the desired count, including after Nomad reverts to an older owned operation
marker. The failed handle is not revived. If that proof is unavailable, no job is
registered; if it disappears before registration, recovery fails rather than
falling back to new placement. A failed automatic attempt is not retried endlessly.
Terminal runtime failure remains an error on subsequent convergence sweeps so
the tracking operation's existing failure budget can terminate truthfully.
If the admission/completion response or engine receipt is lost, a same-target
scale adopts the durable retain-only receipt rather than starting an ordinary
canary rollout. Count-only registrations preserve the serving job's Meta because
Nomad includes job/group/task Meta in its destructive-update comparison. Their
durable receipt uses the exact native job version and JobModifyIndex instead of a
new job-Meta operation marker. Before registration, the CAS base index, expected
next version and exact normalized specification digest are persisted; a lost
response adopts only that exact content/version, never a later superseding or
auto-reverted registration. Current runtime health still gates fresh Ready.
Ownership loss before registration terminates recovery without touching the
foreign job. Terminal proxy failures are surfaced even when the proxy's native
status is unavailable or reports not-running.

An identical replica PATCH creates a new tracking operation at the same generation,
but does not force a new native rollout or override these proof checks. Restore
native health/capacity first. If a failed native deployment genuinely requires
new placement, submit a genuine corrective count/resource update through the
normal product API; unchanged PATCH is not an unsafe provider retry/re-arm.

PostgreSQL distinguishes healthy serving writer routes from missing desired proxy
instances. An unplaced desired instance keeps Ready false with
`ProxyInstanceUnplaced` and native placement details; `WriterRouteUnavailable`
is reserved for an actual serving-instance route/probe failure. Every serving
proxy still needs a fresh ONLINE hostgroup-10 route to the current primary.

Public exposure changes add/remove static host-port reservations and [CNI](https://www.cni.dev/) port
mapping, replacing allocations rather than hot-editing a firewall. Concurrent
public proxies need distinct eligible nodes with the requested port free. A
canary also needs spare placement capacity; occupied static ports can block it.
Do not assume that a blocked canary automatically stops all predecessors to free
ports, or that a load balancer's old node mapping follows a relocated allocation.
Supply sufficient capacity and operator-managed load-balancer membership.

In one two-proxy PostgreSQL exposure cycle, OFF reached fresh Ready in 143.185 s.
The subsequent ON request served its first acknowledged write after 122.310 s
and reached fresh Ready after 241.130 s. Of the combined 232.207 s acknowledgement
gap, 109.897 s preceded the ON request while public access was intentionally off;
that interval is not an ON rollout outage. These are observations, not guaranteed
latencies or proof of the time spent in each preparation/placement stage.
Deployment health/progress deadlines (10/12 minutes for stateless jobs) and the
provider's 15-minute startup window bound waiting for an unsuccessful rollout;
they are failure limits, not availability guarantees. External load-balancer
check intervals and failure/rise thresholds add operator-controlled delay.

For measurements, separate OFF submission, actual route withdrawal, ON submission,
first successful ON traffic and fresh current-generation Ready. Probe every live
proxy by allocation identity as well as the public load-balancer route, rediscover
replacements, and retain Nomad job/deployment/allocation timestamps. A single
OFF-to-ON writer gap cannot distinguish intentional denial from rollout downtime.


## Capabilities

|Capability|Nomad|Notes|
|---|---|---|
|`publicTcp`|yes|Static host ports; `publicIPv4`/`publicIPv6` must reach an operator-managed load balancer that forwards each port to the correct node. This is not a routing mesh.|
|`tcpAllowlist`|no|Public TCP requires an allowlist covering the whole address family and explicit `allowAllAddresses: true`. Enforce narrower access in your firewall/load balancer.|
|`publicHttps`|no|No managed domains or certificates for Kibana/RabbitMQ management; an external proxy does not enable the provider capability.|
|`placement`|yes|Authoritative Nomad node per member (`placementGroup` = node name, falling back to node ID).|
|`logs`|yes|Nomad stdout/stderr; no native per-line timestamps. See [limitations](#operations-and-limitations).|
|`dedicatedCpu`|yes|Nomad `cores` reservations; requires a whole number of at least one core. Shared CPU uses Nomad CPU units.|
|`horizontalScaling`|yes|Stateless components scale by allocation count; a volume-bearing runtime always has one allocation.|
|`diskGrowth`, `volumeQuota`|no|The `mkdir` plugin ignores requested capacity. No enforced quota or supported growth operation.|
|`scopedTokens`|no|Namespace ACLs authorize writes, but Nomad 2.0.7 leaks other namespaces' host-volume list metadata and native-service metadata; see [ACL policy](#acl-policy).|
|`exec`, `metrics`|no|Nomad has native allocation exec/stats APIs, but Karakoram does not expose or consume them under the provider contract ([ADR 0030](adr/0030-backend-neutral-infrastructure-contract.md)).|
|`envChangeRestarts`|no|Variable changes apply on the next restart, not immediately.|

## Requirements

- [Linux](https://www.kernel.org/) amd64 clients, [Ubuntu](https://ubuntu.com/) **24.04** runtime rootfs, [Nomad](https://developer.hashicorp.com/nomad) `exec` driver and cgroups. Nomad **1.10 or newer** is the feature floor for dynamic host volumes; **2.0.7 is the only tested version**, not a promise of compatibility with all versions between 1.10 and 2.0.7 or with later releases. Validate other versions before use.
- Dedicated Nomad clients and a cluster whose **every `submit-job` principal is trusted**, preferably a dedicated cluster. In the tested Nomad 2.0.7 configuration, any root exec job scheduled on these clients gets an unbounded root capability set unless its own entrypoint limits it. A malicious submitter can bypass Karakoram's entrypoint and compromise the host; a node pool and `allow_caps` do **not** prevent this. `chroot_env` and root username policy are client-wide; do not adapt a general-purpose pool to these jobs.
- Nomad ACLs and native service discovery enabled; one existing Nomad namespace **dedicated exclusively to Karakoram (no other jobs or services)** and a client ACL token per target. Every namespace-native service is trusted for `/etc/hosts`: a foreign registration can hijack a member hostname and redirect engine traffic. Runtime import rejects an existing Nomad job without a Karakoram record, but this is not a replacement for namespace exclusivity. The built-in dynamic host-volume `mkdir` plugin needs no external executable.
- The read-only rootfs below, mounted on a **different filesystem/device from Nomad's allocation/data directory**. Nomad otherwise hardlinks source files, allowing writes in an allocation to affect the source. A read-only directory on the same filesystem is not sufficient.
- Routed CNI bridge networking: one non-overlapping subnet per node, routes between all nodes and from the control plane to all allocation subnets, working allocation DNS, and outbound HTTPS for Ubuntu/Percona/RabbitMQ/Elastic package sources and artifacts. Members also need access to configured S3 endpoints.
- Enough node disk for persistent data, rootfs copies, package caches, extracted archives and allocations awaiting garbage collection, with the [disk and GC policy](#allocation-disk-and-client-garbage-collection) below. Requested volume sizes do not reserve space.
- Client `max_kill_timeout` at least **120 s**, so the provider's 120 s shutdown grace period is not truncated.

### Allocation disk and client garbage collection

Every native allocation receives its own rootfs copy and engine installation.
The job's `ephemeral_disk.size` reserves **MiB per allocation**, not per job or
persistent volume. New member deployments use these budgets, including space for
package caches and compressed/extracted artifacts:

|Component|Reservation (MiB)|Sizing basis|
|---|---:|---|
|MongoDB (including router/config roles)|3072|Prepared replica-set allocation measured ~1.90 GiB, plus ~1.10 GiB margin; router/config-role sizing is conservative|
|RabbitMQ|2048|Prepared allocation measured ~0.66 GiB, plus ~1.34 GiB rootfs/package/artifact growth margin|
|PostgreSQL data (including Supabase database)|3072|Provisional native-install budget including Patroni/pgBackRest and extension preparation|
|PostgreSQL ProxySQL|1024|Provisional smaller rootfs/proxy-package/artifact budget|
|Elasticsearch / Kibana|4096|Provisional larger unpacked distribution/JVM or Node.js budget|
|Logstash|3072|Provisional distribution/JVM budget|
|APM server|1024|Provisional standalone-binary budget|
|Supabase Studio|4096|Provisional Node.js dependency-tree budget|
|Supabase realtime / analytics / functions|3072|Provisional BEAM/edge-runtime and native dependency budget|
|Other Supabase components|2048|Provisional native component/dependency budget|
|Relay / shipped control-plane job|1024|Control-plane allocation measured ~0.27 GiB, plus binary/artifact margin; relay sizing is provisional; SQLite is separate|
|Unknown/custom member runtime|3072|Conservative fallback, not a bound on arbitrary prepare commands|

These are scheduler reservations, **not filesystem quotas**, and exclude all
persistent data. Measure peak preparation usage on the allocation directory
from the node (not allocation exec), for example
`du -sx --block-size=1 --exclude='*/var/lib/karakoram/volumes/*' <allocation-dir>`.
Explicitly exclude host-volume contents even when they share the allocation
filesystem; `-x` alone does not exclude same-filesystem bind mounts. Compare
with `df` on both `alloc_dir` and `data_dir`. Rootfs image,
engine version, package indexes and mirrors affect the footprint. Raise the
`ephemeralDiskMB` target setting (`KARAKORAM_NOMAD_EPHEMERAL_DISK_MB`) for larger
images/preparation; this is an upward floor over each component default, with
an accepted range of **1–65536 MiB** (maximum 64 GiB). Configuration outside this
range is rejected before deployment side effects. This ceiling is not proof
that a particular node has room. The shipped control-plane job exposes it as
`member_ephemeral_disk_mb` (default `0` means no override).
Increase the shipped control-plane job's `ephemeral_disk.size` separately if needed.
The budget is persisted on **Deploy**, before environment values are removed
from deployment metadata. Existing deployments, including legacy jobs without
an explicit disk stanza, keep their original budget across control-plane
upgrades, observation, restart, resize and scale; the next intended member
deployment adopts the new policy. No background migration rolls running members.
An explicit subsequent deployment recalculates its budget from the current
setting, so lowering a previously unplaceable override takes effect on an
operator-requested redeploy without silently rewriting the existing job.

Clients **must explicitly configure** `gc_disk_usage_threshold`,
`gc_inode_usage_threshold`, `gc_max_allocs` and `gc_interval`; the example below
uses 60%, 70%, 8 allocations and 15 seconds. Choose a lower allocation-count cap
for small disks or large chroots, and reserve at least the peak concurrent
replacement/preparation footprint plus OS and client-state space. The count
includes live allocations: GC never removes live allocations merely to meet it.
Budget persistent data separately, preferably on a separate filesystem with
its own capacity policy; `mkdir` host volumes do not enforce requested sizes.
Allow additional room for stateless canaries and terminal directories waiting
for destruction. Scheduler disk accounting does not enforce a free-space
watermark or account for all retained/orphan directories and host-volume growth.

Nomad 2.0.7 [defaults](https://github.com/hashicorp/nomad/blob/v2.0.7/client/config/config.go)
to 80% disk, 70% inodes, 50 allocations and a one-minute GC interval.
Its [client GC](https://github.com/hashicorp/nomad/blob/v2.0.7/client/gc.go)
checks the **allocation directory's filesystem**, independently of the
allocation-count limit; it does not ignore chroot bytes when measuring disk.
It collects only terminal runners known to that client, and removes them from
its queue before asynchronous destruction. A runner/executor stuck in teardown
can retain mounts/directories and a GC destruction slot indefinitely; a
“no terminal allocations” message does not prove that allocation directories
were removed. Lower thresholds are headroom, not a fix for a wedged executor.

Monitor free bytes/inodes on allocation, client-state and persistent-data
filesystems. On pressure, stop new workload churn and diagnose client teardown
before the client state database itself reaches ENOSPC. Scheduler
`DimensionExhausted.disk` already appears in `BackendPlacementFailed` member
conditions and pending operation messages; it is not engine readiness.
Allocation setup ENOSPC is a client/runtime failure, not proof of a scheduler
disk rejection. For a wedged client, use operator-controlled node maintenance:
identify the exact old allocation, prove its processes no longer own or write
any retained volume, and recover the client using your Nomad procedure.
Never blindly remove allocation directories, unmount database volumes or force
GC from a control-plane readiness loop. Karakoram does not mutate client GC
configuration or automatically reap orphan executors.

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
  # Required explicit GC policy for native per-allocation installations.
  # Re-size these thresholds/headroom for your disks and allocation churn.
  gc_disk_usage_threshold  = 60
  gc_inode_usage_threshold = 70
  gc_max_allocs            = 8
  gc_interval              = "15s"
  reserved {
    disk = 4096 # MiB for OS/client state and replacement/GC headroom; not data.
  }
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

The provider requests those nine capabilities and bounds them with `setpriv` **before** preparation. In our tests Nomad 2.0.7 root `exec` tasks received an unbounded root capability set despite `cap_drop`/`cap_add` and the client `allow_caps` setting. The allowlist rejects disallowed capability **requests**, but is **not a mitigation against a malicious root job**; only Karakoram's own entrypoint imposes its kernel bound. All principals able to submit jobs that can reach these clients must be trusted, across all namespaces. A node pool is not an ACL boundary; use a dedicated trusted cluster or separately enforce equivalent placement governance. Do not enable `raw_exec` or grant `SYS_ADMIN` to make package installation work.

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

Install the standard [CNI](https://www.cni.dev/) `loopback`, `bridge`, `host-local` and `portmap` binaries in `cni_path`. The custom conflist must **invoke `loopback`**, not merely have its binary installed: a fresh allocation needs interface `lo` up for local engine communication. Each node needs a conflist with the **same network name** and a **different subnet**. For example, `/opt/cni/config/karakoram.conflist` on the first client:

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

Use `10.240.2.0/24`, `10.240.3.0/24`, etc. on other clients, or choose another non-overlapping private range. Replace `192.0.2.53` with a DNS resolver **reachable from allocations**; it is an example address, not a public resolver. Put `dns` on the bridge plugin as shown. The host's loopback resolver (such as `127.0.0.53`) is not reachable inside the allocation network namespace. DNS here resolves package/S3/external names; member discovery uses Nomad native services and `/etc/hosts`, not [Consul](https://developer.hashicorp.com/consul) DNS.

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

Variables paths are relative to the selected Nomad namespace, not prefixed by its
name. Keep `karakoram/*` and `nomad/jobs/*` unchanged when replacing `databases`.
For example, `path "databases/*"` does not authorize the service records, and
`path "nomad/jobs/databases*"` does not authorize a relay or members with other
hostnames. Nomad can return an empty successful Variables list with such a token
while denying an exact record read. Namespace creation checks the exact relay
record as well: rejected read access returns **422 `invalid_namespace`** with a
relay read-permissions diagnostic before credentials or enrollment are saved.

`PREFIX` is the **first eight lowercase hex characters of SHA-256 of the exact Nomad namespace name**, with no trailing newline. Compute the full allowed name prefix with:

```sh
namespace=databases
printf '%s' "$namespace" | sha256sum | cut -c1-8 | xargs printf 'kk-%s-\n'
```

A physical member volume is named `kk-<hash>-<hostname>`; use that prefix plus `*` in the `host_volume` rule, not `*` for every host volume. The namespace host-volume capabilities authorize creation/deletion; the separate `host_volume` write rule is required to **mount** it. No `read-fs`, `alloc-exec`, `dispatch-job`, agent or operator grants are needed: the provider consumes logs, not filesystem cat/list or exec endpoints. The exact Variables deny for `nomad/jobs/karakoram` takes precedence over the wildcard, protecting the control plane's master key, admin password, ACL token and TLS material; its own workload identity still renders its templates. Register the control-plane job and provision its volume with an **operator token**, not this restricted client token. Deliberately omit a `host_volume "karakoram-data"` mount grant. Node read is cluster-wide; the token can explicitly select a node when creating a volume, so a Karakoram `nodePool` setting is not an ACL placement boundary.

**Control-plane storage boundary.** Omitting its mount grant does **not** protect a dynamic control-plane volume from deletion: `host-volume-delete` authorizes deletion of any unclaimed dynamic host volume in the target namespace, including `karakoram-data` during a stop-first upgrade. The supplied dynamic-volume example assumes a trusted provider token. To keep SQLite/PKI storage outside this API, provision a static client-configured `host_volume` on only the chosen client, pin the control-plane job there, and keep its mount grant operator-only; static host volumes are not deleted through the dynamic-volume API. Do not give the provider token to untrusted users.

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
|`ephemeralDiskMB`|`KARAKORAM_NOMAD_EPHEMERAL_DISK_MB`|Optional integer MiB floor, range 1–65536 (64 GiB maximum), for new member deployments; cannot lower component defaults; subsequent intended Deploy adopts a changed floor|

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

**mTLS.** Prefer HTTPS with Nomad HTTP TLS and ACLs enabled. Karakoram requires TLS 1.2 or newer. Mount CA/certificate/key files into the control-plane allocation and use their paths in the variables above. They are read when the provider factory is constructed, so restart the control plane after rotating them. The same TLS configuration applies to every Nomad target; namespace users cannot upload another client certificate or disable certificate verification. This is separate from Karakoram's own internal management mTLS.

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

   Wait until `karakoram-data` is ready. If using another namespace/name, edit the literal volume specification too. Volume and ACL files are **not HCL2 jobspecs**: job `-var`/`-var-file` settings do not substitute their values. The node holding this volume also holds [SQLite](https://www.sqlite.org/) and sealed internal PKI; never delete/recreate it as part of a binary update.

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

   The job renders only `KARAKORAM_*` items into the environment file; TLS PEM values are rendered **only as files under `secrets/`**, readable by uid/gid 10001, never exported into the process environment. These PEM item names belong to the example job, while the provider uses `KARAKORAM_NOMAD_CACERT`/`CLIENT_CERT`/`CLIENT_KEY` file paths. All six items are required by this mTLS example. Use a trusted secret-storage workflow for this file and remove temporary plaintext copies after loading it.

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

Nomad does not reserve the stopped control plane's CPU and memory. Because its allocation is pinned to the volume's node, a placement that is waiting for room there (for example a member resize that could not be placed) can take the freed capacity while the control plane is stopped, and the updated control-plane job then stays pending with no control plane to reconcile anything. Before stopping, confirm `nomad eval list -namespace=karakoram -status=blocked` is empty and no member operation is waiting for placement, and that the volume's node will keep at least the control-plane job's CPU and memory free. If the control plane is left pending, free that node's capacity deliberately (for example stop the member job that claimed it, without `-purge`); the pending control-plane allocation is then placed.

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

**Public TCP.** `portmap` forwards a reserved host port only on the node running the allocation. Configure the target's public address as a stable load-balancer address, and route each advertised public port to that member's **actual node and host port**. Sending it to an arbitrary node fails; there is no Swarm-style routing mesh. Update routes when replacing a member. Karakoram does not configure or reconcile this external load balancer. Apply client source filtering at the LB and restrict **every published node port to only the LB/proxy sources** in the node's forwarding firewall; provider allowlists are not enforced. Also protect the configured dynamic host-port range against accidental publication. For public IPv6, provide a working IPv6 listener and suitable forwarding to the prepared allocation network; merely setting `publicIPv6` does not configure networking.

For PostgreSQL proxy replica reductions, configure both bounded TCP checks and
connect-failure retry/redispatch, without closing established sessions on DOWN.
The agent pauses a healthy retiring proxy's listeners, allows a 10-second LB
detection window and drains existing sessions for at most 25 seconds total
before native shutdown. No Nomad `shutdown_delay` is necessary: a delay before
SIGTERM alone would leave static TCP checks healthy until the eventual stop.
See the [PostgreSQL LB contract and HAProxy example](postgres/access-and-tls.md#planned-proxy-drain-and-external-load-balancers).


**HTTPS, SSE and WebSocket.** Terminate public HTTPS for the control plane at your reverse proxy and forward to its HTTP listener on port **8080**. UI live updates use **SSE** at `/api/v1/events/stream`; preserve streaming responses without buffering. The relay carrier uses **WebSocket** at `/relay/v1/connect`; preserve its upgrade and bidirectional binary traffic. Both passed through an operator [HAProxy](https://www.haproxy.org/) in testing. The control-plane private TLS listener on **9443** must be reachable from default-namespace members over the routed network but must not be exposed as the browser/API listener. Set `KARAKORAM_PUBLIC_URL` to the external HTTPS origin and `KARAKORAM_ADVERTISE_URL` to the private member-reachable TLS address. Member public HTTPS remains unsupported by the provider even if you manually operate a separate proxy.

## Operations and limitations

### Noninteractive exec sessions and stalled stops

Use closed stdin, no TTY, and a bounded observer timeout for administrative
commands, for example:

```sh
timeout 60s nomad alloc exec -i=false -t=false -namespace=databases -task=member ALLOCATION COMMAND
```

Never leave an administrative `nomad alloc exec` session open. On the tested
[Nomad](https://developer.hashicorp.com/nomad) 2.0.7 / [runc](https://github.com/opencontainers/runc) 1.5.1 combination, a failed streaming exec start retained the
container mutex while `os/exec.Wait` waited for its stdin-copy goroutine. Task
shutdown then blocked in `Container.Status` **before** sending its termination
signal or starting the kill timeout. This is an upstream exec-stream lifecycle
failure, not an engine drain barrier; increasing the task timeout cannot release
that lock. Closing the CLI session is the first diagnostic step, but an already
orphaned server-side stream may remain blocked even after the CLI exits.

Karakoram reports `BackendStopStalled` with the exact allocation/node and an
open-exec hint after a fresh `Killing` event exceeds that allocation's original
task kill timeout plus 15 seconds. The condition and a deduplicated warning event
also appear during deletion, and clear after fresh inventory confirms recovery.
Elapsed time is **not** fencing proof: Karakoram neither purges the allocation
nor releases volume/single-writer or all-live-proxy barriers. If closing sessions
does not release the stop, inspect the Nomad client/executor goroutines and plan
platform recovery under your normal operator change procedure. In our tests a
Nomad agent restart did not release it and the wedged executor ignored `SIGQUIT`;
sending `SIGTERM` to the task's own processes and then `SIGKILL` to that
allocation's executor process only let the stop complete without touching other
allocations. Do this only after confirming the stalled allocation is the one being
stopped.

### Lifecycle and provider limits

- **Imports reserve identity before creating resources.** A Variables CAS claim records each service's incarnation, original import-batch fingerprint and pinned volume name/node before any volume or environment creation. Accepted-but-lost create/save replies are reconciled by exact record/environment read-back and reserved physical-volume identity, not destructive rollback. An incomplete import remains Creating and retains its partial service IDs; retry the original batch/specification to resume it. Changing that specification, deploying or deleting an unresolved reservation is refused with an actionable conflict. The logical service ID is independent of Nomad's physical host-volume ID; never derive one from the other. Completed imports still expose ordinary contract statuses, and explicit deletion removes their records/resources.
- **Volume health is observed, not inferred from metadata.** Volume GetService/ListServices validate physical ID/name/namespace/node and inspect the original node. Missing/mismatched storage reports Failed with restore/explicit-replacement guidance; absent, down, draining and scheduling-ineligible nodes have distinct diagnostics; a non-ready host volume is not Active. Runtime mutations cannot recreate a missing/mismatched/non-ready original volume. This does not detect missing files inside an otherwise ready physical volume.
- **Lost lifecycle replies do not strand persisted operations.** Ordinary service/container observations resume any active persisted operation, not only public-port changes, using the same CAS lease and delivery quarantine. A caller can recover through observation after losing the returned operation handle; failed historical operations are not automatically reissued.
- **Confirmed external job stops are recoverable.** An owned Nomad job with `Stop=true`, desired-running state, and completed/no pending lifecycle work is reported Stopped so the engine can call Start with the saved deployment. Held lifecycle/restart claims, restoration fences and failed/pending operations are not mistaken for an external stop. Unknown allocation state or an empty allocation list without a confirmed job stop is not reported Stopped.
- **Provider retry deadlines remain observable.** HTTP 429/503 responses with a valid future Retry-After retain their uncapped retry deadline in the neutral error alongside the unavailable classification. Nomad Variable errors continue to hide secret Items while preserving revoked-token authentication classification.
- **Node loss pins recovery to the original node.** A volume-bearing member is never automatically rebuilt on a different node: sticky single-node-single-writer storage pins replacements to its volume's node. Members and the supplied SQLite control-plane job enable unlimited failed-allocation rescheduling with exponential delays from **30 s to 1 h**, while `disconnect` retains `lost_after = "12h"`, `replace = false`, `reconcile = "keep_original"`. This is not an unconditional recovery guarantee: [Nomad still applies the deployment progress deadline](https://developer.hashicorp.com/nomad/docs/job-specification/reschedule#unlimited), configured here as **20 minutes**; a never-healthy deployment can exhaust that window and need Start/Restart. Once a deployment is healthy, later task failures can retry without an attempt limit. The volume's lifetime `flock` serializes old/replacement writers. Wait for the node to recover, or explicitly replace the member through Karakoram to rebuild elsewhere from healthy peers/backups. If no allocation is running despite desired running state, **Restart** recreates the saved job with stop → terminal → register; **Start** can likewise recover a dead or absent job. Restart of a running allocation still uses Nomad's allocation restart endpoint. Recovery requires a healthy node/rootfs/CNI and available capacity.
- **Single-writer fencing is required.** Sticky `single-node-single-writer` claims alone did not prevent old and new allocations overlapping after node-loss reconciliation in our tests. The entrypoint holds an exclusive `flock` on the member volume across preparation and the final execution; a contender waits rather than writing. Do not remove the lock file or bypass the entrypoint while a writer may exist.
- **Agent death and host reboot are different recovery boundaries.** A failed member task gets two in-allocation retries within 30 minutes, then Nomad reschedules it on the same volume's node under the policy above. The lifetime volume lock remains mandatory. New process-launch receipts record the kernel boot identity, so an interrupted launch cannot strand startup after a proven host reboot; task/agent restarts within the same boot do not clear ambiguous ownership. Older ambiguous receipts without boot evidence remain fail-closed and require reconciliation.
- **PostgreSQL retirement can fence a reachable but stopped member.** If generic agent status explicitly reports Patroni stopped or failed with no owned PID, replacement/scale-in can use the provider runtime-stop path instead of waiting for that member to stream. This still requires a healthy surviving primary with linearizable DCS health, the exact owned runtime/volume identity, completed stop acknowledgement and terminal runtime observation before membership or storage removal. A transient demotion or an engine RPC error alone is not stopped evidence.
- **Replacement needs headroom.** A replacement is added before its predecessor is removed. Placement ignores the replaced volume when spreading the new one, so if every eligible node is already used, the replacement may share its predecessor's node temporarily. Reserve CPU, memory, disk and host-port headroom for both, or add a node/free capacity first.
- **Volume-bearing changes retain the destructive fence.** Deployment, resource and public-port changes on volume members use explicit stop → wait for terminal allocations → register. Stop/Delete use that fence for every runtime. Allow the full 120 s shutdown timeout plus scheduling and preparation time; the lifetime writer lock remains required.
- **Destructive resource updates are planned before stopping.** CPU/RAM changes to volume-pinned members and singleton relays first submit the proposed job to Nomad's job-plan API. A plan with failed task-group placements rejects the update with the pinned node and exhausted dimensions/filtered constraints, terminates the update operation, and leaves the running allocation and applied backend resources unchanged. Scheduler/API failures also prevent the stop. Convergence may recheck the desired resources, but cannot stop a member while its plan is infeasible. Free capacity or submit smaller bounds to proceed. Planning accounts for replaced allocations but does not reserve capacity: unrelated scheduling between plan and registration can still race the update. Stateless canary rollouts retain their existing placement behavior.
  The plan uses the new resource operation's exact replacement task specification, including its operation marker. A no-op or in-place-only plan is not replacement-placement proof and is refused; the pinned node must also be ready, scheduling-eligible and undrained. Engines that take a database offline before calling the backend can use the same read-only preflight before their own disruption boundary.
  An engine's explicit preflight also covers volume-less runtimes it stops itself, such as MySQL proxies: it evaluates cold replacement, not overlapping canary capacity. Ordinary stateless resource updates that do not explicitly stop the predecessor keep their normal canary behavior.
  A definitive resource-placement rejection is not stored as a platform retry of the captured resources: a corrective PATCH can converge from the last applied bounds. Initial deployments proven never placed are different: corrected bounds may still replace their blocked job while the node is ineligible, under the existing allocation-history fence, because there is no serving allocation to preserve.
  PostgreSQL (including Citus) also releases a resize receipt when polling returns a durable placement rejection. It keeps the last applied member bounds rather than letting that rejected handle block a subsequent corrective generation.
- **Stateless changes roll without stopping the job.** Deployments, resources, counts and public-port changes register over the exact owned job using Nomad index enforcement. Only the single canary is start-first: it auto-promotes after passing checks and takes one predecessor's place. Nomad then replaces each remaining predecessor serially (`max_parallel = 1`) with a destructive update, stopping it before starting its replacement. Multi-instance components temporarily serve with one fewer instance per step (never zero when count ≥ 2); readiness reports the live deficit until convergence. TCP checks on declared application listeners (not the agent port) must pass for **10 s**; they do not restart tasks or remove discovery during bootstrap. For a runtime without application TCP ports, the healthy interval covers task state only. These listener checks are not SQL/REST correctness probes: engine application-readiness and every-live-proxy configuration/drain barriers remain authoritative. A singleton's old allocation is retained until its canary passes; connections to an allocation eventually retired can still need reconnecting.
  Supabase's native gateway additionally retries only pre-processing transport
  failures (at most two retries, preferring another host) and passively ejects
  locally failing upstreams, covering replacement-before-listen and stale retired
  discovery records when a healthy reachable alternative exists. HTTP 5xx and
  resets after request headers were sent are not replayed; in-flight requests
  on a killed member can still fail and websocket sessions may need reconnecting.
  The 50% ejection cap retains singleton reachability and avoids [Envoy](https://www.envoyproxy.io/)'s default
  panic threshold; it is not application readiness or a guarantee against retry
  exhaustion. See [gateway rollout limits](supabase/operations.md).
- **Count-only changes are the canary exception.** With the exact current specification healthy at the existing count (increase) or desired retained count (reduction), registration preserves serving Meta and uses `Canary=0`, `AutoPromote=false`. Increases retain serving allocation identities and schedule only additional instances; reductions wait for surplus termination without new placement. This also repairs an already-running scale-back blocked under the former policy. A different serving image/configuration/resource/port specification never acknowledges an unready replacement.
- **Relays are singleton, not stateless application rollouts.** Namespace relay updates stop the predecessor and wait for terminal allocations before registering their replacement. They never receive an overlapping canary, including on volume-less relay jobs; overlapping relay sessions can evict one another.
- **Stateless rollouts require spare placement capacity.** Reserve CPU/memory and, for an already published static port, a free slot on another eligible node for the canary. Operators needing full-count serving capacity during rollout must size the configured count to accommodate the temporary one-instance deficit. Canary placement blockage leaves old allocations serving and reports a capacity/host-port diagnostic; do not free capacity by stopping the job. The application healthy deadline is **10 minutes**, deployment progress deadline **12 minutes**, and failed deployments auto-revert to the previous stable job when one exists. Failed/reverted operations report an actionable failure rather than succeeding because an old allocation still runs. The provider's **15 minute** startup window bounds a rollout that cannot place even its first canary. Correct capacity/readiness and retry. An operator-managed load balancer must select healthy providers across allocation relocation; Karakoram does not update its routes.
- **Purged running jobs are derived-object repairs.** GetService/ListServices restore an absent deployed job from its durable Variables record, preserving the service ID, deployment, environment and public ports. Repair waits for completed lifecycle operations and a **5 second registration grace**, uses Variables CAS plus absent-only job registration, and never restores desired-stopped jobs. Existing jobs must match the record ID/kind, namespace and owner marker; mismatches fail explicitly rather than being adopted. A data member is registered only while its original host-volume ID/name/node still match and the volume is ready; missing or changed storage reports **FAILED** for engine replacement, never silently creates a new volume. Stateful jobs explicitly constrain placement to the volume's recorded node.
- **Restoration cleanup belongs to the superseding lifecycle operation.** Repair persists its registration claim with Variables CAS before submitting an absent-only job, with a **10 second submission deadline**. Stop, Delete and replacement/retirement keep that durable claim and cannot finalize their terminal stop/purge phase until **60 seconds after the claim**. Delete retains both service and environment Variables during this fence; Stop retains its desired-stopped record and reports in progress rather than stopped. After the fence, operation polling re-observes the hostname, refuses foreign ownership, stops any owned job that landed late (including a restore-tagged job), and waits for terminal allocations before purging/completing. The obligation survives lost responses, caller cancellation and control-plane/provider reconstruction; failed cleanup is retried from the retained operation record. The restorer never issues a compensating stop or purge, so it cannot delete a successor.
- **Bounded-delivery assumption:** a restoration request must be delivered and committed by Nomad within the 60 second claim window, including server work that continues after the 10 second client deadline. The deadline starts before the claim is saved, leaving margin for server commit latency; clocks across control-plane processes must be synchronized. This is not a fence against an arbitrarily delayed server commit or an external writer deliberately recreating jobs after cleanup.
- **Multi-instance lifecycle exclusion.** Before stopping, purging, registering or advancing a lifecycle operation, each poll CAS-acquires a persisted lease with a random provider-instance owner and a **120 second expiry**. The complete mutation attempt has a **10 second deadline**, and no destructive request starts without at least that much lease remaining. Attempts track whether an issued mutation (including Variables CAS/phase writes) has an unacknowledged outcome. Fully acknowledged attempts, read-only failures and definitive **4xx mutation rejections** release only the exact acquired lease from fresh durable state, without persisting unsaved failed-attempt changes. Only **unknown mutation outcomes** (transport failures, deadlines/cancellation and 5xx responses to writes) retain delivery quarantine. Another attempt (including the original instance after an uncertain mutation) waits until lease expiry **plus a 60 second bounded-delivery margin** before taking over. New lifecycle operations honor the same exclusion. Ownership is checked before the initial stop; stops re-register the exact observed jobspec with `Stop=true` and an enforced job modify-index, so a changed job is not stopped. Purge requires an observed stopped job, terminal allocations and the lease; a stopping-phase poll never stops/purges its own operation's registered successor. Older records without leases remain readable and acquire one on their next mutation. These guarantees require synchronized clocks and delivery/commit of in-flight requests within the stated margin; arbitrary external writers are not serialized by a Karakoram lease.
- **Running-allocation Restart uses the same exclusion.** It acquires a durable service-level restart claim before selecting allocations, refuses held/quarantined lifecycle claims, and bounds every allocation restart request by the attempt deadline and remaining lease. It still restarts existing allocations without replacing their job or the caller's pending operation, including older deployed records without a pending handle. Successful or definitively rejected writes and read-only failures release the claim; an unknown restart-write outcome retains exclusion until safe takeover.
- Public-port methods return once the route is saved and its rolling stateless change (or fenced volume-member change) is initiated; they do not wait for preparation. Service/container observations and operation polling advance the persisted operation, including after a control-plane restart. Stateless service status stays **CREATING** through native canary promotion and serial deployment completion; a running task or healthy predecessor alone is not rollout readiness. PostgreSQL additionally requires fresh configuration and writer-health acknowledgement from every live proxy before acknowledging the exposure generation. A later lifecycle change uses the saved ports and supersedes the pending port operation.
- **Placement blockage is pending, not terminal failure.** Current scheduler rejections expose per-member `Placement/<member>` conditions and warning events with reason `BackendPlacementFailed`, including the pinned node and Nomad's exhausted resource dimensions or filtered constraints. These are read-only evaluation observations, not placement overrides. While placement remains blocked, the existing registration stays pending without spending deployment retries or expiring its startup window. Once current-version allocations are placed, historical evaluation failures no longer count as current placement failures; the condition clears and the runtime receives a fresh **15 minute unblocked startup window**. Native deployment health, application readiness and single-writer fences still apply. Waiting for an old allocation to become terminal is not bounded by the startup window: a disconnected node can hold the stop fence pending. Deadline expiry or failed tasks can fail an unblocked replacement's operation, but **do not stop/deregister that job**; Nomad rescheduling remains active and stateless deployments may auto-revert. A matching successful native deployment proves stateless completion even after evaluation garbage collection; otherwise its evaluation must report complete and its desired allocation count must be running. A reverted job or unhealthy canary is never completion proof. Volume-member observation retains its current-version allocation rule. A subsequent Start, Restart, Deploy or changed public-port request supersedes a historical failure using the normal rollout policy. Retrying an unchanged **failed port operation** also retries it; an idempotent port call does not restart a member solely because an unrelated historical operation failed.

- **Corrected stateless resources replace blocked rollouts.** A corrected Supabase
  component resource spec replaces a placement-blocked stateless resource rollout,
  including a revert to the last successful bounds. The exact owned blocked job
  must have created no allocations for its current version; older serving
  allocations remain in place. Once the new version has allocated, even if it is
  still awaiting health, reconciliation waits for that deployment's normal outcome
  rather than resubmitting. Volume-bearing members, singleton relays, restore
  operations and count-only update rules are unchanged.
  If the control plane loses its checkpoint after a correction is accepted, the
  provider recovers that exact predecessor-and-target receipt without submitting
  another rollout; unrelated or different-target successors are not adopted.
- **Placement diagnostics are member-scoped and cannot revive a failed rollout.** Only exact member identities from the engine's current cluster snapshot are probed; overlapping cluster-name prefixes are not ownership boundaries. Stateless operation polling checks the exact job version/operation identity and native failed/cancelled deployment outcomes before waiting on placement. Automatic reversion or another terminal native outcome ends the operation even if an old evaluation still contains placement failures; only genuinely pending placement suspends the startup deadline.
- **Delete supersedes obsolete deployment readiness.** All engine cleanup paths on Nomad can delete a never-placed or never-started runtime without waiting for its deployment startup window. Import reservations must still resolve to exact owned identities. Deletion keeps the provider's CAS leases, delayed-registration quarantine, stop acknowledgement and terminal-allocation checks; it does not weaken the writer fence or purge a live allocation early. Delete progress describes resource removal rather than a superseded create attempt.
- **Shrinking supersedes never-placed scale-up members.** If the desired count is reduced while a new member's original deployment is blocked, Sync retires that undesired identity before polling or retrying its obsolete deployment. This infrastructure-only path requires engine driver evidence that the member never joined and positive Nomad proof: no current or terminal allocations, an exact owned job/operation, and its original still-blocked evaluation chain. Missing/garbage-collected history or a previously initialized member never authorizes this shortcut. A persisted retirement journal submits exact-ID runtime deletion before service observations can restore the obsolete job, then removes the volume only after runtime absence. Normal provider leases, delivery quarantine and terminal-allocation fences remain; PostgreSQL also removes an already-added etcd learner through a surviving voter. Imports and replacement/maintenance ownership are not discarded. Disabled Supabase components similarly replace their obsolete deployment handle with exact-ID deletion. Whole-cluster cleanup takes over remaining work and preserves unissued volume deletion when suspending with `keepVolumes`.
- **Rootfs copies cost disk and time.** In our tests, new-allocation preparation used approximately **25 s / 333 MiB for RabbitMQ**, **100 s / 1.6 GiB for PSMDB + PBM**, and **95 s / 1.4 GiB for Elasticsearch**. These figures include installed packages/caches, exclude scheduling/artifact/template delays, and are not performance promises. A new allocation repeats preparation; a task restart in the same allocation can reuse its rootfs and prepare marker. Monitor garbage collection and transient extraction space as well as persistent data.
- **Disk requests are not enforced.** The built-in `mkdir` plugin returns no capacity quota; minimum/maximum sizes are metadata, not reservations. Disk growth and volume quotas are false. A custom capacity-enforcing plugin would need a separately supported lifecycle and does not enable these capabilities today.
- **Logs have no per-line timestamps.** Nomad returns plain stdout/stderr. Karakoram labels lines with the task's latest start time; `since` excludes only runs with a known finish time before that bound, not ongoing runs or individual messages. Unreadable terminal or disconnected-allocation logs are skipped, and oversized tails are truncated. Log retention is bounded by Nomad rotation/garbage collection.
- Environment and secret edits are stored immediately but apply at the next restart (`envChangeRestarts: false`).
- `keepVolumes` retains the independent Variables record and host volume; resuming uses the original node. Do not delete Variables/job/volume objects manually while Karakoram owns them. Durable operation handles track evaluations/allocations and survive control-plane restarts.
- **RabbitMQ suspension/deletion can stall on broker drain.** A three-broker quorum-queue test removed the first runtime, then remained `Deleting` because the next broker's drain repeatedly returned HTTP 503. The failure occurred in the engine's provider-neutral serial cleanup, before deleting the next Nomad runtime; a causal relationship to disappearing deleted-peer DNS was not established. Automatic suspension required an operator recovery preserving all volumes. Resume retained the same volume identities and restored all verified messages. A later full delete also needed recovery and its operation exhausted retries even though the final resource inventory was empty. Preserve storage and investigate broker/quorum health if this occurs; do not treat an accepted delete as completed suspension or bypass the writer fence. Check both operation outcome and resource inventory.
- Invalid or foreign service records do not break ordinary listings/imports. Volume deletion fails with a conflict while an undecodable record could hide a runtime's volume reference; repair such manually modified records before deleting storage.
- Namespace-scoped ACL tokens do not prevent the metadata leaks described above; native allocation exec/stats availability does not imply Karakoram `exec`/`metrics` support.
- Elasticsearch's S3 client trusts only public CAs; use a publicly trusted S3 endpoint for snapshots. For operator-approved private/nonstandard-port S3 endpoints, configure `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` ([Backups and storage](backups.md)).

## Nomad licence

Nomad Community Edition has used **BUSL 1.1 since Nomad 1.7**. Karakoram does not include or redistribute Nomad. Check [HashiCorp's licence terms](https://www.hashicorp.com/en/license-faq) for your use; this guide is not legal advice.
