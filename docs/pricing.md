<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Pricing and cost estimates

**Rough estimate — check provider pricing.** Karakoram is Apache-2.0 software; these are infrastructure estimates, not a Karakoram subscription, quote, performance promise or availability guarantee.

Karakoram is multi-platform: **Zerops** is the reference backend; **Dokploy (experimental)** and **Nomad (experimental)** run on infrastructure you supply. The same VM budget below applies to both experimental backends; the cloud providers are hosting examples, not additional Karakoram backends. See [Backends](backends.md), [clusters](clusters.md) and [limitations](limitations.md).

## Assumptions — rough estimate — check provider pricing

- Checked **2026-10-26**. Use **730 hours/month**, on-demand Linux/amd64, no reservations, savings plans, credits or free trials. Hetzner and DigitalOcean monthly caps apply; Zerops' published 30-day rates are multiplied by 730/720.
- Native currencies: **USD for Zerops, AWS and DigitalOcean; EUR for Hetzner**. No currency conversion. Totals are rounded to whole currency units, or one decimal below 10; unit rates retain precision. VAT/taxes excluded.
- Egress, cross-zone traffic, object requests, paid support, load balancers, NAT gateways, replacement/upgrade overlap and retained/orphaned resources are excluded. Private networking/IPv6 is assumed where necessary. Hetzner's selected public price includes IPv4; AWS public IPv4 is not included. Do not expose internal management listeners.
- Each engine table is an **entire standalone installation**: engine resources + one Karakoram control plane + one backend server host for the experimental backends + backup storage. Zerops includes a free Lightweight core for MINIMAL, or one Serious core for MINIMAL HA / PRODUCTION-SHAPED. Share overhead across engines in the same installation instead of paying it five times.
- Backup occupancy per cluster is **20 GB/GiB MINIMAL, 100 GB/GiB MINIMAL HA, 500 GB/GiB PRODUCTION-SHAPED**. This is retained physical object occupancy, not logical database size, a retention policy or a compression assumption. Size WAL/oplog history, full/incremental copies and restore staging separately.
- CPU/RAM/data disk are stated per member as **cores / GiB / GB** (provider storage billing units differ). Every VM has spare RAM above the engine reservation; VM quantities include the OS/scheduler budget. Small shared/burstable VMs are not performance-equivalent to dedicated cores.
- MINIMAL and MINIMAL HA use explicit admission floors, not necessarily the larger presets. Where validation supplies no positive numeric RAM floor, use the created default and say so. Explicitly set minimum and maximum CPU/RAM/data-disk bounds to the listed size; otherwise omitted maxima can allocate more storage/resources.
- MINIMAL HA means an engine quorum topology, **not end-to-end service HA**. PostgreSQL's minimum proxy and Supabase's minimum API services remain singletons. For experimental backends, place each voting/data member on a distinct worker VM; auxiliary replicas also have separate VMs here. Zerops **cannot guarantee host/rack/zone anti-affinity**, so its HA-shaped topology is not a failure-domain guarantee. See [limitations](limitations.md).

## How we calculated — rough estimate — check provider pricing

Zerops defaults to **SHARED** CPU in all five engine providers and the control-plane import. Resource cost is the sum over runtimes of CPU + RAM + ephemeral disk, plus persistent volume capacity. Object storage and project core are separate. The estimates budget the upper ephemeral-disk bound: 3 GB per MongoDB/RabbitMQ/Elasticsearch member. PostgreSQL/Supabase currently pass data-component disk bounds to both the runtime and persistent volume, so both are counted (5 + 5 GB at the floor; 100 + 100 GB in the production example). Auxiliary services use their 1-GB runtime-disk default. No extra CPU/RAM charge is invented for a Local Storage volume.

VM estimates are **host count × VM price + storage + object storage + shared control overhead**. AWS adds a 20-GiB gp3 OS disk to every VM and a separate data disk to each data member; DigitalOcean includes its OS disk and adds Volumes for data. Hetzner uses included local SSD capacity for node-pinned persistent host directories, not network Volumes: 40 GB on CX23 or 160 GB on CX43 accommodates the listed data plus OS budget. This is local, non-replicated member storage; engine replication and separate backups remain necessary. An optional Hetzner network-Volume variant is **not priced** because its current unit rate was not recoverable from the official public page; do not interpret that as free block storage.

### Unit prices — rough estimate — check provider pricing

Every source in this table was checked **2026-10-26**. AWS regional machine-readable catalogs supplement its dynamically rendered pricing pages.

| Provider | Unit used | Published price / basis | Official source |
| --- | --- | --- | --- |
| **Zerops** | Shared / dedicated CPU | $0.60 / $6 per core per 720 h; $0.0008333 / $0.0083333 per core-hour | [Pricing](https://zerops.io/pricing), [pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | RAM / disk | $3 per GB per 720 h ($0.0041667/GB-hour); disk $0.10/GB per 720 h | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | Project core | Lightweight free; Serious $10/720 h; one per project, not per member | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | S3-compatible object storage | $0.01/GB per 720 h | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | Automatic volume-backup overage (not used below) | $0.50 per extra 5 GB; Lightweight includes 5 GB, Serious 25 GB | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| **Hetzner** | CX23, Nuremberg/Helsinki selection | 2 vCPU / 4 GB / 40 GB local SSD, €5.99 monthly cap, €0.0096/h on the current product page | [Cost-optimized cloud](https://www.hetzner.com/cloud/cost-optimized/) |
| Hetzner | CX43, same selection | 8 vCPU / 16 GB / 160 GB local SSD, €16.49 monthly cap, €0.0264/h | [Cost-optimized cloud](https://www.hetzner.com/cloud/cost-optimized/) |
| Hetzner | Object Storage, European location | €6.49 monthly cap (€0.0104/h), includes 1 TB; excess storage €0.0087/TB-hour | [Object Storage pricing](https://www.hetzner.com/storage/object-storage/pricing/) |
| **AWS** | EC2 t3.small / t3.medium, eu-central-1 | 2 vCPU / 2 GiB: $0.024/h; 2 vCPU / 4 GiB: $0.048/h | [EC2 on-demand](https://aws.amazon.com/ec2/pricing/on-demand/), [official Frankfurt Linux price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20%28Frankfurt%29/Linux/index.json) |
| AWS | EC2 m6i.xlarge, eu-central-1 | 4 vCPU / 16 GiB: $0.230/h | [Official Frankfurt Linux price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20%28Frankfurt%29/Linux/index.json) |
| AWS | EBS gp3, eu-central-1 | $0.0952/GiB-month, 3,000 IOPS / 125 MiB/s baseline | [EBS pricing](https://aws.amazon.com/ebs/pricing/), [official EBS price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json) |
| AWS | S3 Standard, eu-central-1 | $0.0245/GB-month, first 50 TB | [S3 pricing](https://aws.amazon.com/s3/pricing/), [official regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-central-1/index.json) |
| **DigitalOcean** | FRA1 Basic Regular Droplets | 1 vCPU / 1 GiB / 25 GiB: $6/month; 1 vCPU / 2 GiB / 50 GiB: $12; 2 vCPU / 4 GiB / 80 GiB: $24; 8 vCPU / 16 GiB / 320 GiB: $96 | [Droplet pricing](https://www.digitalocean.com/pricing/droplets) |
| DigitalOcean | Volumes | $0.10/GiB-month | [Volume pricing](https://docs.digitalocean.com/products/volumes/details/pricing/) |
| DigitalOcean | Spaces | $5/month includes 250 GiB; excess $0.02/GiB-month | [Spaces pricing](https://www.digitalocean.com/pricing/spaces-object-storage) |

**Price/availability qualifications:** Hetzner's live product page marks CX23/CX43 unavailable at the check date. These are published-price budgets, not capacity reservations. Its [June 2026 price adjustment](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/) lists €5.49 / €15.99 excluding IPv4; the current selected product page lists €5.99 / €16.49, which we use. Recheck location, IPv4 selection and availability before ordering. T3 CPU-credit surcharges are excluded; AWS [publishes $0.05/vCPU-hour](https://aws.amazon.com/ec2/instance-types/t3/) for sustained T3 Unlimited surplus usage (checked 2026-10-26). Production data members use non-burstable m6i.xlarge instead.

### Shared overhead and backup storage — rough estimate — check provider pricing

Control-plane sizing follows the shipped import: one shared core, RAM 0.5–1 GiB, runtime disk 1–2 GB, plus the **5-GB `karakoramdata` Local Storage**. Budget its upper bounds, $4.4/month on Zerops. There is **one control-plane replica, no control-plane HA**; preserve a consistent state backup and master key. See [installation](install.md), [control-plane recovery](operations.md#control-plane-state-backup-and-recovery) and [limitations](limitations.md).

| Monthly item — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| Karakoram control plane + 5-GB state | $4.4 | €6.0: 1 × CX23, included local SSD | $20: 1 × t3.small + 20-GiB root + 5-GiB state | $13: 1 × Basic 2-GiB + 5-GiB Volume |
| Dokploy/Nomad server host (experimental; choose one backend) | Not applicable | €6.0: 1 × CX23 | $37: 1 × t3.medium + 20-GiB root | $24: 1 × Basic 4-GiB |
| Project core, MINIMAL | $0 | Included / no extra project fee used | No extra project fee used | No extra project fee used |
| Project core, HA / production | $10 | Same as above | Same as above | Same as above |
| Backup objects: 20 GB/GiB | $0.2 | €6.5 | $0.5 | $5.0 |
| Backup objects: 100 GB/GiB | $1.0 | €6.5 | $2.5 | $5.0 |
| Backup objects: 500 GB/GiB | $5.1 | €6.5 | $12 | $10 |

The small backend server host is a **budget allowance**, not a product admission minimum or an HA scheduler design. It is separate from every worker and from Karakoram. A redundant Nomad server quorum or larger Dokploy server needs extra hosts. Hetzner/Spaces account-level object-storage minimums can be shared across clusters; the tables conservatively charge each standalone installation the full minimum. Zerops' automatic volume backups are not the engine-native S3 backups priced here. [Backups and storage](backups.md) explains coverage and consistency.

## MongoDB — rough estimate — check provider pricing

Use a replica set, **not a sharded cluster**, for these tiers. Admission permits 1/3/5/7 data members, at least one whole CPU, 0.5 GiB RAM and 5 GB persistent disk. Defaults/presets create 1 GiB RAM, not the 0.5-GiB admission floor. PBM runs with each data member; it is not an extra VM. These tiny floors are for disposable workloads, not recovery/load guarantees. See [MongoDB](mongodb/README.md) and [replica sets](mongodb/replica-sets.md).

| Tier sizing — rough estimate — check provider pricing | Members × CPU / RAM / persistent disk | Worker VMs: Hetzner / AWS / DigitalOcean |
| --- | --- | --- |
| MINIMAL | 1 × 1 / 0.5 / 5; no redundancy | 1 × CX23 / t3.small / Basic 1-GiB |
| MINIMAL HA | 3 × 1 / 0.5 / 5; three voting data members | 3 × CX23 / t3.small / Basic 1-GiB |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; voting members on separate hosts; S3 backups | 3 × CX43 / m6i.xlarge / Basic 16-GiB |

| Monthly total — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| MINIMAL | $7.5 | €24 | $77 | $48 |
| MINIMAL HA | $24 | €36 | $119 | $61 |
| PRODUCTION-SHAPED | $131 | €68 | $607 | $365 |

## PostgreSQL — rough estimate — check provider pricing

Admission requires 1 member for `single`, or 3/5 for `ha`; data members require 1 CPU, 1 GiB RAM and 5 GB disk. The created RAM default is 2 GiB. **Patroni and etcd are co-located on each HA data member**: three data VMs already include all three etcd voters. HA requires at least one separate ProxySQL replica. For `single`, explicitly disabling the default proxy is admitted and gives the strict minimum. ProxySQL has no explicit numeric positive RAM floor beyond common validation, so use its 1 CPU / 1 GiB / 1-GB runtime default. See [PostgreSQL](postgres/README.md).

| Tier sizing — rough estimate — check provider pricing | Members × CPU / RAM / persistent disk; auxiliary runtimes | Worker VMs: Hetzner / AWS / DigitalOcean |
| --- | --- | --- |
| MINIMAL | 1 × 1 / 1 / 5; ProxySQL explicitly disabled | 1 × CX23 / t3.small / Basic 2-GiB |
| MINIMAL HA | 3 × 1 / 1 / 5 with Patroni + etcd; 1 × ProxySQL at 1 / 1 / 1 runtime disk | 4 × CX23 / t3.small / Basic 2-GiB |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100 with Patroni + etcd; 2 × ProxySQL at 1 / 1 / 1 runtime disk; S3 backups | Data: 3 × CX43 / m6i.xlarge / Basic 16-GiB; proxies: 2 × CX23 / t3.small / Basic 2-GiB |

| Monthly total — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| MINIMAL | $9.2 | €24 | $77 | $54 |
| MINIMAL HA | $33 | €42 | $138 | $91 |
| PRODUCTION-SHAPED | $168 | €80 | $646 | $389 |

One proxy is a singleton client endpoint despite the HA database core. Two proxies require an application/backend routing design; no unpriced external load balancer is implied. Review [ProxySQL compatibility](postgres/access-and-tls.md#proxy-compatibility) and [pgBackRest](postgres/backups.md).

## Supabase — rough estimate — check provider pricing

Supabase inherits PostgreSQL's data/topology floors. **The strict admitted minimum is database-only**: every optional application component can be explicitly disabled, as can the proxy with `single`. Its MINIMAL and MINIMAL HA costs are therefore the PostgreSQL rows above: respectively **$9.2 / €24 / $77 / $54** and **$33 / €42 / $138 / $91**, in Zerops / Hetzner / AWS / DigitalOcean order. That is not a functioning Supabase API stack.

The table below makes the requested **database plus services** comparison explicit: add gateway, Auth and PostgREST to those strict minima. They are **default-enabled, not mandatory admission dependencies**. Each has 1 CPU / 1 GiB RAM / 1-GB runtime disk by default; common validation requires positive RAM but defines no fixed positive numeric RAM floor, so the created default is used. Omitted application entries enable these three; explicit zero disables them. The functional MINIMAL row explicitly disables the default proxy, as the single core permits.

Realtime, Storage, imgproxy, metadata, Studio, functions, pooler, analytics and vector are optional and **excluded** from these prices. imgproxy requires Storage; Studio requires metadata + gateway; vector requires analytics. Storage requires registered S3 configuration; its application objects are additional to backup occupancy. Analytics has a 1-GiB admission floor; Realtime permits only one enabled replica. See [Supabase](supabase/README.md) and [components](supabase/components.md).

| Tier sizing — rough estimate — check provider pricing | Database and selected application services | Worker VMs: Hetzner / AWS / DigitalOcean |
| --- | --- | --- |
| MINIMAL (functional API) | 1 × data at 1 / 1 / 5; 1 each gateway/Auth/REST at 1 / 1 / 1 runtime disk; proxy disabled | 4 × CX23 / t3.small / Basic 2-GiB |
| MINIMAL HA (functional API) | 3 × data at 1 / 1 / 5 with co-located Patroni/etcd; 1 proxy + 1 each gateway/Auth/REST at 1 / 1 / 1 runtime disk | 7 × CX23 / t3.small / Basic 2-GiB |
| PRODUCTION-SHAPED | 3 × data at 4 / 8 / 100 with Patroni/etcd; 2 proxies + 2 each gateway/Auth/REST at 1 / 1 / 1 runtime disk; S3 backups | Data: 3 × CX43 / m6i.xlarge / Basic 16-GiB; services: 8 × CX23 / t3.small / Basic 2-GiB |

| Monthly total — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| MINIMAL (functional API) | $20 | €42 | $136 | $90 |
| MINIMAL HA (functional API) | $45 | €60 | $197 | $127 |
| PRODUCTION-SHAPED | $191 | €116 | $762 | $461 |

HA here describes the PostgreSQL core; singleton API services are not an HA application guarantee. Production replicas still require appropriate ingress/routing and sufficient independent workers. Database backups do not protect separate Storage objects, function source or control-plane-held secrets; see [Supabase operations](supabase/operations.md).

## RabbitMQ — rough estimate — check provider pricing

Admission allows 1/3/5/7 brokers, at least 1 whole CPU, 0.5 GiB RAM and 5 GB data disk. The created RAM default is 1 GiB. Three brokers allow quorum queues with three replicas; classic-queue messages are not made redundant merely by adding brokers. Backups cover **definitions only, not messages**; the object-storage allowance below is a budget, not a message-backup feature. See [RabbitMQ](rabbitmq/README.md).

| Tier sizing — rough estimate — check provider pricing | Brokers × CPU / RAM / persistent disk | Worker VMs: Hetzner / AWS / DigitalOcean |
| --- | --- | --- |
| MINIMAL | 1 × 1 / 0.5 / 5; no queue redundancy | 1 × CX23 / t3.small / Basic 1-GiB |
| MINIMAL HA | 3 × 1 / 0.5 / 5; use replicated quorum queues | 3 × CX23 / t3.small / Basic 1-GiB |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; separate hosts; quorum queues; definitions to S3 | 3 × CX43 / m6i.xlarge / Basic 16-GiB |

| Monthly total — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| MINIMAL | $7.5 | €24 | $77 | $48 |
| MINIMAL HA | $24 | €36 | $119 | $61 |
| PRODUCTION-SHAPED | $131 | €68 | $607 | $365 |

## Elasticsearch — rough estimate — check provider pricing

Combined master/data nodes admit 1–9 members, at least 1 whole CPU, 1.5 GiB RAM and 5 GB persistent disk. Their resource default is 2 GiB RAM and 5–20 GB disk; these floor estimates explicitly pin disk to 5 GB. Three combined nodes supply the smallest useful majority-based redundant topology; configure shard replicas as well. **Dedicated masters are optional**, not three missing VMs in these estimates. The alternative dedicated-master topology requires 3 or 5 masters in addition to data nodes. Kibana, Logstash and APM are optional and disabled in every row below. See [Elasticsearch](elasticsearch/README.md).

| Tier sizing — rough estimate — check provider pricing | Combined nodes × CPU / RAM / persistent disk | Worker VMs: Hetzner / AWS / DigitalOcean |
| --- | --- | --- |
| MINIMAL | 1 × 1 / 1.5 / 5; no redundant shard copy | 1 × CX23 / t3.small / Basic 2-GiB |
| MINIMAL HA | 3 × 1 / 1.5 / 5; three master-eligible data nodes; shard replicas required | 3 × CX23 / t3.small / Basic 2-GiB |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; separate hosts; shard replicas and S3 snapshots | 3 × CX43 / m6i.xlarge / Basic 16-GiB |

| Monthly total — rough estimate — check provider pricing | Zerops (USD) | Hetzner (EUR) | AWS (USD) | DigitalOcean (USD) |
| --- | ---: | ---: | ---: | ---: |
| MINIMAL | $11 | €24 | $77 | $54 |
| MINIMAL HA | $33 | €36 | $119 | $79 |
| PRODUCTION-SHAPED | $131 | €68 | $607 | $365 |

Plan space for shard recovery/rebalance and watermarks, not just indexed data. Optional Kibana 9.x has a 1.5-GiB active RAM floor; Logstash and dedicated masters have 1-GiB floors, APM 0.25 GiB. None is needed for the Elasticsearch-only topology priced here. Review [snapshots](elasticsearch/snapshots.md), [Stack components](elasticsearch/stack.md) and Elastic's licensing restrictions in the [engine guide](elasticsearch/README.md).
