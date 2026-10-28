<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operating costs

**The figures below are rough estimates; check the providers’ current pricing before ordering. Prices collected on 26 October 2026.** Karakoram is Apache-2.0 software; these are infrastructure estimates, not a Karakoram subscription, quote, performance promise or availability guarantee.

Karakoram is multi-platform: **[Zerops](https://zerops.io/)** is the reference provider; **[Dokploy](https://dokploy.com)** and **[Nomad](https://developer.hashicorp.com/nomad)** run on infrastructure you supply. The same VM budget below applies to both providers; the cloud engine drivers are hosting examples, not additional Karakoram providers. See [Providers](providers.md), [clusters](clusters.md) and [limitations](limitations.md).

Karakoram itself is **free and open source**. These are the infrastructure costs of running clusters on each provider/engine driver, not a price Karakoram charges.

## Assumptions

- Use **730 hours/month**, on-demand [Linux](https://www.kernel.org/)/amd64, no reservations, savings plans, credits or free trials. [Hetzner](https://www.hetzner.com/) and [DigitalOcean](https://www.digitalocean.com/) monthly caps apply; Zerops' published 30-day rates are multiplied by 730/720. Regions: **Nuremberg/Helsinki for Hetzner compute and European object storage; Frankfurt (eu-central-1) for [AWS](https://aws.amazon.com/); FRA1 for DigitalOcean; Gravelines (GRA), France, for [OVHcloud](https://www.ovhcloud.com/) Public Cloud**.
- Native currencies: **USD for Zerops, AWS and DigitalOcean; EUR for Hetzner and OVHcloud**. No currency conversion. Totals are rounded to whole currency units, or one decimal below 10; unit rates retain precision. VAT/taxes excluded.
- Egress, cross-zone traffic, object requests, paid support, load balancers, NAT gateways, replacement/upgrade overlap and retained/orphaned resources are excluded. Private networking/IPv6 is assumed where necessary. Hetzner's selected public price includes IPv4; AWS public IPv4 is not included. Do not expose internal management listeners.
- Each engine table is an **entire standalone installation**: engine resources + one Karakoram control plane + one provider server host for Dokploy or Nomad + backup storage. Zerops includes a free Lightweight core for MINIMAL, or one Serious core for MINIMAL HA / PRODUCTION-SHAPED. Share overhead across engines in the same installation instead of paying it once per engine.
- Backup occupancy per cluster is **20 GB/GiB MINIMAL, 100 GB/GiB MINIMAL HA, 500 GB/GiB PRODUCTION-SHAPED**. This is retained physical object occupancy, not logical database size, a retention policy or a compression assumption. Size WAL/oplog history, full/incremental copies and restore staging separately.
- CPU/RAM/data disk are stated per member as **cores / GiB / GB** (provider storage billing units differ). Every VM has spare RAM above the engine reservation; VM quantities include the OS/scheduler budget. Small shared/burstable VMs are not performance-equivalent to dedicated cores.
- MINIMAL and MINIMAL HA use explicit admission floors, not necessarily the larger presets. Where validation supplies no positive numeric RAM floor, use the created default and say so. Explicitly set minimum and maximum CPU/RAM/data-disk bounds to the listed size; otherwise omitted maxima can allocate more storage/resources.
- MINIMAL HA means an engine quorum topology, **not end-to-end service HA**. [PostgreSQL](https://www.postgresql.org/)'s minimum proxy and [Supabase](https://supabase.com/)'s minimum API services remain singletons. For Dokploy and Nomad, place each voting/data member on a distinct worker VM; auxiliary replicas also have separate VMs here. Zerops **cannot guarantee host/rack/zone anti-affinity**, so its HA-shaped topology is not a failure-domain guarantee. See [limitations](limitations.md).

## How we calculated

[Zerops](https://zerops.io/) defaults to **SHARED** CPU in the engine drivers and the control-plane import. Resource cost is the sum over runtimes of CPU + RAM + ephemeral disk, plus persistent volume capacity. Object storage and project core are separate. The estimates budget the upper ephemeral-disk bound: 3 GB per [MongoDB](https://www.mongodb.com/)/[RabbitMQ](https://www.rabbitmq.com/)/[Elasticsearch](https://www.elastic.co/elasticsearch)/[OpenBao](https://openbao.org/)/[Weaviate](https://weaviate.io/) member. PostgreSQL/Supabase/[MySQL](https://www.mysql.com)/[PXC](https://docs.percona.com/percona-xtradb-cluster/8.4/) and [Neo4j](https://neo4j.com/) currently pass data-component disk bounds to both the runtime and persistent volume, so both are counted. Zerops Local Storage has a 5-GB minimum even when engine admission permits 1 GB. Auxiliary services use their 1-GB runtime-disk default. No extra CPU/RAM charge is invented for a Local Storage volume.

VM estimates are **host count × VM price + storage + object storage + shared control overhead**. AWS adds a 20-GiB gp3 OS disk to every VM and a separate data disk to each data member; DigitalOcean includes its OS disk and adds Volumes for data. Hetzner uses included local SSD capacity for node-pinned persistent host directories, not network Volumes: 40 GB on CX23 or 160 GB on CX43 accommodates the listed data plus OS budget. This is local, non-replicated member storage; engine replication and separate backups remain necessary. An optional Hetzner network-Volume variant is **not priced** because its current unit rate was not recoverable from the official public page; do not interpret that as free block storage.

OVHcloud uses **hourly Discovery d2-2 / d2-4 and General Purpose b2-15 instances**, not the separate monthly-billing offers. Their included 25 / 50 / 100-GB local disks cover the OS and auxiliary runtime disks. Add **High Speed Gen2 Block Storage** for each data member and Karakoram state: its [10-GB minimum volume](https://docs.ovhcloud.com/en/guides/storage-and-backup/block-storage/block-storage-the-right-storage-class) means a 5-GB data/state requirement is billed as 10 GB; production data volumes are 100 GB each. Backups use **S3-compatible One Zone Standard Object Storage**, not SWIFT, snapshots or instance backups. GRA is a single-zone hosting estimate, not zone-redundant HA; backup objects in that same region are not an off-site disaster-recovery copy.

### Unit prices

AWS regional machine-readable catalogs supplement its dynamically rendered pricing pages. OVHcloud's euro prices below are from its official Ireland price list; its [regional availability matrix](https://www.ovhcloud.com/en-ie/public-cloud/regions-availability/) lists Discovery, General Purpose, High Speed Gen2 and Standard Object Storage in Gravelines.

| Provider | Unit used | Published price / basis | Official source |
| --- | --- | --- | --- |
| **Zerops** | Shared / dedicated CPU | $0.60 / $6 per core per 720 h; $0.0008333 / $0.0083333 per core-hour | [Pricing](https://zerops.io/pricing), [pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | RAM / disk | $3 per GB per 720 h ($0.0041667/GB-hour); disk $0.10/GB per 720 h | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | Project core | Lightweight free; Serious $10/720 h; one per project, not per member | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | S3-compatible object storage | $0.01/GB per 720 h | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| Zerops | Automatic volume-backup overage (not used below) | $0.50 per extra 5 GB; Lightweight includes 5 GB, Serious 25 GB | [Pricing documentation](https://docs.zerops.io/company/pricing) |
| **Hetzner** | CX23 | 2 vCPU / 4 GB / 40 GB local SSD, €5.99 monthly cap, €0.0096/h on the current product page | [Cost-optimized cloud](https://www.hetzner.com/cloud/cost-optimized/) |
| Hetzner | CX43 | 8 vCPU / 16 GB / 160 GB local SSD, €16.49 monthly cap, €0.0264/h | [Cost-optimized cloud](https://www.hetzner.com/cloud/cost-optimized/) |
| Hetzner | Object Storage | €6.49 monthly cap (€0.0104/h), includes 1 TB; excess storage €0.0087/TB-hour | [Object Storage pricing](https://www.hetzner.com/storage/object-storage/pricing/) |
| **AWS** | EC2 t3.small / t3.medium | 2 vCPU / 2 GiB: $0.024/h; 2 vCPU / 4 GiB: $0.048/h | [EC2 on-demand](https://aws.amazon.com/ec2/pricing/on-demand/), [official Frankfurt Linux price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20%28Frankfurt%29/Linux/index.json) |
| AWS | EC2 m6i.xlarge | 4 vCPU / 16 GiB: $0.230/h | [Official Frankfurt Linux price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20%28Frankfurt%29/Linux/index.json) |
| AWS | EBS gp3 | $0.0952/GiB-month, 3,000 IOPS / 125 MiB/s baseline | [EBS pricing](https://aws.amazon.com/ebs/pricing/), [official EBS price data](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json) |
| AWS | S3 Standard | $0.0245/GB-month, first 50 TB | [S3 pricing](https://aws.amazon.com/s3/pricing/), [official regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-central-1/index.json) |
| **DigitalOcean** | Basic Regular Droplets | 1 vCPU / 1 GiB / 25 GiB: $6/month; 1 vCPU / 2 GiB / 50 GiB: $12; 2 vCPU / 4 GiB / 80 GiB: $24; 8 vCPU / 16 GiB / 320 GiB: $96 | [Droplet pricing](https://www.digitalocean.com/pricing/droplets) |
| DigitalOcean | Volumes | $0.10/GiB-month | [Volume pricing](https://docs.digitalocean.com/products/volumes/details/pricing/) |
| DigitalOcean | Spaces | $5/month includes 250 GiB; excess $0.02/GiB-month | [Spaces pricing](https://www.digitalocean.com/pricing/spaces-object-storage) |
| **OVHcloud** | Discovery d2-2 / d2-4 | 1 vCPU / 2 GB / 25-GB local disk: €0.0104/h; 2 vCPU / 4 GB / 50-GB local disk: €0.0206/h | [Public Cloud prices: Discovery](https://www.ovhcloud.com/en-ie/public-cloud/prices/) |
| OVHcloud | General Purpose b2-15 | 4 vCPU / 15 GB / 100-GB local SSD: €0.1342/h | [Public Cloud prices: General Purpose](https://www.ovhcloud.com/en-ie/public-cloud/prices/) |
| OVHcloud | High Speed Gen2 Block Storage | €0.000119/GB-hour = €0.08687/GB per 730 h; minimum 10 GB per volume | [Public Cloud prices: Block Storage](https://www.ovhcloud.com/en-ie/public-cloud/prices/), [storage classes](https://docs.ovhcloud.com/en/guides/storage-and-backup/block-storage/block-storage-the-right-storage-class) |
| OVHcloud | One Zone Standard Object Storage | €0.00000972/GiB-hour = €0.0070956/GiB per 730 h; no minimum retention charge | [Public Cloud prices: Object Storage — One Zone](https://www.ovhcloud.com/en-ie/public-cloud/prices/) |

**Price/availability qualifications:** Hetzner's live product page marks CX23/CX43 unavailable at the check date. These are published-price budgets, not capacity reservations. Its [June 2026 price adjustment](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/) lists €5.49 / €15.99 excluding IPv4; the current selected product page lists €5.99 / €16.49, which we use. T3 CPU-credit surcharges are excluded; AWS [publishes $0.05/vCPU-hour](https://aws.amazon.com/ec2/instance-types/t3/) for sustained T3 Unlimited surplus usage. Production data members use non-burstable m6i.xlarge instead.

OVHcloud Discovery instances use shared resources and are advertised for test/development/sandbox use; production data members instead use b2-15. Published RAM is in GB, not GiB: d2-2 still leaves RAM above the largest 1.5-GiB floor, while b2-15 leaves RAM above the 8-GiB production reservation. Standard-region instance prices include IPv4. The published d2/b2 monthly offers are a different billing choice, not a cap on these hourly estimates.

### Shared overhead and backup storage

Control-plane sizing follows the shipped import: one shared core, RAM 0.5–1 GiB, runtime disk 1–2 GB, plus the **5-GB `karakoramdata` Local Storage**. Budget its upper bounds, $4.4/month on Zerops. There is **one control-plane replica, no control-plane HA**; preserve a consistent state backup and master key. See [installation](install.md), [control-plane recovery](operations.md#control-plane-state-backup-and-recovery) and [limitations](limitations.md).

Host allowances below are monthly, before backup objects and any Zerops project core:

| Provider | Karakoram + state | Dokploy/Nomad server |
| --- | --- | --- |
| **Zerops (USD)** | $4.4: managed runtime + 5-GB state | Not applicable |
| Hetzner (EUR) | €6.0: 1 × CX23, local state | €6.0: 1 × CX23 |
| AWS (USD) | $20: t3.small + 20-GiB root + 5-GiB state | $37: t3.medium + 20-GiB root |
| DigitalOcean (USD) | $13: Basic 2-GiB + 5-GiB Volume | $24: Basic 4-GiB |
| OVHcloud (EUR) | €8.5: d2-2 + 10-GB state volume | €15: d2-4 |

Backup occupancy is 20 / 100 / 500 GB or GiB respectively:

| Backup objects/month | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $0.2 | $1.0 | $5.1 |
| Hetzner (EUR) | €6.5 | €6.5 | €6.5 |
| AWS (USD) | $0.5 | $2.5 | $12 |
| DigitalOcean (USD) | $5.0 | $5.0 | $10 |
| OVHcloud (EUR) | €0.1 | €0.7 | €3.5 |

Zerops adds **$0 / $10 / $10** for the project core in Minimal / Minimal HA / Production-shaped. The VM providers have no extra project fee in these estimates. Calculations use unrounded rates: for example, OVHcloud shared host overhead is `(0.0104 + 0.0206 + 10 × 0.000119) × 730 = €23.4987/month`, plus engine workers, their data volumes and backup occupancy.

The small provider server host is a **budget allowance**, not a product admission minimum or an HA scheduler design. It is separate from every worker and from Karakoram. A redundant Nomad server quorum or larger Dokploy server needs extra hosts. Hetzner/Spaces account-level object-storage minimums can be shared across clusters; the tables conservatively charge each standalone installation the full minimum. Zerops' automatic volume backups are not the engine-native S3 backups priced here. [Backups and storage](backups.md) explains coverage and consistency.

## MongoDB

Use a replica set, **not a sharded cluster**, for these tiers. Admission permits 1/3/5/7 data members, at least one whole CPU, 0.5 GiB RAM and 5 GB persistent disk. Defaults/presets create 1 GiB RAM, not the 0.5-GiB admission floor. [PBM](https://docs.percona.com/percona-backup-mongodb/) runs with each data member; it is not an extra VM. These tiny floors are for disposable workloads, not recovery/load guarantees. See [MongoDB](mongodb/README.md) and [replica sets](mongodb/replica-sets.md).

| Tier sizing | Members × CPU / RAM / persistent disk |
| --- | --- |
| MINIMAL | 1 × 1 / 0.5 / 5; no redundancy |
| MINIMAL HA | 3 × 1 / 0.5 / 5; three voting data members |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; voting members on separate hosts; S3 backups |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 managed member | 3 managed members | 3 managed members |
| Hetzner | 1 × CX23 | 3 × CX23 | 3 × CX43 |
| AWS | 1 × t3.small | 3 × t3.small | 3 × m6i.xlarge |
| DigitalOcean | 1 × Basic 1-GiB | 3 × Basic 1-GiB | 3 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-2 | 3 × b2-15 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $7.5 | $24 | $131 |
| Hetzner (EUR) | €24 | €36 | €68 |
| AWS (USD) | $77 | $119 | $607 |
| DigitalOcean (USD) | $48 | $61 | $365 |
| OVHcloud (EUR) | €32 | €50 | €347 |

## PostgreSQL

Admission requires 1 member for `single`, or 3/5 for `ha`; data members require 1 CPU, 1 GiB RAM and 5 GB disk. The created RAM default is 2 GiB. **[Patroni](https://patroni.readthedocs.io/) and [etcd](https://etcd.io/) are co-located on each HA data member**: three data VMs already include all three etcd voters. HA requires at least one separate [ProxySQL](https://proxysql.com/) replica. For `single`, explicitly disabling the default proxy is admitted and gives the strict minimum. ProxySQL has no explicit numeric positive RAM floor beyond common validation, so use its 1 CPU / 1 GiB / 1-GB runtime default. See [PostgreSQL](postgres/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 × 1 / 1 / 5; ProxySQL explicitly disabled |
| MINIMAL HA | 3 × 1 / 1 / 5 with Patroni + etcd; 1 × ProxySQL at 1 / 1 / 1 runtime disk |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100 with Patroni + etcd; 2 × ProxySQL at 1 / 1 / 1 runtime disk; S3 backups |

Production worker cells distinguish data members from proxies:

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 managed data member | 3 data + 1 proxy runtime | 3 data + 2 proxy runtimes |
| Hetzner | 1 × CX23 | 4 × CX23 | 3 × CX43 + 2 × CX23 |
| AWS | 1 × t3.small | 4 × t3.small | 3 × m6i.xlarge + 2 × t3.small |
| DigitalOcean | 1 × Basic 2-GiB | 4 × Basic 2-GiB | 3 × Basic 16-GiB + 2 × Basic 2-GiB |
| OVHcloud | 1 × d2-2 | 4 × d2-2 | 3 × b2-15 + 2 × d2-2 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $9.2 | $33 | $168 |
| Hetzner (EUR) | €24 | €42 | €80 |
| AWS (USD) | $77 | $138 | $646 |
| DigitalOcean (USD) | $54 | $91 | $389 |
| OVHcloud (EUR) | €32 | €57 | €362 |

One proxy is a singleton client endpoint despite the HA database core. Two proxies require an application/provider routing design; no unpriced external load balancer is implied. Review [ProxySQL compatibility](postgres/access-and-tls.md#proxy-compatibility) and [pgBackRest](postgres/backups.md).

## Supabase

[Supabase](https://supabase.com/) inherits [PostgreSQL](https://www.postgresql.org/)'s data/topology floors. **The strict admitted minimum is database-only**: every optional application component can be explicitly disabled, as can the proxy with `single`. Its MINIMAL and MINIMAL HA costs are therefore exactly the corresponding provider cells in the PostgreSQL monthly-total table above. That is not a functioning Supabase API stack.

The table below makes the requested **database plus services** comparison explicit: add gateway, Auth and [PostgREST](https://postgrest.org/) to those strict minima. They are **default-enabled, not mandatory admission dependencies**. Each has 1 CPU / 1 GiB RAM / 1-GB runtime disk by default; common validation requires positive RAM but defines no fixed positive numeric RAM floor, so the created default is used. Omitted application entries enable these three; explicit zero disables them. The functional MINIMAL row explicitly disables the default proxy, as the single core permits.

Realtime, Storage, [imgproxy](https://imgproxy.net/), metadata, Studio, functions, pooler, analytics and vector are optional and **excluded** from these prices. imgproxy requires Storage; Studio requires metadata + gateway; vector requires analytics. Storage requires registered S3 configuration; its application objects are additional to backup occupancy. Analytics has a 1-GiB admission floor; Realtime permits only one enabled replica. See [Supabase](supabase/README.md) and [components](supabase/components.md).

| Tier sizing | Database and selected application services |
| --- | --- |
| MINIMAL (functional API) | 1 × data at 1 / 1 / 5; 1 each gateway/Auth/REST at 1 / 1 / 1 runtime disk; proxy disabled |
| MINIMAL HA (functional API) | 3 × data at 1 / 1 / 5 with co-located Patroni/etcd; 1 proxy + 1 each gateway/Auth/REST at 1 / 1 / 1 runtime disk |
| PRODUCTION-SHAPED | 3 × data at 4 / 8 / 100 with Patroni/etcd; 2 proxies + 2 each gateway/Auth/REST at 1 / 1 / 1 runtime disk; S3 backups |

Production worker cells distinguish data members from proxies/API services:

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 data + 3 API runtimes | 3 data + 4 service runtimes | 3 data + 8 service runtimes |
| Hetzner | 4 × CX23 | 7 × CX23 | 3 × CX43 + 8 × CX23 |
| AWS | 4 × t3.small | 7 × t3.small | 3 × m6i.xlarge + 8 × t3.small |
| DigitalOcean | 4 × Basic 2-GiB | 7 × Basic 2-GiB | 3 × Basic 16-GiB + 8 × Basic 2-GiB |
| OVHcloud | 4 × d2-2 | 7 × d2-2 | 3 × b2-15 + 8 × d2-2 |

Minimal and Minimal HA below include the functional API services:

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $20 | $45 | $191 |
| Hetzner (EUR) | €42 | €60 | €116 |
| AWS (USD) | $136 | $197 | $762 |
| DigitalOcean (USD) | $90 | $127 | $461 |
| OVHcloud (EUR) | €55 | €80 | €408 |

HA here describes the PostgreSQL core; singleton API services are not an HA application guarantee. Production replicas still require appropriate ingress/routing and sufficient independent workers. Database backups do not protect separate Storage objects, function source or control-plane-held secrets; see [Supabase operations](supabase/operations.md).

## RabbitMQ

Admission allows 1/3/5/7 brokers, at least 1 whole CPU, 0.5 GiB RAM and 5 GB data disk. The created RAM default is 1 GiB. Three brokers allow quorum queues with three replicas; classic-queue messages are not made redundant merely by adding brokers. Backups cover **definitions only, not messages**; the object-storage allowance below is a budget, not a message-backup feature. See [RabbitMQ](rabbitmq/README.md).

| Tier sizing | Brokers × CPU / RAM / persistent disk |
| --- | --- |
| MINIMAL | 1 × 1 / 0.5 / 5; no queue redundancy |
| MINIMAL HA | 3 × 1 / 0.5 / 5; use replicated quorum queues |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; separate hosts; quorum queues; definitions to S3 |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 managed broker | 3 managed brokers | 3 managed brokers |
| Hetzner | 1 × CX23 | 3 × CX23 | 3 × CX43 |
| AWS | 1 × t3.small | 3 × t3.small | 3 × m6i.xlarge |
| DigitalOcean | 1 × Basic 1-GiB | 3 × Basic 1-GiB | 3 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-2 | 3 × b2-15 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $7.5 | $24 | $131 |
| Hetzner (EUR) | €24 | €36 | €68 |
| AWS (USD) | $77 | $119 | $607 |
| DigitalOcean (USD) | $48 | $61 | $365 |
| OVHcloud (EUR) | €32 | €50 | €347 |

## Elasticsearch

Combined master/data nodes admit 1–9 members, at least 1 whole CPU, 1.5 GiB RAM and 5 GB persistent disk. Their resource default is 2 GiB RAM and 5–20 GB disk; these floor estimates explicitly pin disk to 5 GB. Three combined nodes supply the smallest useful majority-based redundant topology; configure shard replicas as well. **Dedicated masters are optional**, not three missing VMs in these estimates. The alternative dedicated-master topology requires 3 or 5 masters in addition to data nodes. [Kibana](https://www.elastic.co/kibana), [Logstash](https://www.elastic.co/logstash) and APM are optional and disabled in every row below. See [Elasticsearch](elasticsearch/README.md).

| Tier sizing | Combined nodes × CPU / RAM / persistent disk |
| --- | --- |
| MINIMAL | 1 × 1 / 1.5 / 5; no redundant shard copy |
| MINIMAL HA | 3 × 1 / 1.5 / 5; three master-eligible data nodes; shard replicas required |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; separate hosts; shard replicas and S3 snapshots |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 managed node | 3 managed nodes | 3 managed nodes |
| Hetzner | 1 × CX23 | 3 × CX23 | 3 × CX43 |
| AWS | 1 × t3.small | 3 × t3.small | 3 × m6i.xlarge |
| DigitalOcean | 1 × Basic 2-GiB | 3 × Basic 2-GiB | 3 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-2 | 3 × b2-15 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $11 | $33 | $131 |
| Hetzner (EUR) | €24 | €36 | €68 |
| AWS (USD) | $77 | $119 | $607 |
| DigitalOcean (USD) | $54 | $79 | $365 |
| OVHcloud (EUR) | €32 | €50 | €347 |

Plan space for shard recovery/rebalance and watermarks, not just indexed data. Optional Kibana 9.x has a 1.5-GiB active RAM floor; Logstash and dedicated masters have 1-GiB floors, APM 0.25 GiB. None is needed for the Elasticsearch-only topology priced here. Review [snapshots](elasticsearch/snapshots.md), [Stack components](elasticsearch/stack.md) and Elastic's licensing restrictions in the [engine guide](elasticsearch/README.md).


## Additional v0.4.0 engines

These engines and the [Citus](https://www.citusdata.com/) distributed topology are new in v0.4.0; their release status is listed once in [engine availability](providers.md#engine-availability). The private, shared-CPU configurations below use the common runtime contract on Zerops, Dokploy and Nomad, except [Temporal](https://temporal.io/) on Zerops as explained below; provider support is not deployment acceptance.

[PostGIS](https://postgis.net/), [pgvector](https://github.com/pgvector/pgvector) and [TimescaleDB](https://www.timescale.com/) profiles cost the same as the corresponding PostgreSQL tier above; no extra runtime is required, but workload-driven CPU/RAM/disk growth is additional.

### OpenBao

Use integrated **Raft storage** and the default Shamir seal: no PostgreSQL source or paid KMS/transit service is included. Admission allows 1/3/5 members at 1 CPU / 0.5 GiB / 5 GB persistent disk; created RAM defaults to 1 GiB. Each member has a separate 1–3-GB runtime disk. PostgreSQL storage is an opt-in alternative requiring a bound source and a different recovery budget, not the Raft installation priced here. See [OpenBao](openbao/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 × 1 / 0.5 / 5; Raft singleton |
| MINIMAL HA | 3 × 1 / 0.5 / 5; three Raft voters |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; Raft snapshots to S3 |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 × managed runtime | 3 × managed runtime | 3 × managed runtime |
| Hetzner | 1 × CX23 | 3 × CX23 | 3 × CX43 |
| AWS | 1 × t3.small | 3 × t3.small | 3 × m6i.xlarge |
| DigitalOcean | 1 × Basic 1-GiB | 3 × Basic 1-GiB | 3 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-2 | 3 × b2-15 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $7.5 | $24 | $131 |
| Hetzner (EUR) | €24 | €36 | €68 |
| AWS (USD) | $77 | $119 | $607 |
| DigitalOcean (USD) | $48 | $61 | $365 |
| OVHcloud (EUR) | €32 | €50 | €347 |

### Temporal

The strict minimum is one **combined** server with UI explicitly disabled. Combined servers run frontend/history/matching/internal worker together; split topology instead requires one of each role. Common component validation requires 1 whole CPU, positive RAM and 1-GB runtime disk but no fixed positive RAM floor: use the created **1-GiB RAM default**. Replicated combined servers supply redundant service instances, not a separate consensus quorum. See [Temporal](temporal/README.md).

A bound PostgreSQL source is mandatory. Each cell includes one additional source at the **PostgreSQL MINIMAL** sizing (1 / 1 / 5, proxy disabled), sharing this installation’s control plane/provider server; backup occupancy is charged once on that source (20/100/500). This remains a singleton persistence bottleneck even in MINIMAL HA; upgrade the source to PostgreSQL HA separately for durable service HA. UI, application workers and external ingress are excluded. **Zerops is not supported** in this release: Temporal members need a private IPv4 route to their PostgreSQL source, while Zerops private service networking is IPv6. Dokploy and Nomad provide the required private IPv4 networking; managed public HTTPS is disabled.

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 combined at 1 / 1 / 1 runtime; + source 1 / 1 / 5 |
| MINIMAL HA | 2 combined at 1 / 1 / 1 runtime; + singleton source 1 / 1 / 5 |
| PRODUCTION-SHAPED | 3 combined at 4 / 8 / 1 runtime; + singleton source 1 / 1 / 5 |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | not supported | not supported | not supported |
| Hetzner | 1 × CX23 + 1 × CX23 | 2 × CX23 + 1 × CX23 | 3 × CX43 + 1 × CX23 |
| AWS | 1 × t3.small + 1 × t3.small | 2 × t3.small + 1 × t3.small | 3 × m6i.xlarge + 1 × t3.small |
| DigitalOcean | 1 × Basic 2-GiB + 1 × Basic 2-GiB | 2 × Basic 2-GiB + 1 × Basic 2-GiB | 3 × Basic 16-GiB + 1 × Basic 2-GiB |
| OVHcloud | 1 × d2-2 + 1 × d2-2 | 2 × d2-2 + 1 × d2-2 | 3 × b2-15 + 1 × d2-2 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | not supported | not supported | not supported |
| Hetzner (EUR) | €30 | €36 | €74 |
| AWS (USD) | $97 | $118 | $598 |
| DigitalOcean (USD) | $66 | $78 | $347 |
| OVHcloud (EUR) | €40 | €48 | €329 |

### Percona Server for MySQL

Admission requires 1 / 1 / 5 per `single` data member and **1 / 2 / 5 per Group Replication member** (RAM default 2 GiB). `single` permits one member and explicitly disabling [ProxySQL](https://proxysql.com/); `group` requires 3 or 5 data members and at least one ProxySQL replica. The minimum GR3 deployment therefore reserves 7 GiB including its default proxy. Corrected agents bound the GR message cache to one eighth of minimum RAM (128 MiB floor), leaving 37.5% outside the default pool and message cache; this is not a workload-RSS guarantee. ProxySQL has no fixed positive numeric RAM floor beyond common validation, so use its 1 / 1 / 1 runtime default. Group Replication is single-primary; [XtraBackup](https://docs.percona.com/percona-xtrabackup/8.4/) runs with data members, not on extra hosts. See [MySQL](mysql/configuration.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 × 1 / 1 / 5; proxy disabled |
| MINIMAL HA | 3 × 1 / 2 / 5; 1 proxy at 1 / 1 / 1 runtime |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; 2 proxies at 1 / 1 / 1 runtime; S3 backups |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 × managed runtime | 3 × managed runtime + 1 × managed runtime | 3 × managed runtime + 2 × managed runtime |
| Hetzner | 1 × CX23 | 3 × CX23 + 1 × CX23 | 3 × CX43 + 2 × CX23 |
| AWS | 1 × t3.small | 3 × t3.medium + 1 × t3.small | 3 × m6i.xlarge + 2 × t3.small |
| DigitalOcean | 1 × Basic 2-GiB | 3 × Basic 4-GiB + 1 × Basic 2-GiB | 3 × Basic 16-GiB + 2 × Basic 2-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-4 + 1 × d2-2 | 3 × b2-15 + 2 × d2-2 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $9.2 | $42 | $168 |
| Hetzner (EUR) | €24 | €42 | €80 |
| AWS (USD) | $77 | $191 | $646 |
| DigitalOcean (USD) | $54 | $127 | $389 |
| OVHcloud (EUR) | €32 | €79 | €362 |

### Percona XtraDB Cluster (PXC)

[PXC](https://docs.percona.com/percona-xtradb-cluster/8.4/) has **no singleton topology**: its admitted minimum is 3 [Galera](https://galeracluster.com/) members plus 1 required [ProxySQL](https://proxysql.com/) replica; 5 data members are also admitted. Data floors are 1 / 2 / 5 (2-GiB data RAM default); proxy resources retain the 1 / 1 / 1 default. Like MySQL Group Replication, PXC rejects 1-GiB data members; Galera and SST add memory overhead beyond singleton MySQL. MINIMAL and MINIMAL HA therefore share workers but differ in backup occupancy and Zerops core allowance. Proxies route one writer; two proxy replicas need an operator routing design. See [PXC](mysql/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 3 × 1 / 2 / 5; 1 proxy at 1 / 1 / 1 runtime |
| MINIMAL HA | 3 × 1 / 2 / 5; 1 proxy at 1 / 1 / 1 runtime |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; 2 proxies at 1 / 1 / 1 runtime; S3 backups |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 3 × managed runtime + 1 × managed runtime | 3 × managed runtime + 1 × managed runtime | 3 × managed runtime + 2 × managed runtime |
| Hetzner | 3 × CX23 + 1 × CX23 | 3 × CX23 + 1 × CX23 | 3 × CX43 + 2 × CX23 |
| AWS | 3 × t3.medium + 1 × t3.small | 3 × t3.medium + 1 × t3.small | 3 × m6i.xlarge + 2 × t3.small |
| DigitalOcean | 3 × Basic 4-GiB + 1 × Basic 2-GiB | 3 × Basic 4-GiB + 1 × Basic 2-GiB | 3 × Basic 16-GiB + 2 × Basic 2-GiB |
| OVHcloud | 3 × d2-4 + 1 × d2-2 | 3 × d2-4 + 1 × d2-2 | 3 × b2-15 + 2 × d2-2 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $31 | $42 | $168 |
| Hetzner (EUR) | €42 | €42 | €80 |
| AWS (USD) | $189 | $191 | $646 |
| DigitalOcean (USD) | $127 | $127 | $389 |
| OVHcloud (EUR) | €79 | €79 | €362 |

### Neo4j Community

**Singleton only: no HA tier.** Admission requires exactly one member, 1 whole CPU / 1 GiB RAM / 1 GB persistent disk; created defaults are 1 / 2 / 10. Zerops bills its 5-GB minimum volume plus the 1-GB runtime disk at the admission floor. Production remains one larger member. Offline dumps interrupt the database; enable scheduled-backup downtime consent when scheduling them. See [Neo4j](neo4j/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 × 1 / 1 / 1 persistent disk |
| PRODUCTION-SHAPED | 1 × 4 / 8 / 100; offline dumps to S3 |

| Worker placement | Minimal | Production-shaped |
| --- | --- | --- |
| **Zerops** | 1 × managed runtime | 1 × managed runtime |
| Hetzner | 1 × CX23 | 1 × CX43 |
| AWS | 1 × t3.small | 1 × m6i.xlarge |
| DigitalOcean | 1 × Basic 2-GiB | 1 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 1 × b2-15 |

| Monthly total | Minimal | Production-shaped |
| --- | ---: | ---: |
| **Zerops (USD)** | $8.8 | $67 |
| Hetzner (EUR) | €24 | €35 |
| AWS (USD) | $77 | $248 |
| DigitalOcean (USD) | $54 | $153 |
| OVHcloud (EUR) | €32 | €134 |

### Weaviate

Admission permits one member for `single`, or 3–9 for `cluster`, at 1 whole CPU / 1 GiB RAM / 1 GB persistent disk. Created defaults are 1 / 2 / 10. The cluster has three fixed Raft metadata voters; configure collection replication as well (cluster replication minimum defaults to 3). Each member has a separate 1–3-GB runtime disk; Zerops bills at least 5 GB per volume. Inference services/API usage and application objects beyond the listed disks are excluded. See [Weaviate](weaviate/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 × 1 / 1 / 1; no redundancy |
| MINIMAL HA | 3 × 1 / 1 / 1; three voters and replicated collections |
| PRODUCTION-SHAPED | 3 × 4 / 8 / 100; replicated collections and S3 full backups |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 1 × managed runtime | 3 × managed runtime | 3 × managed runtime |
| Hetzner | 1 × CX23 | 3 × CX23 | 3 × CX43 |
| AWS | 1 × t3.small | 3 × t3.small | 3 × m6i.xlarge |
| DigitalOcean | 1 × Basic 2-GiB | 3 × Basic 2-GiB | 3 × Basic 16-GiB |
| OVHcloud | 1 × d2-2 | 3 × d2-2 | 3 × b2-15 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $9.0 | $29 | $131 |
| Hetzner (EUR) | €24 | €36 | €68 |
| AWS (USD) | $77 | $118 | $607 |
| DigitalOcean (USD) | $54 | $78 | $365 |
| OVHcloud (EUR) | €32 | €50 | €347 |

### Keycloak

**Singleton only: no HA tier** in the implemented engine driver. Admission requires 1 CPU / 1.5 GiB RAM / 1-GB disposable runtime disk; pin maxima explicitly rather than the created 2-CPU / 2-GiB / 3-GB upper bounds. A bound [PostgreSQL](https://www.postgresql.org/) source is mandatory: each cell includes PostgreSQL MINIMAL (1 / 1 / 5, proxy disabled), sharing control-plane/provider-server overhead, with the 20/500 backup occupancy charged once on that source. Production-shaped enlarges the [Keycloak](https://www.keycloak.org/) runtime, not the singleton source; size/upgrade the source separately for workload and recovery needs. Backups protect the source rather than the disposable runtime. Private ingress only is priced, so Nomad’s lack of managed public HTTPS does not exclude it; external public ingress costs remain excluded. See [Keycloak](keycloak/README.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 runtime at 1 / 1.5 / 1; + source 1 / 1 / 5 |
| PRODUCTION-SHAPED | 1 runtime at 4 / 8 / 1; + singleton source 1 / 1 / 5 |

| Worker placement | Minimal | Production-shaped |
| --- | --- | --- |
| **Zerops** | 1 × managed runtime + 1 × managed runtime | 1 × managed runtime + 1 × managed runtime |
| Hetzner | 1 × CX23 + 1 × CX23 | 1 × CX43 + 1 × CX23 |
| AWS | 1 × t3.small + 1 × t3.small | 1 × m6i.xlarge + 1 × t3.small |
| DigitalOcean | 1 × Basic 2-GiB + 1 × Basic 2-GiB | 1 × Basic 16-GiB + 1 × Basic 2-GiB |
| OVHcloud | 1 × d2-2 + 1 × d2-2 | 1 × b2-15 + 1 × d2-2 |

| Monthly total | Minimal | Production-shaped |
| --- | ---: | ---: |
| **Zerops (USD)** | $14 | $51 |
| Hetzner (EUR) | €30 | €41 |
| AWS (USD) | $97 | $259 |
| DigitalOcean (USD) | $66 | $155 |
| OVHcloud (EUR) | €40 | €133 |

### PostgreSQL with Citus distributed topology

Each coordinator/worker **group** admits 1 or 3 data members, each at PostgreSQL’s 1 / 1 / 5 floor (created RAM default 2 GiB). At least **one worker group plus one coordinator group** is required; worker-group default is 2, maximum 8. Replicated groups run Patroni; coordinator members co-locate the shared etcd quorum and workers are its clients. Replicated groups require at least one shared coordinator ProxySQL replica; an unreplicated distributed minimum may explicitly disable it. No extra proxy per worker group is required. Production below chooses two worker groups. The 20/100/500 backup occupancy is for the **whole distributed cluster**, not per group; provision enough for every group and coordinated restore points. See [Citus](postgres/distributed.md).

| Tier sizing | Members × CPU / RAM / persistent disk; auxiliary runtimes |
| --- | --- |
| MINIMAL | 1 coordinator + 1 worker, each 1 × 1 / 1 / 5; proxy disabled |
| MINIMAL HA | 1 coordinator + 1 worker, each 3 × 1 / 1 / 5; 1 proxy at 1 / 1 / 1 runtime |
| PRODUCTION-SHAPED | 1 coordinator + 2 workers, each 3 × 4 / 8 / 100; 2 proxies at 1 / 1 / 1 runtime; S3 backups |

| Worker placement | Minimal | Minimal HA | Production-shaped |
| --- | --- | --- | --- |
| **Zerops** | 2 × managed runtime | 6 × managed runtime + 1 × managed runtime | 9 × managed runtime + 2 × managed runtime |
| Hetzner | 2 × CX23 | 6 × CX23 + 1 × CX23 | 9 × CX43 + 2 × CX23 |
| AWS | 2 × t3.small | 6 × t3.small + 1 × t3.small | 9 × m6i.xlarge + 2 × t3.small |
| DigitalOcean | 2 × Basic 2-GiB | 6 × Basic 2-GiB + 1 × Basic 2-GiB | 9 × Basic 16-GiB + 2 × Basic 2-GiB |
| OVHcloud | 2 × d2-2 | 6 × d2-2 + 1 × d2-2 | 9 × b2-15 + 2 × d2-2 |

| Monthly total | Minimal | Minimal HA | Production-shaped |
| --- | ---: | ---: | ---: |
| **Zerops (USD)** | $14 | $47 | $450 |
| Hetzner (EUR) | €30 | €60 | €179 |
| AWS (USD) | $97 | $198 | $1722 |
| DigitalOcean (USD) | $67 | $129 | $1025 |
| OVHcloud (EUR) | €41 | €83 | €1002 |
