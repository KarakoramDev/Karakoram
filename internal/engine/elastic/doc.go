// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package elastic reconciles Elasticsearch nodes and their Kibana, Logstash and
// APM component services. Ownership is established by recorded service IDs,
// retained-volume IDs, identities returned by the member's own import, a
// service-bound non-sensitive KARAKORAM_OWNER marker, or readable legacy agent
// token secrets. Sensitive variables are masked for tokens without sudo mode;
// masked values and service hostnames alone never establish ownership.
//
// An unrecorded Local Storage service may also be verified by a runtime whose
// marker or legacy secret proves ownership and whose data directory names that
// exact volume mount. Markers are backfilled only after every existing service
// of the member is verified. Expired import processes are reconciled against
// actual services: missing services are imported again, while existing services
// still require ownership evidence.
package elastic
