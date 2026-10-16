// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package zerops implements Karakoram's Zerops REST infrastructure backend.
//
// ImportServices omits inline pipelines: runtimes are READY_TO_DEPLOY until Deploy
// uploads a prebuilt gzip-tar artifact and supplies a complete zerops document.
// ServiceSpec Kind/Base values select platform types; Env is imported as
// envSecrets. Deploy renders the neutral RuntimeSpec into zerops.yaml, including
// non-secret variables, readiness checks and volume mounts.
//
// SetResources preserves fetched effective autoscaling values when a bound is
// zero. Growing persistent storage requires the volume's hostname and max disk;
// runtime disk is ephemeral. SetContainerCount preserves the full vertical
// configuration. Process completion is not application or container readiness.
// Free-resource thresholds retain their original nullable custom overrides:
// computed defaults such as 0.0625 GB are returned by GET but rejected by PUT's
// three-decimal-place input validation. They must not be rounded or frozen.
//
// SetEnvSecrets waits for API writes but does not restart containers; providers
// must roll restarts safely to apply environment changes to existing processes.
// Public routing changes are staged until project synchronization; routing methods
// wait for both its process and observable synchronized route state.
// ListPublicPorts includes every project service and staged/deleting routes, so
// allocation cannot reuse another controller's port. GetService includes only
// Karakoram identity and data-directory environment keys in non-serialized
// ownership evidence; providers verify it before adopting legacy resources.
// Start/Stop control every container in a service, including replicated mongos.
// A deleted service may return HTTP 400 with serviceStackNotFound, not HTTP 404;
// both invalidate cached service IDs and resolve the absent hostname as nil.
//
// SubdomainAccess is an optional infra capability, separate from public TCP routes.
// Enable/disable PUTs return durable process IDs and are never replayed; matching
// state returns a completed no-op. SubdomainURLs includes only enabled HTTP ports
// (run.ports.httpSupport is returned as ports[].scheme="http") and derives
// https://<service>-<zeropsSubdomainHost>-<port>.<nestId>.zerops.app from service and
// project GETs. Zerops's shared L7 balancer terminates TLS and forwards HTTP.
//
// The client retries only explicitly idempotent GET/PUT requests on network errors,
// HTTP 429 and HTTP 5xx. Deploy, upload, lifecycle and routing synchronization are
// not replayed because they create processes or stream non-repeatable data. No
// authorization, environment content or credential-bearing URLs are logged.
// Neutral errors expose authorization, conflicts, rejections and transient outages
// through errors.Is. NewFactory constructs a factory with the backend's
// capabilities; registration happens in the server composition root. The API URL
// is operator-only configuration supplied through the factory's base Options,
// never through per-target Config.
//
// Unit replay fixtures preserve anonymized Zerops API response shapes.
// The e2e-tagged live smoke test needs ZEROPS_TOKEN and ZEROPS_PROJECT_ID,
// creates uniquely named kkzt-prefixed services and verifies teardown by listing
// the project after deleting its runtime and volume.
package zerops
