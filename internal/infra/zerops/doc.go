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
// Receipt and environment writes get one early (at most 100ms) process poll,
// then use the configured cadence if still pending. Their usual ~50ms completion
// must not impose a full deployment polling interval per durable checkpoint.
// Every write still requires positive terminal success; deploy/admission cadence
// and retry budgets are unaffected.
// Public routing changes are staged until project synchronization; routing methods
// wait for both its process and observable synchronized route state.
// ListPublicPorts includes every project service and staged/deleting routes, so
// allocation cannot reuse another controller's port. GetService includes only
// Karakoram identity and data-directory environment keys in non-serialized
// ownership evidence; providers verify it before adopting legacy resources.
// Start/Stop control every container in a service, including replicated mongos.
// A deleted service may return HTTP 400 with serviceStackNotFound, not HTTP 404;
// both invalidate cached service IDs and resolve the absent hostname as nil.
// Missing app versions likewise return HTTP 400 appVersionNotFound or HTTP 404.
// This is resource absence, not successful deployment evidence. If the owning
// service remains, replacement requires the same bounded terminal process and
// app-version evidence as the previous-deployment gate; incomplete evidence waits
// without mutation.
// Exact-version process history may have aged out: a complete service inventory
// with no non-terminal work authorizes bounded replacement even when history is
// empty or contains only unrelated terminal work. A positively absent version
// cannot later be deployed; missing identity-matched history is not a permanent
// recovery fence. Unreadable or truncated inventories never grant authorization.
// Replacement consumes the shared initial-plus-three deployment
// retry budget, retained across selected versions. Exhaustion requires the normal
// explicit retry-epoch re-arm, which permits a fresh allocation through admission.
// If both are absent, the pending deployment finishes as failed so cluster
// cleanup can discard its retry without issuing platform mutations.
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
// Platform deployment admission is shared by all backends in this process for
// the same API endpoint and project, across namespaces. The default cap is two
// in-flight owned deploy/build processes plus submissions awaiting visibility.
// Set KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS to an integer >= 1 to override it;
// invalid values fail configuration. Foreign and protected services do not count.
// Readable ownership markers select service process inventories, shared through
// a ten-second per-project observation cache. Admission never scans project-wide
// process history. Each owned service inventory is bounded to five 100-item pages;
// unreadable/truncated evidence or bound exhaustion waits rather than permitting
// more mutations. Process age never excludes running work from the cap.
// Live submission reservations never expire. Once their caller returns, retained
// reservations expire after five minutes without process visibility. Unknown
// process states/actions count unless positively terminal or known unrelated.
// The previous-deployment gate uses the same five-page bound and reuses a fresh
// owned-service observation where available. Deployment waits expose stable
// service/process identities separately from age-bearing status diagnostics.
// Saturation reports "waiting for a platform deployment slot: N/LIMIT in flight"
// with service/status/age evidence and a retry delay of at least fifteen seconds.
// New version allocation/upload also waits for prior service deployment/build
// processes and non-terminal app versions, reporting "waiting for previous
// platform deployment to finish". Platform Retry-After delays are preserved.
//
// Unit replay fixtures preserve anonymized Zerops API response shapes.
// The e2e-tagged live smoke test needs ZEROPS_TOKEN and ZEROPS_PROJECT_ID,
// creates uniquely named kkzt-prefixed services and verifies teardown by listing
// the project after deleting its runtime and volume.
package zerops
