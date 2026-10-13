// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package zerops implements Karakoram's Zerops REST infrastructure backend.
//
// ImportServices omits inline pipelines: runtimes are READY_TO_DEPLOY until Deploy
// uploads a prebuilt gzip-tar artifact and supplies a complete zerops document.
// ServiceSpec.Env is imported as envSecrets; non-secret runtime variables and
// volume mounts belong in Deployment.ZeropsYAML. ServiceSpec.ZeropsYAML is not
// imported because doing so triggers an empty-code build on Zerops.
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
// The client retries only explicitly idempotent GET/PUT requests on network errors,
// HTTP 429 and HTTP 5xx. Deploy, upload, lifecycle and routing synchronization are
// not replayed because they create processes or stream non-repeatable data. No
// authorization, environment content or credential-bearing URLs are logged.
//
// Unit replay fixtures preserve anonymized Zerops API response shapes.
// The e2e-tagged live smoke test needs ZEROPS_TOKEN and ZEROPS_PROJECT_ID,
// creates uniquely named kkzt-prefixed services and verifies teardown by listing
// the project after deleting its runtime and volume.
package zerops
