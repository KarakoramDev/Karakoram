// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rabbitmq renders and supervises a RabbitMQ member using the shared
// agent.Engine protocol. Managed files live below DATA_DIR/karakoram; broker
// storage lives below DATA_DIR/rabbitmq. Cookies and keys are owner-only files.
//
// Planned discovery peers and the initial quorum replica count are written for
// the next boot without restarting an existing broker. Other configuration,
// version, plugin, cookie and TLS changes require a restart. Maintenance and
// durable operator stop holds remain authoritative. The RabbitMQ stop route
// drains and stops without creating a durable hold: configuration reloads do
// not undo that stop, but an explicit start or a fresh supervisor can resume it.
//
// RabbitMQ routes are fixed, authenticated by the shared transport, serialized
// with configuration and generic process controls, and deadline-bounded. CLI
// arguments are not caller-controlled command lines. Password updates read
// cached system credentials and supply passwords exclusively on standard input.
// KARAKORAM_RABBITMQ_BIN selects the CLI directory; its default is the pinned
// distribution's /usr/lib/rabbitmq/bin directory.
//
// Metrics scrape the local aggregated Prometheus endpoint and translate only
// rmqspec.MetricNames, without forwarding broker labels. Running member count
// includes the local node and established distribution links to planned peers;
// transient CLI distribution connections are excluded.
package rabbitmq
