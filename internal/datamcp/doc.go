// Package datamcp provides the authenticated, manifest-filtered per-cluster MCP
// data gateway. New constructs an HTTP handler and Close releases cached clients.
// Call admission precedes upstream discovery, including for denied calls.
// Token bindings follow SDK session lifetime: DELETE prunes them immediately;
// the minute reaper removes bindings for other closures, including idle expiry.
// Upstreams open independently per cluster/access level without holding the
// gateway mutex. Roots, sampling and elicitation are neither advertised nor
// fulfilled for upstreams, including multi-round-trip input requests.
//
// Built-in confirmations use the SDK's multi-round-trip InputRequests mechanism,
// not a blocking ServerSession.Elicit call. Protocol 2026-07-28 clients fulfill a
// form and retry with InputResponses and RequestState; the SDK translates this
// exchange into elicitation/create for legacy stateful clients. RequestState is
// HMAC-authenticated and binds the cluster, API token, tool, effective canonical
// argument hash, expiry and single-use nonce. It contains no query text. Missing
// capability, refusal, expiry, altered binding and replay all fail closed. An
// injected Confirmer retains the synchronous interface for trusted integrations.
// Each protocol tools/call is audited: an input-required challenge has outcome
// "pending" and confirmation "requested"; the retry records its final outcome.
// Thus an abandoned confirmation still leaves a data-free audit trail.
// Legacy SDK elicitation failures also produce a terminal error audit even when
// the SDK cannot reinvoke the tool handler.
//
// Structured output is normalized into the same bounded text channel as other
// content, preventing an unbounded or unwrapped parallel data channel. Upstream
// output schemas are therefore not advertised. Argument and result text is never
// logged; audit argument text requires explicit cluster opt-in.
package datamcp
