// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import "net"

// selectListenAddress is local runtime policy, not a control-plane wire field
// or restart-token input. IPv6-capable JVM listeners also accept IPv4; hosts
// without usable IPv6 sockets must bind the IPv4 wildcard instead. Immediately
// release the ephemeral socket; listenerAddress caches the result on first apply.
func selectListenAddress(listen func(string, string) (net.Listener, error)) string {
	listener, err := listen("tcp6", "[::]:0")
	if err != nil {
		return "0.0.0.0"
	}
	_ = listener.Close()
	return "::"
}

// All adapters are constructed even for readiness of another engine. Defer the
// probe until Neo4j is actually applied, and retain trusted native-test overrides.
func (e *Engine) listenerAddress() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.listenAddress == "" {
		e.listenAddress = selectListenAddress(net.Listen)
	}
	return e.listenAddress
}
