// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"net/netip"
	"sync"
)

const allocationAddressCacheLimit = 4096

type allocationAddresses struct {
	Address, AddressIPv6 string
}

type allocationAddressKey struct {
	target string
	id     string
}

// Nomad assigns CNI addresses once for an allocation's lifetime. Only those
// immutable addresses are shared across passes; liveness and authorization
// always come from the allocation inventory, never from this cache.
// Keys retain only the registry identity, so removed target snapshots and their
// listing clients can be reclaimed even when the address cache stays idle.
type allocationAddressCache struct {
	mu      sync.Mutex
	entries map[allocationAddressKey]allocationAddresses
	order   [allocationAddressCacheLimit]allocationAddressKey
	next    int
}

var allocationAddressInventory allocationAddressCache

func (c *allocationAddressCache) lookup(key allocationAddressKey) (allocationAddresses, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.entries[key]
	return value, ok
}

func (c *allocationAddressCache) store(key allocationAddressKey, addresses allocationAddresses) {
	if key.id == "" || addresses.Address == "" && addresses.AddressIPv6 == "" {
		return
	}
	for _, address := range []string{addresses.Address, addresses.AddressIPv6} {
		if address != "" {
			ip, err := netip.ParseAddr(address)
			if err != nil || ip.Zone() != "" {
				return
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[key]; exists {
		return
	}
	if c.entries == nil {
		c.entries = make(map[allocationAddressKey]allocationAddresses)
	}
	delete(c.entries, c.order[c.next])
	c.order[c.next] = key
	c.next = (c.next + 1) % allocationAddressCacheLimit
	c.entries[key] = addresses
}
