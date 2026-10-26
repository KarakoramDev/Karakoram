// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"net/netip"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func coversFamily(allowlist []string, ipv4 bool) bool {
	if len(allowlist) == 0 {
		return true
	}
	prefixes := map[netip.Prefix]bool{}
	for _, s := range allowlist {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return false
		}
		if p.Addr().Is4() != ipv4 {
			continue
		}
		prefixes[p.Masked()] = true
	}
	for p := range prefixes {
		for p.Bits() > 0 {
			bytes := p.Addr().AsSlice()
			bit := p.Bits() - 1
			bytes[bit/8] ^= 1 << uint(7-bit%8)
			addr, ok := netip.AddrFromSlice(bytes)
			if !ok {
				return false
			}
			sibling := netip.PrefixFrom(addr, p.Bits()).Masked()
			if !prefixes[sibling] {
				break
			}
			p = netip.PrefixFrom(p.Addr(), p.Bits()-1).Masked()
			prefixes[p] = true
		}
	}
	if ipv4 {
		return prefixes[netip.MustParsePrefix("0.0.0.0/0")]
	}
	return prefixes[netip.MustParsePrefix("::/0")]
}
func validatePublicPort(p infra.PublicPort) error {
	if p.Port < 1 || p.Port > 65535 || p.TargetPort < 1 || p.TargetPort > 65535 || (p.Protocol != "tcp" && p.Protocol != "udp") {
		return reject("invalid public port or protocol")
	}
	if p.IPType != "IPV4" && p.IPType != "IPV6" {
		return reject("public port requires IPV4 or IPV6")
	}
	for _, s := range p.Allowlist {
		if _, err := netip.ParsePrefix(s); err != nil {
			return reject("invalid public port allowlist CIDR")
		}
	}
	if !coversFamily(p.Allowlist, p.IPType == "IPV4") {
		return infra.ErrUnsupported
	}
	return nil
}
func (b *Backend) EnsurePublicPort(ctx context.Context, hostname string, p infra.PublicPort) error {
	if err := validatePublicPort(p); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return err
	}
	if r.Kind != infra.KindRuntime {
		return reject("volume services do not expose public ports")
	}
	for _, old := range r.PublicPorts {
		if old.Port == p.Port && old.TargetPort == p.TargetPort && old.Protocol == p.Protocol && old.IPType == p.IPType {
			if r.Pending != nil && r.Pending.Action == "ports" && r.Pending.Phase == "failed" {
				return b.applyPorts(ctx, r)
			}
			return nil
		}
	}
	kept := r.PublicPorts[:0]
	for _, old := range r.PublicPorts {
		if old.Port != p.Port || old.Protocol != p.Protocol {
			kept = append(kept, old)
		}
	}
	r.PublicPorts = append(kept, p)
	return b.applyPorts(ctx, r)
}
func (b *Backend) applyPorts(ctx context.Context, r *record) error {
	if r.Deployment == nil || r.Desired != "running" {
		return b.save(ctx, r)
	}
	// Initiate, but do not wait under b.mu for shutdown or preparation.
	// Observations resume this durable operation; later mutations supersede it
	// using the latest persisted ports and the runtime's rollout policy.
	_, err := b.begin(ctx, r, "ports")
	return err
}

func (b *Backend) resumePending(ctx context.Context, r *record) error {
	if r.Pending == nil || r.Pending.Phase == "done" || r.Pending.Phase == "failed" {
		return nil
	}
	ctx = infra.WithoutObservation(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	current, err := b.require(ctx, r.Hostname)
	if err != nil {
		return err
	}
	if current.ID != r.ID {
		return infra.ErrConflict
	}
	if current.Pending != nil {
		if _, err = b.opStatus(ctx, encodeOp(*current.Pending)); err != nil {
			return err
		}
		current, err = b.load(ctx, r.Hostname)
		if current == nil && err == nil {
			return nil // Delete completed while observing its durable operation.
		}
		if err != nil {
			return err
		}
	}
	*r = *current
	return nil
}
func (b *Backend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	rs, _, err := b.records(ctx, "")
	if err != nil {
		return nil, err
	}
	out := []infra.PublicPortRoute{}
	for _, r := range rs {
		for _, p := range r.PublicPorts {
			out = append(out, infra.PublicPortRoute{Hostname: r.Hostname, PublicPort: p})
		}
	}
	return out, nil
}
func (b *Backend) DeletePublicPorts(ctx context.Context, hostname string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.load(ctx, hostname)
	if err != nil || r == nil {
		return err
	}
	if len(r.PublicPorts) == 0 {
		if r.Pending != nil && r.Pending.Action == "ports" && r.Pending.Phase == "failed" {
			return b.applyPorts(ctx, r)
		}
		return nil
	}
	r.PublicPorts = nil
	return b.applyPorts(ctx, r)
}
