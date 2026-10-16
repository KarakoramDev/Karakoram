// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
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
		p = p.Masked()
		prefixes[p] = true
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
func samePort(a port, p infra.PublicPort) bool {
	return a.PublishedPort == p.Port && a.TargetPort == p.TargetPort && a.Protocol == p.Protocol && a.PublishMode == "ingress"
}
func (b *Backend) EnsurePublicPort(ctx context.Context, hostname string, p infra.PublicPort) error {
	if err := validatePublicPort(p); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return err
	}
	if m.Kind != infra.KindRuntime {
		return reject("volume holders do not expose public ports")
	}
	for _, old := range a.Ports {
		if samePort(old, p) {
			return nil
		}
	}
	for _, old := range a.Ports {
		if old.PublishedPort == p.Port && old.Protocol == p.Protocol {
			if err = b.c.post(ctx, "port.delete", map[string]any{"portId": old.ID}, nil); err != nil {
				return err
			}
		}
	}
	if err = b.c.post(ctx, "port.create", map[string]any{"publishedPort": p.Port, "targetPort": p.TargetPort, "protocol": p.Protocol, "publishMode": "ingress", "applicationId": a.ID}, nil); err != nil {
		return err
	}
	kept := m.PublicPorts[:0]
	for _, old := range m.PublicPorts {
		if old.Port != p.Port || old.Protocol != p.Protocol {
			kept = append(kept, old)
		}
	}
	m.PublicPorts = append(kept, p)
	_, err = b.reloadUpdated(ctx, a, m, nil)
	return err
}
func (b *Backend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	e, err := b.environment(ctx)
	if err != nil {
		return nil, err
	}
	out := []infra.PublicPortRoute{}
	for _, a := range e.Applications {
		m, ok := parseMetadata(a.Description)
		if !ok {
			m.Hostname = a.Name
		}
		var full application
		if err = b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &full); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				continue
			}
			return nil, err
		}
		for _, p := range full.Ports {
			neutral := infra.PublicPort{Port: p.PublishedPort, TargetPort: p.TargetPort, Protocol: p.Protocol, IPType: "IPV4"}
			if b.target.Config["publicIPv4"] == "" {
				neutral.IPType = "IPV6"
			}
			for _, stored := range m.PublicPorts {
				if stored.Port == p.PublishedPort && stored.Protocol == p.Protocol {
					neutral = stored
					neutral.TargetPort = p.TargetPort
					break
				}
			}
			out = append(out, infra.PublicPortRoute{Hostname: m.Hostname, PublicPort: neutral})
		}
	}
	return out, nil
}
func (b *Backend) DeletePublicPorts(ctx context.Context, hostname string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	a, err := b.application(ctx, hostname)
	if err != nil || a == nil {
		return err
	}
	m, _ := parseMetadata(a.Description)
	if len(a.Ports) == 0 {
		return nil
	}
	for _, p := range a.Ports {
		if err = b.c.post(ctx, "port.delete", map[string]any{"portId": p.ID}, nil); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				continue
			}
			return err
		}
	}
	m.PublicPorts = nil
	_, err = b.reloadUpdated(ctx, a, m, nil)
	if errors.Is(err, infra.ErrNotFound) {
		return nil
	}
	return err
}
func (b *Backend) serviceNetworks(m metadata) []map[string]any {
	networks := []map[string]any{{"Target": b.network(), "Aliases": []string{m.Hostname}}}
	if b.network() != "dokploy-network" && len(m.HTTPPorts) > 0 && m.SubdomainEnabled && b.Capabilities().PublicHTTPS {
		networks = append(networks, map[string]any{"Target": "dokploy-network"})
	}
	return networks
}

func (b *Backend) sharedNetworkChanged(a *application, m metadata) bool {
	hasShared := false
	for _, n := range a.Networks {
		hasShared = hasShared || n.Target == "dokploy-network"
	}
	wantShared := b.network() == "dokploy-network" || (len(m.HTTPPorts) > 0 && m.SubdomainEnabled && b.Capabilities().PublicHTTPS)
	return hasShared != wantShared
}
func (b *Backend) domainHost(hostname string, port int) string {
	sum := sha256.Sum256([]byte(b.target.ID))
	label := hostname + "-" + strconv.Itoa(port) + "-" + fmt.Sprintf("%x", sum[:4])
	if len(label) > 63 {
		digest := sha256.Sum256([]byte(label))
		label = strings.TrimRight(label[:54], "-") + "-" + fmt.Sprintf("%x", digest[:4])
	}
	return label + "." + b.target.Config["domainSuffix"]
}

func (b *Backend) desiredDomains(m metadata) map[string]int {
	if !m.SubdomainEnabled || !b.Capabilities().PublicHTTPS {
		return nil
	}
	desired := make(map[string]int, len(m.HTTPPorts))
	for _, port := range m.HTTPPorts {
		desired[b.domainHost(m.Hostname, port)] = port
	}
	return desired
}

func (b *Backend) managedDomain(m metadata, d domain) bool {
	if contains(m.DomainHosts, d.Host) {
		return true
	}
	// Recover existing managed hosts independently of the current HTTP-port
	// list, including compact labels produced for long service hostnames.
	if d.Port > 0 && d.Port <= 65535 && d.Host == b.domainHost(m.Hostname, d.Port) {
		return true
	}
	sum := sha256.Sum256([]byte(b.target.ID))
	prefix := m.Hostname + "-"
	suffix := "-" + fmt.Sprintf("%x", sum[:4]) + "." + b.target.Config["domainSuffix"]
	if !strings.HasPrefix(d.Host, prefix) || !strings.HasSuffix(d.Host, suffix) {
		return false
	}
	port, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(d.Host, prefix), suffix))
	return err == nil && port > 0 && port <= 65535 && d.Host == b.domainHost(m.Hostname, port)
}

// reconcileDomains applies the complete desired domain set, not just ports in
// the latest runtime manifest. A redeploy can remove an HTTP marking before a
// subsequent disable request, and those former routes must still be withdrawn.
func (b *Backend) reconcileDomains(ctx context.Context, a *application, m *metadata) (bool, error) {
	desired := b.desiredDomains(*m)
	hosts := make([]string, 0, len(desired))
	for host := range desired {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	changed := len(hosts) != len(m.DomainHosts)
	for i, host := range hosts {
		if !changed && host != m.DomainHosts[i] {
			changed = true
		}
	}
	existing := make(map[string]bool, len(a.Domains))
	for _, d := range a.Domains {
		port, wanted := desired[d.Host]
		if !wanted && !b.managedDomain(*m, d) {
			continue
		}
		if wanted && d.HTTPS && d.Port == port && !existing[d.Host] {
			existing[d.Host] = true
			continue
		}
		if err := b.c.post(ctx, "domain.delete", map[string]any{"domainId": d.ID}, nil); err != nil && !errors.Is(err, infra.ErrNotFound) {
			return changed, err
		}
		changed = true
	}
	certificateType := b.target.Config["certificateType"]
	if certificateType == "" {
		certificateType = "letsencrypt"
	}
	for _, host := range hosts {
		if existing[host] {
			continue
		}
		if err := b.c.post(ctx, "domain.create", map[string]any{"host": host, "path": "/", "port": desired[host], "https": true, "certificateType": certificateType, "applicationId": a.ID, "domainType": "application", "stripPath": false}, nil); err != nil {
			return changed, err
		}
		changed = true
	}
	m.DomainHosts = hosts
	return changed, nil
}

func (b *Backend) SetSubdomainAccess(ctx context.Context, hostname string, enabled bool) (infra.Op, error) {
	if enabled && !b.Capabilities().PublicHTTPS {
		return infra.Op{}, infra.ErrUnsupported
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, err := b.application(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if a == nil {
		if !enabled {
			return infra.Op{}, nil
		}
		return infra.Op{}, infra.ErrNotFound
	}
	if !b.Capabilities().PublicHTTPS {
		return infra.Op{}, infra.ErrUnsupported
	}
	m, _ := parseMetadata(a.Description)
	if m.Kind != infra.KindRuntime {
		return infra.Op{}, reject("volume holders do not expose HTTP domains")
	}
	wasEnabled := m.SubdomainEnabled
	m.SubdomainEnabled = enabled
	changed, err := b.reconcileDomains(ctx, a, &m)
	if err != nil {
		if !enabled && errors.Is(err, infra.ErrNotFound) {
			return infra.Op{}, nil
		}
		return infra.Op{}, err
	}
	if wasEnabled == enabled && !b.sharedNetworkChanged(a, m) {
		if changed {
			err = b.update(ctx, a, m, nil)
		}
		if !enabled && errors.Is(err, infra.ErrNotFound) {
			return infra.Op{}, nil
		}
		return infra.Op{}, err
	}
	op, err := b.reloadUpdated(ctx, a, m, map[string]any{"networkSwarm": b.serviceNetworks(m)})
	if !enabled && errors.Is(err, infra.ErrNotFound) {
		return infra.Op{}, nil
	}
	return op, err
}
func (b *Backend) SubdomainURLs(ctx context.Context, hostname string) ([]string, error) {
	a, err := b.application(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return []string{}, nil
	}
	if !b.Capabilities().PublicHTTPS {
		return nil, infra.ErrUnsupported
	}
	m, _ := parseMetadata(a.Description)
	out := []string{}
	desired := b.desiredDomains(m)
	for _, d := range a.Domains {
		if port, wanted := desired[d.Host]; wanted && d.HTTPS && d.Port == port {
			out = append(out, "https://"+d.Host)
			delete(desired, d.Host)
		}
	}
	sort.Strings(out)
	return out, nil
}
