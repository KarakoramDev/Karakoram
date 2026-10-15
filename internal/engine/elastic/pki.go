// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"net"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/pki"
)

// CA is the shared certificate authority type. Its signing key is secret.
type CA = pki.CA

// NewCA creates a cluster CA using the shared PKI implementation.
func NewCA(commonName string, validity time.Duration) (*CA, error) {
	return pki.NewCA(commonName, validity)
}

// LoadCA validates a cluster CA certificate and its matching signing key.
func LoadCA(certPEM, keyPEM []byte) (*CA, error) { return pki.LoadCA(certPEM, keyPEM) }

// NeedsRenewal checks a leaf certificate using the shared renewal policy.
func NeedsRenewal(certPEM []byte, before time.Duration, now time.Time) (bool, error) {
	return pki.NeedsRenewal(certPEM, before, now)
}

// IssueTransport issues a node's mutually authenticated transport identity.
// Bare service hostnames are the Zerops discovery names, as for PSMDB; no
// undocumented DNS suffix is assumed. Loopback SANs support local probes.
func IssueTransport(ca *CA, node string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
	return issueNode(ca, node, nil, ips, true)
}

// IssueHTTP issues a server-only HTTP identity covering service discovery,
// loopback probes and any explicitly supplied public DNS names/IP addresses.
func IssueHTTP(ca *CA, node string, dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
	return issueNode(ca, node, dnsNames, ips, false)
}

func issueNode(ca *CA, node string, dnsNames []string, ips []net.IP, client bool) ([]byte, []byte, error) {
	if !ValidHostname(node) {
		return nil, nil, errors.New("certificate node must be a valid service hostname")
	}
	names := []string{node, "localhost"}
	for _, name := range dnsNames {
		found := false
		for _, n := range names {
			if n == name {
				found = true
				break
			}
		}
		if !found {
			names = append(names, name)
		}
	}
	addresses := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	for _, ip := range ips {
		found := false
		for _, existing := range addresses {
			if existing.Equal(ip) {
				found = true
				break
			}
		}
		if !found {
			addresses = append(addresses, ip)
		}
	}
	return ca.Issue(pki.CertRequest{CommonName: node, DNSNames: names, IPs: addresses, Server: true, Client: client})
}
