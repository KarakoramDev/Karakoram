// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"bytes"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPKI(t *testing.T) {
	now := time.Now()
	ca, key, err := GenerateCA(now)
	if err != nil {
		t.Fatal(err)
	}
	root, err := ParseCertificatePEM(ca)
	if err != nil {
		t.Fatal(err)
	}
	if !root.IsCA {
		t.Fatal("not a CA")
	}
	client, _, err := IssueClientCertificate(ca, key, "proj-test", now)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ParseCertificatePEM(client)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := NamespaceFromCert(cert)
	if err != nil || ns != "proj-test" {
		t.Fatal(ns, err)
	}
	if cert.SerialNumber.BitLen() != 128 || cert.NotAfter.Sub(now) < 364*24*time.Hour || cert.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth {
		t.Fatal("incorrect client certificate")
	}
	if NeedsRenewal(cert, now) || !NeedsRenewal(cert, cert.NotAfter.Add(-29*24*time.Hour)) || NeedsRenewal(cert, cert.NotAfter.Add(-30*24*time.Hour)) {
		t.Fatal("incorrect renewal threshold")
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	if _, err = cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatal(err)
	}
	hub, _, err := IssueHubCertificate(ca, key, now)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ParseCertificatePEM(hub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.Verify(x509.VerifyOptions{Roots: roots, DNSName: hubServerName}); err != nil {
		t.Fatal(err)
	}
	if _, err = NamespaceFromCert(server); err == nil {
		t.Fatal("server identity accepted as client")
	}
	for _, ns := range []string{"default", "x", "UPPER", "a.b"} {
		if _, _, err = IssueClientCertificate(ca, key, ns, now); err == nil {
			t.Fatal("invalid namespace accepted", ns)
		}
	}
}
func TestHeaderFraming(t *testing.T) {
	for _, v := range []any{Dial{Type: "dial", Addr: "member:27017"}, Agent{Type: "agent"}, Policy{Type: "policy", Targets: []string{"member:9443"}, Generation: 1}, Hello{Type: "hello", Namespace: "proj-test"}} {
		var b bytes.Buffer
		if err := WriteHeader(&b, v); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadHeader(&b); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"type":"unknown"}`, `{"type":"agent","extra":true}`, `{"type":"agent"} {}`, `null`} {
		var b bytes.Buffer
		binary.Write(&b, binary.BigEndian, uint32(len(raw)))
		b.WriteString(raw)
		if _, err := ReadHeader(&b); err == nil {
			t.Fatal("invalid header accepted", raw)
		}
	}
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(MaxHeaderSize+1))
	if _, err := ReadHeader(&b); err == nil {
		t.Fatal("oversized input accepted")
	}
	b.Reset()
	if err := WriteHeader(&b, Dial{Type: "dial", Addr: strings.Repeat("x", MaxHeaderSize)}); err == nil || b.Len() != 0 {
		t.Fatal("oversized output accepted")
	}
	b.Reset()
	binary.Write(&b, binary.BigEndian, uint32(0))
	if _, err := ReadHeader(&b); err == nil {
		t.Fatal("zero size accepted")
	}
	for _, reply := range []Reply{{OK: true}, {Error: "refused"}} {
		b.Reset()
		if err := WriteHeader(&b, reply); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadReply(&b); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLargeControlPolicy(t *testing.T) {
	targets := make([]string, 2000)
	for i := range targets {
		targets[i] = fmt.Sprintf("member%d:27017", i)
	}
	policy := Policy{Type: "policy", Targets: targets, Generation: 1}
	var frame bytes.Buffer
	if err := WriteControlHeader(&frame, policy); err != nil {
		t.Fatal(err)
	}
	if frame.Len() <= MaxHeaderSize {
		t.Fatal("test policy is not larger than a stream header")
	}
	if _, err := ReadHeader(bytes.NewReader(frame.Bytes())); err == nil {
		t.Fatal("large policy accepted on ordinary stream")
	}
	if decoded, err := ReadControlHeader(&frame); err != nil || len(decoded.(*Policy).Targets) != 2000 {
		t.Fatal("large control policy rejected", err)
	}
	f := newFixture(t)
	f.hub.SetPolicy("proj-test", targets)
	f.runRelay(f.o)
	f.connected()
	if err := tunnelEcho(f.hub, targets[len(targets)-1]); err != nil {
		t.Fatal(err)
	}
}

func TestControlBoundsAndValidation(t *testing.T) {
	for _, targets := range [][]string{{"missing-port"}, {":27017"}, {"member:invalid"}, {"member:0"}, {"member:65536"}, {"bad host:27017"}, make([]string, MaxPolicyTargets+1)} {
		var frame bytes.Buffer
		if err := WriteControlHeader(&frame, Policy{Type: "policy", Targets: targets}); err == nil || frame.Len() != 0 {
			t.Fatal("invalid outbound policy accepted")
		}
		if err := writeHeader(&frame, Policy{Type: "policy", Targets: targets}, MaxControlSize); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadControlHeader(&frame); err == nil {
			t.Fatal("invalid inbound policy accepted")
		}
	}
	var frame bytes.Buffer
	if err := WriteControlHeader(&frame, Hello{Type: "hello", Version: strings.Repeat("x", MaxControlSize)}); err == nil || frame.Len() != 0 {
		t.Fatal("oversized control output accepted")
	}
	binary.Write(&frame, binary.BigEndian, uint32(MaxControlSize+1))
	if _, err := ReadControlHeader(&frame); err == nil {
		t.Fatal("oversized control input accepted")
	}
	frame.Reset()
	if err := WriteControlHeader(&frame, Dial{Type: "dial", Addr: "member:27017"}); err == nil {
		t.Fatal("dial accepted on control stream")
	}
	if err := WriteHeader(&frame, Agent{Type: "agent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadControlHeader(&frame); err == nil {
		t.Fatal("agent accepted on control stream")
	}
	frame.Reset()
	hello := Hello{Type: "hello", Version: strings.Repeat("x", MaxHeaderSize)}
	if err := WriteControlHeader(&frame, hello); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadControlHeader(&frame); err != nil {
		t.Fatal(err)
	}
	frame.Reset()
	if err := WriteHeader(&frame, Reply{Error: strings.Repeat("x", MaxHeaderSize)}); err == nil {
		t.Fatal("oversized reply accepted")
	}
}
