// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

var _ Access = (*Provider)(nil)

const certificateJournalKey = "temporal/client-certificates"
const maxClientCertificates = 4096

// storedClientCertificate deliberately excludes the read-time CurrentCA flag.
type storedClientCertificate struct {
	Serial              string    `json:"serial"`
	CommonName          string    `json:"commonName"`
	FingerprintSHA256   string    `json:"fingerprintSha256"`
	IssuedAt            time.Time `json:"issuedAt"`
	NotAfter            time.Time `json:"notAfter"`
	CAFingerprintSHA256 string    `json:"caFingerprintSha256"`
}

var clientCN = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,62}[a-zA-Z0-9])?$`)

func fingerprint(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

// IssueClientCertificate returns a client identity once and journals public metadata.
func (p *Provider) IssueClientCertificate(ctx context.Context, rc *engine.Context, cn string, ttl time.Duration) (IssuedClientCertificate, error) {
	p.certificateMu.Lock()
	defer p.certificateMu.Unlock()
	var out IssuedClientCertificate
	if !clientCN.MatchString(cn) || len(cn) > 64 {
		return out, engine.Invalid("commonName must be a DNS-label-like name of at most 64 characters")
	}
	if ttl < time.Hour || ttl > 397*24*time.Hour {
		return out, engine.Invalid("certificate ttl must be between 1h and 397d")
	}
	journal, e := p.ListClientCertificates(ctx, rc)
	if e != nil {
		return out, e
	}
	if len(journal) >= maxClientCertificates {
		return out, fmt.Errorf("%w: client certificate journal is full", engine.ErrConflict)
	}
	ca, e := ensureCA(ctx, rc)
	if e != nil {
		return out, e
	}
	cert, key, e := ca.Issue(pki.CertRequest{CommonName: cn, Validity: ttl, Client: true})
	if e != nil {
		return out, e
	}
	block, _ := pem.Decode(cert)
	leaf, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		return out, e
	}
	out.ClientCertificate = ClientCertificate{
		Serial: leaf.SerialNumber.String(), CommonName: cn,
		FingerprintSHA256: fingerprint(leaf.Raw),
		IssuedAt:          clockNow(rc), NotAfter: leaf.NotAfter,
		CAFingerprintSHA256: fingerprint(ca.Cert.Raw),
		CurrentCA:           true,
	}
	journal = append(journal, out.ClientCertificate)
	stored := make([]storedClientCertificate, len(journal))
	for i, c := range journal {
		stored[i] = storedClientCertificate{
			Serial: c.Serial, CommonName: c.CommonName, FingerprintSHA256: c.FingerprintSHA256,
			IssuedAt: c.IssuedAt, NotAfter: c.NotAfter, CAFingerprintSHA256: c.CAFingerprintSHA256,
		}
	}
	raw, e := json.Marshal(stored)
	if e != nil {
		return out, e
	}
	if e = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), certificateJournalKey, raw); e != nil {
		return IssuedClientCertificate{}, e
	}
	out.CertificatePEM = string(cert)
	out.PrivateKeyPEM = string(key)
	out.CAPEM = string(ca.CertPEM)
	out.FrontendAddress = memberName(rc.Cluster, frontendComponent(rc.Cluster)) + ":7233"
	out.ServerName = FrontendServerName
	return out, nil
}

// ListClientCertificates returns metadata; CA fingerprint mismatch marks invalidation.
func (*Provider) ListClientCertificates(ctx context.Context, rc *engine.Context) ([]ClientCertificate, error) {
	out := []ClientCertificate{}
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), certificateJournalKey)
	if missing(e) {
		return out, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(raw, &out); e != nil {
		return nil, e
	}
	if len(out) == 0 {
		return out, nil
	}
	ca, e := ensureCA(ctx, rc)
	if e != nil {
		return nil, e
	}
	current := fingerprint(ca.Cert.Raw)
	for i := range out {
		out[i].CurrentCA = out[i].CAFingerprintSHA256 == current
	}
	return out, nil
}

func frontendClient(ctx context.Context, rc *engine.Context) (*agentapi.Client, error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return nil, e
	}
	m := s.Members[frontendComponent(rc.Cluster)]
	if m == nil || rc.Agents == nil {
		return nil, engine.ErrUnavailable
	}
	st, e := componentStatus(ctx, rc, m)
	if e != nil || !st.Ready {
		return nil, engine.ErrUnavailable
	}
	c, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return nil, engine.ErrUnavailable
	}
	return c, nil
}

// ListNamespaces reports observed namespaces and whether options manage them.
func (*Provider) ListNamespaces(ctx context.Context, rc *engine.Context) ([]Namespace, error) {
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		return nil, e
	}
	c, e := frontendClient(ctx, rc)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	var observed []tspec.NamespaceStatus
	if e = c.Engine(ctx, tspec.Engine, tspec.ActionNamespaces, struct{}{}, &observed); e != nil {
		return nil, engine.ErrUnavailable
	}
	managed := map[string]bool{"default": true}
	for _, n := range o.Namespaces {
		managed[n.Name] = true
	}
	out := make([]Namespace, 0, len(observed))
	for _, n := range observed {
		out = append(out, Namespace{Name: n.Name, Retention: n.Retention, Description: n.Description, State: n.State, Managed: managed[n.Name]})
	}
	return out, nil
}

// DeleteNamespace requests deletion only for non-reserved, unmanaged namespaces.
func (*Provider) DeleteNamespace(ctx context.Context, rc *engine.Context, name string) error {
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		return e
	}
	protected := name == "default" || name == "temporal-system"
	for _, n := range o.Namespaces {
		protected = protected || n.Name == name
	}
	if protected {
		return fmt.Errorf("%w: namespace is reserved or declared in engineOptions", engine.ErrConflict)
	}
	if e = (tspec.Namespace{Name: name, Retention: "72h"}).Validate(); e != nil {
		return engine.Invalid("invalid namespace name")
	}
	c, e := frontendClient(ctx, rc)
	if e != nil {
		return e
	}
	defer c.Close()
	if e = c.Engine(ctx, tspec.Engine, tspec.ActionDeleteNamespace, tspec.DeleteNamespace{Name: name}, nil); e != nil {
		return namespaceError(e)
	}
	return nil
}

func namespaceError(e error) error { // Never expose remote response bodies.
	var remote *agentapi.HTTPError
	if errors.As(e, &remote) {
		switch remote.StatusCode {
		case 404:
			return engine.ErrNotFound
		case 409:
			return engine.ErrConflict
		case 400, 422:
			return engine.Invalid("invalid namespace request")
		}
	}
	if e != nil {
		return engine.ErrUnavailable
	}
	return nil
}

// SetUIOIDCClientSecret seals a write-only secret for the next configuration roll.
func (*Provider) SetUIOIDCClientSecret(ctx context.Context, rc *engine.Context, secret string) error {
	if len(secret) == 0 || len(secret) > 4096 {
		return engine.Invalid("OIDC client secret must contain 1-4096 bytes")
	}
	// desiredSpec hashes the secret-bearing UI projection; Sync republishes it without storing the secret in state.
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), UIOIDCClientSecretKey, []byte(secret))
}
