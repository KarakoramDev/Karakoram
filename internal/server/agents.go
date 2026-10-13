// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type cachedAgent struct {
	fingerprint string
	client      *agentapi.Client
}

// AgentDialer caches member clients while invalidating them on CA/token rotation.
type AgentDialer struct {
	store      *store.Store
	namespaces engine.Namespaces
	mu         sync.Mutex
	clients    map[string]cachedAgent
}

// NewAgentDialer constructs a cluster-scoped pinned HTTPS dialer.
func NewAgentDialer(st *store.Store, namespaces engine.Namespaces) *AgentDialer {
	return &AgentDialer{store: st, namespaces: namespaces, clients: map[string]cachedAgent{}}
}

var memberHostname = regexp.MustCompile(`^[a-z][a-z0-9]{0,39}$`)

// Dial returns a member client whose trust roots and token come from sealed persistence.
func (d *AgentDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if !memberHostname.MatchString(member) {
		return nil, engine.ErrInvalid
	}
	ca, err := d.store.GetClusterSecret(ctx, ref, "tls/ca.crt")
	if err != nil {
		return nil, err
	}
	tok, err := d.store.GetAgentToken(ctx, ref, member)
	if err != nil {
		return nil, err
	}
	fingerprint := secrets.Hash(string(ca)) + secrets.Hash(tok.Token)
	key := ref.Key() + "/" + member
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, exists := d.clients[key]; exists && c.fingerprint == fingerprint {
		return c.client, nil
	}
	opts := agentapi.ClientOptions{BaseURL: "https://" + member + ":9443", Token: tok.Token, CAPEM: ca}
	if d.namespaces != nil {
		if dialer := d.namespaces.Dialer(ref.Namespace); dialer != nil {
			opts.DialContext = dialer.DialContext
		}
	}
	client, err := agentapi.NewClient(opts)
	if err != nil {
		return nil, err
	}
	if old, exists := d.clients[key]; exists {
		old.client.Close()
	}
	d.clients[key] = cachedAgent{fingerprint: fingerprint, client: client}
	return client, nil
}

// Close releases all cached idle transports.
func (d *AgentDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, c := range d.clients {
		c.client.Close()
		delete(d.clients, key)
	}
}

// InternalHandler only exposes member configuration to the persisted member identity.
func (s *Server) InternalHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/agent/v1/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		authz := strings.Fields(r.Header.Get("Authorization"))
		if len(authz) != 2 || !strings.EqualFold(authz[0], "Bearer") {
			_ = fail(401, "unauthorized", "Agent authentication required").write(w)
			return
		}
		token, err := s.Store.FindAgentTokenByHash(r.Context(), secrets.Hash(authz[1]))
		if err != nil {
			_ = fail(401, "unauthorized", "Agent authentication required").write(w)
			return
		}
		rc, p, err := s.context(r.Context(), domain.ClusterRef{Namespace: token.Namespace, Name: token.Cluster})
		if err != nil {
			_ = s.failure(err).write(w)
			return
		}
		if failure := s.providerContext(rc); failure != nil {
			_ = failure.write(w)
			return
		}
		source, yes := p.(engine.AgentConfigSource)
		if !yes {
			_ = s.failure(engine.ErrNotSupported).write(w)
			return
		}
		cfg, err := source.MemberConfig(r.Context(), rc, token.Member)
		if err != nil {
			_ = s.failure(err).write(w)
			return
		}
		_ = ok(cfg).write(w)
	})
	return mux
}

type internalIdentity struct{ CACert, CAKey, Cert, Key []byte }

func internalTLS(ctx context.Context, st *store.Store, sealer *secrets.Sealer, hostname string, additionalNames ...string) (*tls.Config, string, error) {
	dnsNames := []string{hostname, "karakoram", "localhost"}
	persistedNamespaces, err := st.ListNamespaces(ctx)
	if err != nil {
		return nil, "", err
	}
	for _, n := range persistedNamespaces {
		if !n.Default && n.Relay.Hostname != "" {
			additionalNames = append(additionalNames, n.Relay.Hostname)
		}
	}
	for _, name := range additionalNames {
		if name == "" {
			continue
		}
		duplicate := false
		for _, existing := range dnsNames {
			if existing == name {
				duplicate = true
				break
			}
		}
		if !duplicate {
			dnsNames = append(dnsNames, name)
		}
	}
	const setting = "server/internal-pki"
	raw, err := st.GetSetting(ctx, setting)
	var identity internalIdentity
	if errors.Is(err, store.ErrNotFound) {
		ca, e := pki.NewCA("Karakoram internal CA", 10*365*24*time.Hour)
		if e != nil {
			return nil, "", e
		}
		cert, key, e := ca.Issue(pki.CertRequest{CommonName: hostname, DNSNames: dnsNames, IPs: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, Validity: 365 * 24 * time.Hour, Server: true})
		if e != nil {
			return nil, "", e
		}
		identity = internalIdentity{CACert: ca.CertPEM, CAKey: ca.KeyPEM, Cert: cert, Key: key}
	} else if err != nil {
		return nil, "", err
	} else {
		var ciphertext string
		if err = json.Unmarshal(raw, &ciphertext); err != nil {
			return nil, "", err
		}
		plain, e := sealer.OpenFor(setting, ciphertext)
		if e != nil {
			return nil, "", e
		}
		if err = json.Unmarshal(plain, &identity); err != nil {
			return nil, "", err
		}
	}
	cert, err := tls.X509KeyPair(identity.Cert, identity.Key)
	if err != nil {
		return nil, "", err
	}
	leaf, err := pki.NeedsRenewal(identity.Cert, 30*24*time.Hour, time.Now())
	if err != nil {
		return nil, "", err
	}
	missingSAN := cert.Leaf == nil
	if cert.Leaf != nil {
		for _, name := range dnsNames {
			if cert.Leaf.VerifyHostname(name) != nil {
				missingSAN = true
			}
		}
	}
	if leaf || missingSAN {
		ca, e := pki.LoadCA(identity.CACert, identity.CAKey)
		if e != nil {
			return nil, "", e
		}
		identity.Cert, identity.Key, e = ca.Issue(pki.CertRequest{CommonName: hostname, DNSNames: dnsNames, IPs: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, Validity: 365 * 24 * time.Hour, Server: true})
		if e != nil {
			return nil, "", e
		}
		cert, e = tls.X509KeyPair(identity.Cert, identity.Key)
		if e != nil {
			return nil, "", e
		}
	}
	if err := persistInternalIdentity(ctx, st, sealer, identity); err != nil {
		return nil, "", err
	}
	// TLS calls GetCertificate for every handshake. Keeping the active leaf
	// behind this lock makes renewal atomic, including concurrent handshakes,
	// and avoids depending on control-plane process restarts.
	var mu sync.Mutex
	getCertificate := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		mu.Lock()
		defer mu.Unlock()
		if cert.Leaf != nil && cert.Leaf.NotAfter.After(time.Now().Add(30*24*time.Hour)) {
			return &cert, nil
		}
		ca, err := pki.LoadCA(identity.CACert, identity.CAKey)
		if err != nil {
			return nil, err
		}
		certPEM, keyPEM, err := ca.Issue(pki.CertRequest{CommonName: hostname, DNSNames: dnsNames, IPs: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, Validity: 365 * 24 * time.Hour, Server: true})
		if err != nil {
			return nil, err
		}
		next, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, err
		}
		renewed := identity
		renewed.Cert, renewed.Key = certPEM, keyPEM
		renewalCtx := hello.Context()
		if renewalCtx == nil {
			renewalCtx = ctx
		}
		if err := persistInternalIdentity(renewalCtx, st, sealer, renewed); err != nil {
			return nil, err
		}
		identity, cert = renewed, next
		return &cert, nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: getCertificate}, string(identity.CACert), nil
}

func persistInternalIdentity(ctx context.Context, st *store.Store, sealer *secrets.Sealer, identity internalIdentity) error {
	const setting = "server/internal-pki"
	data, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	sealed, err := sealer.SealFor(setting, data)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(sealed)
	if err != nil {
		return err
	}
	return st.SetSetting(ctx, setting, raw)
}

var _ engine.AgentDialer = (*AgentDialer)(nil)
