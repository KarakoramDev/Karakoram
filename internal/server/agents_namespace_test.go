// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type namespaceAgentTransport struct {
	engine.StaticNamespaces
	mu        sync.Mutex
	addresses []string
}

func (n *namespaceAgentTransport) Dialer(namespace string) engine.ContextDialer {
	if domain.NormalizedNamespace(namespace) == domain.DefaultNamespace {
		return nil
	}
	return n
}

func (n *namespaceAgentTransport) DialContext(_ context.Context, _ string, addr string) (net.Conn, error) {
	n.mu.Lock()
	n.addresses = append(n.addresses, addr)
	n.mu.Unlock()
	return nil, errors.New("synthetic relay connection failure")
}

func TestAgentDialerNamespacesRouteAndCacheIndependently(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	ca, err := pki.NewCA("test agent CA", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	refs := []domain.ClusterRef{{Namespace: domain.DefaultNamespace, Name: "demo"}, {Namespace: "production", Name: "demo"}, {Namespace: "development", Name: "demo"}}
	for _, ref := range refs {
		if err := s.Store.SetClusterSecret(ctx, ref, "tls/ca.crt", ca.CertPEM); err != nil {
			t.Fatal(err)
		}
		if err := s.Store.SetAgentToken(ctx, &domain.AgentToken{Namespace: ref.Namespace, Cluster: ref.Name, Member: "localhost", Token: ref.Namespace + "-test-token"}); err != nil {
			t.Fatal(err)
		}
	}
	namespaces := &namespaceAgentTransport{}
	d := NewAgentDialer(s.Store, namespaces)
	defer d.Close()
	defaultClient, err := d.Dial(ctx, refs[0], "localhost")
	if err != nil {
		t.Fatal(err)
	}
	remoteClient, err := d.Dial(ctx, refs[1], "localhost")
	if err != nil || remoteClient == defaultClient {
		t.Fatal("namespace cache identities collided", err)
	}
	otherClient, err := d.Dial(ctx, refs[2], "localhost")
	if err != nil || otherClient == remoteClient {
		t.Fatal("non-default namespace cache identities collided", err)
	}
	cached, err := d.Dial(ctx, refs[1], "localhost")
	if err != nil || cached != remoteClient {
		t.Fatal("same namespace cache missed", err)
	}
	if _, err := remoteClient.Status(ctx); err == nil {
		t.Fatal("failing relay transport unexpectedly succeeded")
	}
	namespaces.mu.Lock()
	if len(namespaces.addresses) != 1 || namespaces.addresses[0] != "localhost:9443" {
		t.Fatalf("relay changed agent hostname: %v", namespaces.addresses)
	}
	namespaces.mu.Unlock()
	directCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, _ = defaultClient.Status(directCtx)
	namespaces.mu.Lock()
	defer namespaces.mu.Unlock()
	if len(namespaces.addresses) != 1 {
		t.Fatal("default namespace used relay transport")
	}
}
