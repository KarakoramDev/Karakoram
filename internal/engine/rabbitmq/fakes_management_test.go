// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type managementDialer struct{ plain, secure string }

func (d managementDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	target := d.plain
	if port == "15671" {
		target = d.secure
	} else if port != "15672" {
		return nil, fmt.Errorf("unexpected management relay target %s", address)
	}
	return (&net.Dialer{}).DialContext(ctx, network, target)
}

func setupManagement(t *testing.T, rc *engine.Context, agents *fakeAgents) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		want, err := rc.Secrets.GetClusterSecret(r.Context(), rc.Cluster.Ref(), "users/karakoram")
		if err != nil || !ok || user != "karakoram" || password != string(want) {
			t.Errorf("management authentication failed: user=%s secret-error=%v", user, err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/api/overview":
			_ = json.NewEncoder(w).Encode(map[string]string{"rabbitmq_version": rc.Cluster.Spec.Version})
		case "/api/queues":
			agents.log.add("mgmt:" + r.Host + ":queues")
			queues := []mgmt.Queue{}
			if agents.queueSource != nil {
				queues = agents.queueSource()
			}
			_ = json.NewEncoder(w).Encode(queues)
		default:
			t.Errorf("unexpected default management request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	plain := httptest.NewServer(handler)
	t.Cleanup(plain.Close)
	secure := httptest.NewUnstartedServer(handler)
	var mu sync.Mutex
	certificates := map[string]*tls.Certificate{}
	secure.TLS = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		crt, err := rc.Secrets.GetClusterSecret(context.Background(), rc.Cluster.Ref(), "tls/ca.crt")
		if err != nil {
			return nil, err
		}
		key, err := rc.Secrets.GetClusterSecret(context.Background(), rc.Cluster.Ref(), "tls/ca.key")
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		cacheKey := string(crt) + hello.ServerName
		if certificate := certificates[cacheKey]; certificate != nil {
			return certificate, nil
		}
		ca, err := pki.LoadCA(crt, key)
		if err != nil {
			return nil, err
		}
		leaf, private, err := ca.Issue(pki.CertRequest{CommonName: hello.ServerName, DNSNames: []string{hello.ServerName}, Validity: 365 * 24 * time.Hour, Server: true})
		if err != nil {
			return nil, err
		}
		certificate, err := tls.X509KeyPair(leaf, private)
		if err != nil {
			return nil, err
		}
		certificates[cacheKey] = &certificate
		return &certificate, nil
	}}
	secure.StartTLS()
	t.Cleanup(secure.Close)
	rc.Dialer = managementDialer{plain: plain.Listener.Addr().String(), secure: secure.Listener.Addr().String()}
}
