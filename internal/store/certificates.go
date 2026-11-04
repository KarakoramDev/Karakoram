// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// CertificateExpiry contains only public identity and expiry, never certificate or key material.
type CertificateExpiry struct {
	Namespace string
	Cluster   string
	Name      string
	NotAfter  time.Time
}

// CertificateInventory reads persisted installation and engine certificates. Partial
// results remain usable if another identity is unreadable; errors never contain PEM.
func (s *Store) CertificateInventory(ctx context.Context) ([]CertificateExpiry, error) {
	var out []CertificateExpiry
	var failures error
	add := func(namespace, cluster, name string, data []byte) {
		for len(data) > 0 {
			block, rest := pem.Decode(data)
			if block == nil {
				break
			}
			data = rest
			if block.Type != "CERTIFICATE" {
				continue
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				failures = errors.Join(failures, fmt.Errorf("cannot parse certificate %s/%s/%s", namespace, cluster, name))
				return
			}
			out = append(out, CertificateExpiry{Namespace: namespace, Cluster: cluster, Name: name, NotAfter: cert.NotAfter})
			return
		}
		failures = errors.Join(failures, fmt.Errorf("certificate unavailable %s/%s/%s", namespace, cluster, name))
	}
	raw, err := s.GetSetting(ctx, "server/internal-pki")
	if err == nil {
		var ciphertext string
		var identity struct{ CACert, Cert []byte }
		err = json.Unmarshal(raw, &ciphertext)
		if err == nil {
			var plain []byte
			plain, err = s.sealer.OpenFor("server/internal-pki", ciphertext)
			if err == nil {
				err = json.Unmarshal(plain, &identity)
			}
		}
		if err == nil {
			add("", "", "internal-ca", identity.CACert)
			add("", "", "internal-server", identity.Cert)
		} else {
			failures = errors.Join(failures, errors.New("cannot inspect internal certificates"))
		}
	} else if !errors.Is(err, ErrNotFound) {
		failures = errors.Join(failures, errors.New("cannot load internal certificates"))
	}
	relay, err := s.GetRelayPKI(ctx)
	if err == nil {
		add("", "", "relay-ca", relay.CACert)
		add("", "", "relay-hub", relay.HubCert)
	} else if !errors.Is(err, ErrNotFound) {
		failures = errors.Join(failures, errors.New("cannot inspect relay certificates"))
	}
	namespaces, err := s.ListNamespaces(ctx)
	if err != nil {
		failures = errors.Join(failures, errors.New("cannot list relay certificate inventory"))
	}
	for _, namespace := range namespaces {
		if len(namespace.ClientCert) > 0 {
			add(namespace.Name, "", "relay-client", namespace.ClientCert)
		}
	}
	clusters, err := s.ListClusters(ctx, "")
	if err != nil {
		failures = errors.Join(failures, errors.New("cannot list complete cluster certificate inventory"))
	}
	for _, cluster := range clusters {
		rows, err := s.reader.QueryContext(ctx, "SELECT key FROM records WHERE kind='secret' AND scope=? ORDER BY key", cluster.Ref().Key())
		if err != nil {
			failures = errors.Join(failures, errors.New("cannot list cluster certificates"))
			continue
		}
		var keys []string
		for rows.Next() {
			var key string
			if err = rows.Scan(&key); err != nil {
				break
			}
			if strings.HasPrefix(key, "tls/") && (strings.HasSuffix(key, ".crt") || strings.HasSuffix(key, ".pem")) && !strings.Contains(key, ".pending.") {
				keys = append(keys, key)
			}
		}
		err = errors.Join(err, rows.Err(), rows.Close())
		if err != nil {
			failures = errors.Join(failures, errors.New("cannot read cluster certificate inventory"))
		}
		for _, key := range keys {
			data, err := s.GetClusterSecret(ctx, cluster.Ref(), key)
			if err != nil {
				failures = errors.Join(failures, errors.New("cannot inspect cluster certificate"))
				continue
			}
			add(domain.NormalizedNamespace(cluster.Namespace), cluster.Name, key, data)
		}
	}
	return out, failures
}
