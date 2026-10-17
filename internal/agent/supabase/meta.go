// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"crypto/subtle"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

// postgres-meta also opens an admin listener at PG_META_PORT + 1 (8082).
// Keep Studio's bridge distinct even when native components share a host.
const studioMetaBridgeAddress = "127.0.0.1:8083"

// privateHandler authenticates a private ingress, or injects the service
// credential on Studio's loopback-only outgoing metadata bridge.
func privateHandler(s sbspec.Spec, target *url.URL, injectCredential bool) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Internal hops must never traverse an environment proxy.
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Header.Del("apikey")
			if injectCredential {
				r.Out.Header.Set("apikey", s.ServiceRoleKey)
			}
		},
		ErrorLog: log.New(io.Discard, "", 0),
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "metadata service unavailable", http.StatusBadGateway)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !injectCredential {
			keys := r.Header.Values("apikey")
			valid := 0
			if len(keys) == 1 && keys[0] != "" {
				for _, key := range []string{s.ServiceRoleKey, s.ServiceAsymmetricJWT} {
					if key != "" {
						valid |= subtle.ConstantTimeCompare([]byte(keys[0]), []byte(key))
					}
				}
			}
			if valid != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	})
}

func startMetaBridge(s sbspec.Spec) (*http.Server, error) {
	network, address := privateHTTPListener(s.DatabaseIPv6, "8080")
	upstream := "127.0.0.1:8081"
	if s.Component == "studio" {
		network, address, upstream = "tcp4", studioMetaBridgeAddress, s.Upstreams["meta"]
	}
	if s.ServiceRoleKey == "" || upstream == "" {
		return nil, errors.New("metadata bridge requires service credentials and upstream")
	}
	target, err := url.Parse("http://" + upstream)
	if err != nil || target.Host == "" || target.User != nil || target.Path != "" || target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("invalid metadata bridge upstream")
	}
	return startHTTPProxy(network, address, privateHandler(s, target, s.Component == "studio"))
}

func startStudioIngress(s sbspec.Spec) (*http.Server, error) {
	if s.ServiceRoleKey == "" {
		return nil, errors.New("service credentials are required for Studio ingress")
	}
	target := &url.URL{Scheme: "http", Host: "127.0.0.1:3000"}
	network, address := privateHTTPListener(s.DatabaseIPv6, "3002")
	return startHTTPProxy(network, address, privateHandler(s, target, false))
}

// Go's tcp wildcard listener may be dual-stack even for 0.0.0.0. Select
// tcp4 explicitly on IPv4 backends; IPv6 keeps dual-stack loopback health.
func privateHTTPListener(ipv6 bool, port string) (string, string) {
	if ipv6 {
		return "tcp", net.JoinHostPort("::", port)
	}
	return "tcp4", net.JoinHostPort("0.0.0.0", port)
}

func startHTTPProxy(network, address string, handler http.Handler) (*http.Server, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Addr: listener.Addr().String(), Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute, ErrorLog: log.New(io.Discard, "", 0)}
	go func() { _ = server.Serve(listener) }()
	return server, nil
}
