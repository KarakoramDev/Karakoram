// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

func localGet(ctx context.Context, s wvspec.MemberSpec, c agentapi.MemberConfig, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tr := &http.Transport{Proxy: nil, TLSClientConfig: readinessTLSConfig(s, c)}
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	scheme := "http"
	if s.TLS {
		scheme = "https"
	}
	var last error
	for _, host := range []string{"127.0.0.1", "::1"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+net.JoinHostPort(host, strconv.Itoa(s.Ports.HTTP))+path, nil)
		req.Header.Set("Authorization", "Bearer "+s.AllowedKeys[0])
		resp, err := client.Do(req)
		if err != nil {
			last = errors.New("native endpoint unavailable")
			continue
		}
		data, e := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		resp.Body.Close()
		if e != nil || len(data) > 1<<20 || resp.StatusCode != 200 {
			return nil, errors.New("native endpoint not ready")
		}
		return data, nil
	}
	return nil, last
}

func readinessTLSConfig(s wvspec.MemberSpec, c agentapi.MemberConfig) *tls.Config {
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(c.TLS.CAPEM))
	config := &tls.Config{RootCAs: roots, ServerName: s.NodeName, MinVersion: tls.VersionTLS12}
	block, _ := pem.Decode([]byte(c.TLS.CertPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return config
	}
	pinned, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !time.Now().After(pinned.NotAfter) {
		return config
	}
	// Only the member's exact configured expired leaf may be observed at a
	// historical in-validity time so its replacement can be orchestrated.
	// VerifyConnection replaces the default verification in full: the CA
	// chain, server-auth usage, and logical member hostname remain mandatory.
	config.InsecureSkipVerify = true
	config.VerifyConnection = func(connection tls.ConnectionState) error {
		if len(connection.PeerCertificates) == 0 {
			return errors.New("native TLS certificate unavailable")
		}
		leaf := connection.PeerCertificates[0]
		currentTime := time.Now()
		if currentTime.After(leaf.NotAfter) && bytes.Equal(leaf.Raw, pinned.Raw) {
			currentTime = pinned.NotBefore.Add(pinned.NotAfter.Sub(pinned.NotBefore) / 2)
		}
		intermediates := x509.NewCertPool()
		for _, certificate := range connection.PeerCertificates[1:] {
			intermediates.AddCert(certificate)
		}
		_, err := leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates, DNSName: s.NodeName,
			CurrentTime: currentTime, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		return err
	}
	return config
}
func nativeVersion(ctx context.Context, s wvspec.MemberSpec, c agentapi.MemberConfig) (string, error) {
	data, err := localGet(ctx, s, c, "/v1/meta")
	if err != nil {
		return "", err
	}
	var meta struct {
		Version string `json:"version"`
	}
	if json.Unmarshal(data, &meta) != nil || len(meta.Version) > 64 || !strings.HasPrefix(meta.Version, "1.") {
		return "", errors.New("native version unavailable")
	}
	return meta.Version, nil
}
func held(c agentapi.MemberConfig) bool {
	return c.Maintenance || c.DesiredProcesses != nil && !slices.Contains(c.DesiredProcesses, wvspec.Process)
}
func (*Engine) Readiness(ctx context.Context, o agent.Options, args []string) error {
	dir := o.DataDir
	for _, arg := range args {
		value, ok := strings.CutPrefix(arg, "--data-dir=")
		if !ok || !filepath.IsAbs(value) {
			return errors.New("readiness check for Weaviate accepts only --data-dir=<absolute path>")
		}
		dir = value
	}
	if dir == "" {
		dir = os.Getenv("KARAKORAM_DATA_DIR")
	}
	if dir == "" {
		return errors.New("data directory required")
	}
	data, err := os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var controls struct {
		Maintenance bool            `json:"maintenance"`
		Stopped     map[string]bool `json:"stopped"`
	}
	if len(data) > 0 && json.Unmarshal(data, &controls) != nil {
		return errors.New("invalid controls")
	}
	if controls.Maintenance || controls.Stopped[wvspec.Process] {
		return nil
	}
	data, err = os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
	if err != nil {
		return err
	}
	var c agentapi.MemberConfig
	if json.Unmarshal(data, &c) != nil {
		return errors.New("invalid cached configuration")
	}
	if held(c) {
		return nil
	}
	s, err := spec(c)
	if err != nil {
		return err
	}
	_, err = localGet(ctx, s, c, "/v1/.well-known/ready")
	return err
}
func (e *Engine) Routes(mux *http.ServeMux, n *agent.Node) {
	mux.HandleFunc("POST /v1/weaviate/readiness", func(w http.ResponseWriter, r *http.Request) {
		if err := decodeReadiness(w, r); err != nil {
			http.Error(w, "invalid readiness request", http.StatusBadRequest)
			return
		}
		e.mu.RLock()
		c, ok := e.config, e.applied
		e.mu.RUnlock()
		result := wvspec.ReadinessResponse{ConfigVersion: c.ConfigVersion}
		switch {
		case !ok:
			result.Message = "Configuration has not been applied"
		case n.Maintenance() || n.Stopped(wvspec.Process) || held(c):
			result.Message = "Process is held"
		default:
			s, err := spec(c)
			if err == nil {
				_, err = localGet(r.Context(), s, c, "/v1/.well-known/ready")
			}
			if err == nil {
				result.Version, err = nativeVersion(r.Context(), s, c)
			}
			if err != nil {
				result.Message = "Native readiness unavailable"
			} else {
				result.Ready = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}
func decodeReadiness(w http.ResponseWriter, r *http.Request) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	d.DisallowUnknownFields()
	var req *wvspec.ReadinessRequest
	if err := d.Decode(&req); err != nil {
		return err
	}
	if req == nil {
		return errors.New("expected an object")
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("expected one object")
	}
	return nil
}
