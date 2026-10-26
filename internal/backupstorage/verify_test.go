// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestVerifyProbeLifecycle(t *testing.T) {
	var payload []byte
	var paths []string
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		methods = append(methods, r.Method)
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			t.Error("S3 request was not signed")
		}
		switch r.Method {
		case "PUT":
			var err error
			payload, err = readProbeBody(r)
			if err != nil {
				t.Error(err)
			}
			w.Header().Set("ETag", `"probe"`)
		case "GET":
			w.Header().Set("ETag", `"probe"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			_, _ = w.Write(payload)
		case "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	storage := domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", Prefix: "nested/prefix/", ForcePathStyle: false, VerifyTLS: true}
	err := verifyLocal(t, storage, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(methods, ",") != "PUT,GET,DELETE" {
		t.Fatal(methods)
	}
	if len(paths) != 3 || paths[0] != paths[1] || paths[1] != paths[2] || !strings.HasPrefix(paths[0], "/test-bucket/nested/prefix/.karakoram-verify-") {
		t.Fatal(paths)
	}
}

func TestVerifyTLSChoice(t *testing.T) {
	var payload []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			payload, _ = readProbeBody(r)
			w.Header().Set("ETag", `"probe"`)
		case "GET":
			w.Header().Set("ETag", `"probe"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
			_, _ = w.Write(payload)
		case "DELETE":
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	storage := domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: false, VerifyTLS: true}
	creds := domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}
	if err := verifyLocal(t, storage, creds); err == nil || !strings.Contains(err.Error(), "TLS certificate verification") {
		t.Fatalf("TLS verification error: %v", err)
	}
	storage.VerifyTLS = false
	if err := verifyLocal(t, storage, creds); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyErrors(t *testing.T) {
	for _, item := range []struct {
		code    string
		status  int
		message string
	}{{"AccessDenied", 403, "authentication or permission"}, {"InvalidAccessKeyId", 403, "authentication or permission"}, {"NoSuchBucket", 404, "bucket does not exist"}, {"RequestTimeTooSkewed", 403, "control-plane host clock"}} {
		t.Run(item.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(item.status)
				_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>failure</Message></Error>", item.code)
			}))
			defer server.Close()
			err := verifyLocal(t, domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true}, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
			if err == nil || !strings.Contains(err.Error(), item.message) || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
}

func TestVerifyCleanupOnReadFailure(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			w.Header().Set("ETag", `"probe"`)
		case "GET":
			w.Header().Set("ETag", `"probe"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			_, _ = w.Write([]byte("wrong"))
		case "DELETE":
			deleted = true
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	err := verifyLocal(t, domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true}, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
	if err == nil || !deleted {
		t.Fatalf("error=%v deleted=%v", err, deleted)
	}
}

func TestVerifyValidation(t *testing.T) {
	for _, endpoint := range []string{"file:///tmp", "https://user:password@example.test", "https://example.test/path", "https://example.test?token=secret"} {
		if err := Verify(context.Background(), domain.BackupStorage{Type: "s3", EndpointURL: endpoint}, domain.StorageCredentials{}); err == nil {
			t.Fatal(endpoint)
		}
	}
}

// S3 streaming signatures wrap HTTP uploads in AWS chunks; the fake storage decodes that wire framing.
func readProbeBody(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") && !strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	reader := bufio.NewReader(r.Body)
	var body bytes.Buffer
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseInt(strings.Split(strings.TrimSpace(line), ";")[0], 16, 64)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return body.Bytes(), nil
		}
		if _, err := io.CopyN(&body, reader, size); err != nil {
			return nil, err
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return nil, err
		}
	}
}

// Probe lifecycle tests explicitly supply a local test transport; the production
// entry point must never allow loopback, even with an operator allowlist.
func verifyLocal(t *testing.T, storage domain.BackupStorage, credentials domain.StorageCredentials) error {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: !storage.VerifyTLS}
	defer transport.CloseIdleConnections()
	return verify(t.Context(), storage, credentials, noRedirectTransport{transport})
}

func TestEndpointAddressPolicy(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "::1", "0.0.0.0", "::", "169.254.169.254", "fe80::1", "224.0.0.1", "::ffff:127.0.0.1", "100.100.100.200", "fd00:ec2::254", "168.63.129.16", "192.0.0.192"} {
		for _, allowPrivate := range []bool{false, true} {
			if endpointAddressAllowed(netip.MustParseAddr(address), allowPrivate) {
				t.Fatalf("admitted forbidden address %s", address)
			}
		}
	}
	for _, address := range []string{"10.0.0.1", "192.168.1.1", "172.16.1.1", "fd00::1"} {
		if endpointAddressAllowed(netip.MustParseAddr(address), false) || !endpointAddressAllowed(netip.MustParseAddr(address), true) {
			t.Fatal("private endpoint policy", address)
		}
	}
	if !endpointAddressAllowed(netip.MustParseAddr("8.8.8.8"), false) {
		t.Fatal("public endpoints must be permitted by default")
	}
}

func TestEndpointAllowlistAndDialTimeRejection(t *testing.T) {
	if !endpointAllowlisted("storage-prg1.zerops.io", "443", "") || endpointAllowlisted("unrelated.zerops.io", "443", "") {
		t.Fatal("Zerops storage allowlist is not exact")
	}
	if !endpointAllowlisted("s3project", "9000", "other, s3project:9000") || endpointAllowlisted("s3project", "9001", "s3project:9000") {
		t.Fatal("operator allowlist lost the port")
	}
	for _, host := range []string{"127.0.0.1", "localhost", "::1", "169.254.169.254"} {
		endpoint := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, "80")}
		if conn, err := endpointDialer(endpoint, host)(t.Context(), "tcp", endpoint.Host); err == nil {
			conn.Close()
			t.Fatalf("dialed forbidden endpoint %s", host)
		}
	}
	endpoint, _ := url.Parse("https://example.test")
	if _, err := endpointDialer(endpoint, "")(t.Context(), "tcp", "unrelated.test:443"); err == nil {
		t.Fatal("admitted unrelated destination")
	}
}

func TestVerifyRejectsLoopbackAndRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	storage := domain.BackupStorage{Type: "s3", EndpointURL: source.URL, Bucket: "test-bucket", ForcePathStyle: true}
	credentials := domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}
	if err := Verify(t.Context(), storage, credentials); err == nil {
		t.Fatal("production verifier admitted loopback")
	}
	if err := verifyLocal(t, storage, credentials); err == nil || !strings.Contains(err.Error(), "redirects") || redirected {
		t.Fatalf("redirect followed: %v, target requested %v", err, redirected)
	}
}
