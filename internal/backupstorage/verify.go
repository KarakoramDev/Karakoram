// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package backupstorage verifies S3-compatible backup destinations without exposing credentials.
package backupstorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Verify probes put, get and delete permissions with a random, private object under the configured prefix.
// TLS certificate verification and path-style addressing follow the storage settings exactly.
func Verify(ctx context.Context, s domain.BackupStorage, c domain.StorageCredentials) error {
	return verify(ctx, s, c, nil)
}

func verify(ctx context.Context, s domain.BackupStorage, c domain.StorageCredentials, probeTransport http.RoundTripper) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, closeClient, err := newClient(s, c, probeTransport)
	if err != nil {
		return err
	}
	defer closeClient()
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return fmt.Errorf("generate S3 probe name: %w", err)
	}
	key := strings.TrimRight(s.Prefix, "/")
	if key != "" {
		key += "/"
	}
	key += ".karakoram-verify-" + hex.EncodeToString(random[:])
	payload := []byte("Karakoram backup storage verification\n")
	_, err = client.PutObject(ctx, s.Bucket, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	if err != nil {
		return probeError("put", err)
	}
	// Cleanup is attempted even if reading is cancelled; never leave a probe after a successful write without reporting it.
	obj, getErr := client.GetObject(ctx, s.Bucket, key, minio.GetObjectOptions{})
	if getErr == nil {
		var got []byte
		got, getErr = io.ReadAll(io.LimitReader(obj, int64(len(payload)+1)))
		closeErr := obj.Close()
		if getErr == nil {
			getErr = closeErr
		}
		if getErr == nil && !bytes.Equal(got, payload) {
			getErr = errors.New("S3 probe read returned different object contents")
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	deleteErr := client.RemoveObject(cleanupCtx, s.Bucket, key, minio.RemoveObjectOptions{})
	if getErr != nil {
		if deleteErr != nil {
			return errors.Join(probeError("get", getErr), probeError("delete probe "+key, deleteErr))
		}
		return probeError("get", getErr)
	}
	if deleteErr != nil {
		return probeError("delete probe "+key, deleteErr)
	}
	return nil
}

// newClient shares the verifier's outbound policy with retained-snapshot cleanup.
func newClient(s domain.BackupStorage, c domain.StorageCredentials, probeTransport http.RoundTripper) (*minio.Client, func(), error) {
	if s.Type != "s3" {
		return nil, nil, fmt.Errorf("unsupported backup storage type %q", s.Type)
	}
	u, err := url.Parse(s.EndpointURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, nil, errors.New("S3 endpoint must be an HTTP(S) origin without credentials, path, query or fragment")
	}
	if s.Bucket == "" {
		return nil, nil, errors.New("S3 bucket is required")
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return nil, nil, errors.New("S3 access key and secret key are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Proxy dialing would bypass endpoint address checks.
	transport.DialContext = endpointDialer(u, os.Getenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST"))
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !s.VerifyTLS} // Explicit user-selected S3 verification policy.
	if probeTransport == nil {
		probeTransport = noRedirectTransport{transport}
	}
	region := s.Region
	if region == "" {
		region = "us-east-1"
	}
	lookup := minio.BucketLookupDNS
	if s.ForcePathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Secure: u.Scheme == "https", Region: region, Transport: probeTransport, BucketLookup: lookup})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, fmt.Errorf("configure S3 client: %w", err)
	}
	return client, transport.CloseIdleConnections, nil
}

func probeError(action string, err error) error {
	code := minio.ToErrorResponse(err).Code
	switch code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken":
		return fmt.Errorf("S3 %s: authentication or permission denied (%s)", action, code)
	case "NoSuchBucket":
		return fmt.Errorf("S3 %s: bucket does not exist", action)
	}
	var tlsErr *tls.CertificateVerificationError
	if errors.As(err, &tlsErr) {
		return fmt.Errorf("S3 %s: TLS certificate verification failed: %w", action, err)
	}
	return fmt.Errorf("S3 %s failed: %w", action, err)
}

// noRedirectTransport also rejects redirects handled internally by the S3
// client, so signed requests never move to a server-selected destination.
type noRedirectTransport struct{ http.RoundTripper }

func (t noRedirectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(r)
	if err == nil && response.StatusCode >= 300 && response.StatusCode < 400 {
		response.Body.Close()
		return nil, errors.New("S3 endpoint redirects are not permitted")
	}
	return response, err
}

func endpointAllowlisted(host, port, allowlist string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "storage-prg1.zerops.io" && (port == "80" || port == "443") {
		return true
	}
	for _, entry := range strings.Split(allowlist, ",") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == net.JoinHostPort(host, port) || (strings.TrimSuffix(entry, ".") == host && (port == "80" || port == "443")) {
			return true
		}
	}
	return false
}

var (
	sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")
	metadataAWS        = netip.MustParseAddr("fd00:ec2::254")
	metadataAzure      = netip.MustParseAddr("168.63.129.16")
	metadataOracle     = netip.MustParseAddr("192.0.0.192")
)

func endpointAddressAllowed(ip netip.Addr, privateAllowed bool) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return false
	}
	// Metadata service addresses are forbidden even for an approved private
	// endpoint; not every provider uses a link-local metadata address.
	if sharedAddressSpace.Contains(ip) || ip == metadataAWS || ip == metadataAzure || ip == metadataOracle {
		return false
	}
	return !ip.IsPrivate() || privateAllowed
}

func endpointDialer(endpoint *url.URL, allowlist string) func(context.Context, string, string) (net.Conn, error) {
	endpointHost := strings.ToLower(strings.TrimSuffix(endpoint.Hostname(), "."))
	endpointPort := endpoint.Port()
	if endpointPort == "" {
		endpointPort = "80"
		if endpoint.Scheme == "https" {
			endpointPort = "443"
		}
	}
	approved := endpointAllowlisted(endpointHost, endpointPort, allowlist)
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		host = strings.ToLower(strings.TrimSuffix(host, "."))
		if port != endpointPort || (host != endpointHost && !strings.HasSuffix(host, "."+endpointHost)) || (!approved && port != "80" && port != "443") {
			return nil, errors.New("S3 endpoint destination or port is not permitted")
		}
		// Resolve on every new connection and dial the checked numeric address,
		// preventing DNS rebinding between policy validation and connection.
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			return nil, errors.New("S3 endpoint has no addresses")
		}
		for _, ip := range addresses {
			if !endpointAddressAllowed(ip, approved) {
				return nil, errors.New("S3 endpoint address is not permitted")
			}
		}
		var dialErr error
		for _, ip := range addresses {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			dialErr = err
		}
		return nil, dialErr
	}
}
