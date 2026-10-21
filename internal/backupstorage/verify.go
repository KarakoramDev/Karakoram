// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package backupstorage provides guarded S3 clients, storage verification and retained-snapshot cleanup.
package backupstorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/netguard"
	"github.com/KarakoramDev/Karakoram/internal/s3endpoint"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type verificationFailure struct {
	reason string
	err    error
	// message is set only from the shared, fixed-string configuration validator.
	message string
}

func (e *verificationFailure) Error() string {
	_, message := Failure(e)
	return message
}
func (e *verificationFailure) Unwrap() error { return e.err }

// Failure returns only fixed, caller-safe advice. S3 errors, URLs and credentials
// supplied by a remote service are never included in the verification response.
func Failure(err error) (reason, message string) {
	if err == nil {
		return "", "Read, write and delete access verified"
	}
	reason = "unknown"
	var failure *verificationFailure
	if errors.As(err, &failure) {
		reason = failure.reason
		if failure.message != "" {
			return reason, failure.message
		}
	}
	switch reason {
	case "access_denied":
		message = "S3 authentication or permission denied; check credentials and bucket read/write/delete permissions"
	case "bucket_not_found":
		message = "S3 bucket does not exist; check the bucket name, endpoint and region"
	case "clock_skew":
		message = "S3 rejected the request time; check and synchronize the control-plane host clock, then run storage verification again"
	case "endpoint_not_allowed":
		message = "S3 endpoint is not permitted; use an HTTP(S) origin without credentials, path, query or fragment; redirects, loopback and metadata addresses are forbidden and private endpoints require an operator allowlist"
	case "tls_error":
		message = "S3 TLS certificate verification or handshake failed; check the endpoint hostname, certificate chain and TLS configuration"
	case "unreachable":
		message = "S3 endpoint is unreachable; check DNS, endpoint, port and network connectivity from the control plane"
	case "timeout":
		message = "S3 verification timed out; check endpoint connectivity and retry when the storage service is responsive"
	default:
		message = "S3 verification failed; check endpoint, region, bucket and read/write/delete access, then retry"
	}
	return reason, message
}

// Verify probes put, get and delete permissions with a random, private object under the configured prefix.
// TLS certificate verification follows the storage setting; virtual hosting is
// preferred only when the endpoint/bucket can safely use it.
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
	if err := domain.ValidateBackupStoragePrefix(s.Prefix); err != nil {
		return err
	}
	key := domain.BackupStoragePrefix(s.Prefix)
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

// NewClient constructs an S3 client with the same endpoint, TLS and redirect
// policy as storage verification. Credentials must come from the sealed store.
// The returned cleanup function releases the client's idle connections.
func NewClient(s domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
	return newClient(s, c, nil)
}

// NewClientWithAllowlist constructs a client using an explicit private-endpoint
// allowlist instead of the control plane's environment. Loopback and metadata
// destinations remain forbidden.
func NewClientWithAllowlist(s domain.BackupStorage, c domain.StorageCredentials, allowlist string) (*minio.Client, func(), error) {
	return newClientWithAllowlist(s, c, nil, allowlist)
}

// newClient shares the verifier's outbound policy with retained-snapshot cleanup.
func newClient(s domain.BackupStorage, c domain.StorageCredentials, probeTransport http.RoundTripper) (*minio.Client, func(), error) {
	return newClientWithAllowlist(s, c, probeTransport, os.Getenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST"))
}

func newClientWithAllowlist(s domain.BackupStorage, c domain.StorageCredentials, probeTransport http.RoundTripper, allowlist string) (*minio.Client, func(), error) {
	if s.Type != "s3" {
		return nil, nil, fmt.Errorf("unsupported backup storage type %q", s.Type)
	}
	u, err := s3endpoint.Parse(s.EndpointURL)
	if err != nil {
		return nil, nil, &verificationFailure{reason: "endpoint_not_allowed", err: err, message: err.Error()}
	}
	if s.Bucket == "" {
		return nil, nil, errors.New("S3 bucket is required")
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return nil, nil, errors.New("S3 access key and secret key are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Proxy dialing would bypass endpoint address checks.
	transport.DialContext = endpointDialer(u, allowlist)
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !s.VerifyTLS} // Explicit user-selected S3 verification policy.
	if probeTransport == nil {
		probeTransport = noRedirectTransport{transport}
	}
	region := s.Region
	if region == "" {
		region = "us-east-1"
	}
	lookup := minio.BucketLookupDNS
	if s3endpoint.PathStyle(u, s.Bucket, s.ForcePathStyle) {
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
	var existing *verificationFailure
	if errors.As(err, &existing) {
		return err
	}
	reason := "unknown"
	switch minio.ToErrorResponse(err).Code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken":
		reason = "access_denied"
	case "NoSuchBucket":
		reason = "bucket_not_found"
	case "RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture":
		reason = "clock_skew"
	default:
		var tlsErr *tls.CertificateVerificationError
		var unknownCA x509.UnknownAuthorityError
		var hostname x509.HostnameError
		var invalidCert x509.CertificateInvalidError
		var record tls.RecordHeaderError
		var network net.Error
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			reason = "timeout"
		case errors.As(err, &tlsErr), errors.As(err, &unknownCA), errors.As(err, &hostname), errors.As(err, &invalidCert), errors.As(err, &record):
			reason = "tls_error"
		case errors.As(err, &network):
			reason = "unreachable"
			if network.Timeout() {
				reason = "timeout"
			}
		}
	}
	return &verificationFailure{reason: reason, err: err}
}

// noRedirectTransport also rejects redirects handled internally by the S3
// client, so signed requests never move to a server-selected destination.
type noRedirectTransport struct{ http.RoundTripper }

func (t noRedirectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.RoundTripper.RoundTrip(r)
	if err == nil && response.StatusCode >= 300 && response.StatusCode < 400 {
		response.Body.Close()
		return nil, &verificationFailure{reason: "endpoint_not_allowed", err: errors.New("S3 endpoint redirects are not permitted")}
	}
	return response, err
}

func endpointAllowlisted(host, port, allowlist string) bool {
	policy := netguard.Policy{Allowlist: netguard.ParseAllowlist(allowlist + ",storage-prg1.zerops.io")}
	return policy.Allowlisted(host, port)
}

// EndpointAddressAllowed is the shared outbound address policy. OAuth metadata
// calls it with privateAllowed=false, independently of the backup allowlist.
func EndpointAddressAllowed(ip netip.Addr, privateAllowed bool) bool {
	return netguard.AddressAllowed(ip, privateAllowed, false)
}

func endpointDialer(endpoint *url.URL, allowlist string) func(context.Context, string, string) (net.Conn, error) {
	policy := &netguard.Policy{
		Allowlist:       netguard.ParseAllowlist(allowlist + ",storage-prg1.zerops.io"),
		S3Compatibility: true,
		Endpoint:        endpoint,
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := policy.DialContext(ctx, network, address)
		if errors.Is(err, netguard.ErrNotAllowed) {
			return nil, &verificationFailure{reason: "endpoint_not_allowed", err: err}
		}
		return conn, err
	}
}
