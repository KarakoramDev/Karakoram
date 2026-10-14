// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/minio/minio-go/v7"
)

func TestFailureCategorizationAndSecretSafety(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason string
	}{
		{"access", minio.ErrorResponse{Code: "AccessDenied", Message: "private-credential-value"}, "access_denied"},
		{"key", minio.ErrorResponse{Code: "InvalidAccessKeyId", Message: "private-credential-value"}, "access_denied"},
		{"signature", minio.ErrorResponse{Code: "SignatureDoesNotMatch", Message: "private-credential-value"}, "access_denied"},
		{"bucket", minio.ErrorResponse{Code: "NoSuchBucket", Message: "private-credential-value"}, "bucket_not_found"},
		{"tls", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, "tls_error"},
		{"dns", &net.DNSError{Err: "private-credential-value", Name: "private-endpoint"}, "unreachable"},
		{"connect", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("private-credential-value")}, "unreachable"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"network-timeout", &net.DNSError{Err: "private-credential-value", IsTimeout: true}, "timeout"},
		{"unknown", errors.New("private-credential-value"), "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := probeError("put", tc.err)
			reason, message := Failure(err)
			if reason != tc.reason || message == "" || strings.Contains(message, "private-credential-value") || strings.Contains(err.Error(), "private-credential-value") {
				t.Fatalf("%s %s: %v", reason, message, err)
			}
			if !errors.Is(err, tc.err) {
				t.Fatal("underlying error not preserved")
			}
		})
	}
	reason, message := Failure(nil)
	if reason != "" || !strings.Contains(message, "verified") {
		t.Fatal(reason, message)
	}
}

func TestVerificationEndpointPolicyCategories(t *testing.T) {
	credentials := domain.StorageCredentials{AccessKeyID: "private-access", SecretAccessKey: "private-secret"}
	for _, endpoint := range []string{"https://user:private-secret@example.test", "https://example.test/path", "https://example.test?token=private-secret", "https://127.0.0.1", "https://10.0.0.1"} {
		err := Verify(t.Context(), domain.BackupStorage{Type: "s3", EndpointURL: endpoint, Bucket: "bucket", Region: "us-east-1", ForcePathStyle: true}, credentials)
		reason, message := Failure(err)
		if reason != "endpoint_not_allowed" || strings.Contains(message, "private-secret") || strings.Contains(message, "private-access") {
			t.Fatalf("%s: %s %s (%v)", endpoint, reason, message, err)
		}
	}
	endpoint, _ := url.Parse("https://example.test")
	_, err := endpointDialer(endpoint, "")(t.Context(), "tcp", "unrelated.test:443")
	reason, _ := Failure(probeError("put", err))
	if reason != "endpoint_not_allowed" {
		t.Fatal(reason, err)
	}
}

func TestVerifyHTTPUsesEndpointPolicy(t *testing.T) {
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", "storage.example.test:9000")
	for _, endpoint := range []string{"http://example.test", "http://storage-prg1.zerops.io", "http://storage.example.test:9000"} {
		// Cancellation prevents outbound requests while proving HTTP reaches the
		// verifier rather than being rejected solely for its transport scheme.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := Verify(ctx, domain.BackupStorage{Type: "s3", EndpointURL: endpoint, Bucket: "bucket", Region: "us-east-1", ForcePathStyle: true}, domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"})
		reason, _ := Failure(err)
		if !errors.Is(err, context.Canceled) || reason == "endpoint_not_allowed" {
			t.Fatalf("%s: %s: %v", endpoint, reason, err)
		}
	}
}
