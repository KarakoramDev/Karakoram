//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func TestPublicConnectRetryable(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"dial timeout", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}, true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"connection closed during handshake", io.EOF, true},
		{"truncated handshake", io.ErrUnexpectedEOF, true},
		{"driver network error", mongo.CommandError{Message: "connection reset", Labels: []string{"NetworkError"}}, true},
		{"server selection timeout", topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}, true},
		{"wrapped server selection timeout", fmt.Errorf("public connect: %w", topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}), true},
		{"authentication code", mongo.CommandError{Code: 18}, false},
		{"authorization code", mongo.CommandError{Code: 13}, false},
		{"authentication with network label", mongo.CommandError{Code: 18, Labels: []string{"NetworkError"}}, false},
		{"embedded authentication failure", fmt.Errorf("server selection: AuthenticationFailed: %w", context.DeadlineExceeded), false},
		{"embedded SCRAM failure", fmt.Errorf("unable to authenticate using mechanism SCRAM-SHA-256: %w", context.DeadlineExceeded), false},
		{"embedded certificate failure", fmt.Errorf("server selection: x509: certificate signed by unknown authority: %w", context.DeadlineExceeded), false},
		{"wrapped certificate verification", &net.OpError{Op: "dial", Net: "tcp", Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}, false},
		{"wrapped TLS record failure", &net.OpError{Op: "read", Net: "tcp", Err: tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}}, false},
		{"embedded TLS alert", fmt.Errorf("remote error: tls: bad certificate: %w", context.DeadlineExceeded), false},
		{"missing credentials", errors.New("connection omitted databaseAdmin credentials"), false},
		{"invalid CA", errors.New("invalid cluster CA"), false},
		{"other command failure", mongo.CommandError{Code: 2, Message: "bad value"}, false},
		{"cancelled parent", context.Canceled, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := publicConnectRetryable(test.err); got != test.want {
				t.Fatalf("retryable=%t want=%t for %v", got, test.want, test.err)
			}
		})
	}
}

func TestRetryPublicProbe(t *testing.T) {
	t.Run("network recovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		attempts := 0
		err := retryPublicProbe(ctx, func(attempt context.Context) error {
			attempts++
			deadline, ok := attempt.Deadline()
			if !ok || time.Until(deadline) > 20*time.Second {
				t.Fatal("probe attempt lacks a bounded deadline")
			}
			if attempts == 1 {
				return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
			}
			return nil
		})
		if err != nil || attempts != 2 {
			t.Fatalf("attempts=%d err=%v, want network failure then success", attempts, err)
		}
	})
	t.Run("terminal errors fail immediately", func(t *testing.T) {
		for _, failure := range []error{
			mongo.CommandError{Code: 18},
			&net.OpError{Op: "dial", Net: "tcp", Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}},
			mongo.CommandError{Code: 13},
		} {
			attempts := 0
			err := retryPublicProbe(context.Background(), func(context.Context) error {
				attempts++
				return failure
			})
			if err == nil || err.Error() != failure.Error() || attempts != 1 {
				t.Fatalf("attempts=%d err=%v, want immediate original failure %v", attempts, err, failure)
			}
		}
	})
	t.Run("caller deadline retains network reason", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		attempts := 0
		err := retryPublicProbe(ctx, func(context.Context) error {
			attempts++
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		})
		if attempts != 1 || !errors.Is(err, syscall.ECONNREFUSED) || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("attempts=%d err=%v, want caller deadline and original network reason", attempts, err)
		}
	})
	t.Run("cancelled caller makes no attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retryPublicProbe(ctx, func(context.Context) error {
			t.Fatal("probe attempted after caller cancellation")
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v want context cancellation", err)
		}
	})
}
