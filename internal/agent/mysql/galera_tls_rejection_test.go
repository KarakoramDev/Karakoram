// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

// A local failure to authenticate the server is not evidence that the server
// authenticated and rejected our client. crypto/tls marks received TLS alerts
// with net.OpError{Op:"remote error"}; accept only its closed certificate alerts.
func nativeRemoteCertificateRejection(err error) bool {
	var verification *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var roots x509.SystemRootsError
	if err == nil || errors.As(err, &verification) || errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &roots) {
		return false
	}
	for current := err; current != nil; current = errors.Unwrap(current) {
		remote, ok := current.(*net.OpError)
		if !ok || remote.Op != "remote error" || remote.Err == nil {
			continue
		}
		switch remote.Err.Error() {
		case "tls: bad certificate", "tls: unsupported certificate", "tls: revoked certificate", "tls: expired certificate", "tls: unknown certificate", "tls: unknown certificate authority", "tls: certificate required":
			return true
		}
	}
	return false
}

func TestNativeRemoteCertificateRejection(t *testing.T) {
	cert := &x509.Certificate{}
	local := x509.UnknownAuthorityError{Cert: cert}
	remote := &net.OpError{Op: "remote error", Err: tls.AlertError(48)}
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"remote unknown authority", remote, true},
		{"wrapped remote unknown authority", fmt.Errorf("handshake: %w", remote), true},
		{"remote bad certificate", &net.OpError{Op: "remote error", Err: tls.AlertError(42)}, true},
		{"remote certificate required", &net.OpError{Op: "remote error", Err: tls.AlertError(116)}, true},
		{"remote revoked certificate", &net.OpError{Op: "remote error", Err: tls.AlertError(44)}, true},
		{"remote expired certificate", &net.OpError{Op: "remote error", Err: tls.AlertError(45)}, true},
		{"local unknown authority", local, false},
		{"local TLS server verification", &tls.CertificateVerificationError{UnverifiedCertificates: []*x509.Certificate{cert}, Err: local}, false},
		{"local unknown authority in network error", &net.OpError{Op: "read", Err: local}, false},
		{"local verification inside remote wrapper", &net.OpError{Op: "remote error", Err: local}, false},
		{"local hostname mismatch", x509.HostnameError{Certificate: cert, Host: "wrong-host"}, false},
		{"local certificate invalid", x509.CertificateInvalidError{Cert: cert, Reason: x509.Expired}, false},
		{"certificate text without remote alert", errors.New("x509: certificate signed by unknown authority"), false},
		{"remote arbitrary certificate text", &net.OpError{Op: "remote error", Err: errors.New("application certificate check failed")}, false},
		{"remote generic handshake failure", &net.OpError{Op: "remote error", Err: tls.AlertError(40)}, false},
		{"remote EOF", &net.OpError{Op: "remote error", Err: io.EOF}, false},
		{"EOF", io.EOF, false},
		{"nil", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := nativeRemoteCertificateRejection(test.err); got != test.want {
				t.Fatalf("remote rejection=%v want=%v", got, test.want)
			}
		})
	}
}
