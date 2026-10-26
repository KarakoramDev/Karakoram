// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var diagnosticURL = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://\S+`)
var jobIndexConflict = regexp.MustCompile(`^Enforcing job modify index [0-9]+: (?:job already exists|job exists with conflicting job modify index: [0-9]+|job does not exist)$`)

// APIError contains bounded, credential-silent diagnostics from the Nomad API.
type APIError struct {
	Status  int
	Op      string
	Message string
}

func (e *APIError) Error() string { return e.SafeDetail() }
func (e *APIError) SafeDetail() string {
	detail := "nomad"
	if e.Status != 0 {
		detail += fmt.Sprintf(" HTTP %d", e.Status)
	}
	if e.Op != "" {
		detail += " " + sanitizedMessage(e.Op, "")
	}
	if e.Message != "" {
		detail += ": " + sanitizedMessage(e.Message, "")
	}
	return boundedDetail(detail)
}
func (e *APIError) Is(target error) bool { return target == e.neutral() }
func (e *APIError) neutral() error {
	switch {
	case e.Status == 401 || e.Status == 403 || (e.Status == 400 && strings.Contains(strings.ToLower(e.Message), "acl token not found")):
		return infra.ErrUnauthorized
	case e.Status == 404:
		return infra.ErrNotFound
	// Nomad reports job registration's EnforceIndex conflicts as uncoded RPC
	// errors with HTTP 500 rather than HTTP 409.
	case e.Status == 409 || (e.Status == 500 && jobIndexConflict.MatchString(strings.TrimSpace(e.Message))):
		return infra.ErrConflict
	case e.Status == 400 || e.Status == 422:
		return infra.ErrRejected
	default:
		return infra.ErrUnavailable
	}
}
func (e *APIError) Unwrap() error {
	if e.neutral() == infra.ErrRejected {
		return &infra.RejectedError{Code: "nomadRejected", Message: e.SafeDetail()}
	}
	return e.neutral()
}

func boundedDetail(s string) string {
	if len(s) <= 300 {
		return s
	}
	s = s[:297]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s + "..."
}

func sanitizedMessage(message, token string) string {
	if token != "" {
		message = strings.ReplaceAll(message, token, "[redacted]")
	}
	message = diagnosticURL.ReplaceAllString(message, "[URL]")
	return boundedDetail(strings.Join(strings.Fields(message), " "))
}

func responseError(status int, op string, body []byte, token string) error {
	return &APIError{Status: status, Op: op, Message: sanitizedMessage(string(body), token)}
}

func silentFailure(message string) error { return &APIError{Message: message} }

type tlsConfigError struct {
	message string
}

func (e *tlsConfigError) Error() string      { return e.SafeDetail() }
func (e *tlsConfigError) SafeDetail() string { return e.message }
func (e *tlsConfigError) Unwrap() error      { return infra.ErrConfiguration }

func tlsConfigurationError(message string) error {
	return &tlsConfigError{message: boundedDetail("nomad " + message)}
}

func transportError(err error) error {
	var hostname x509.HostnameError
	var authority x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var verification *tls.CertificateVerificationError
	var alert tls.AlertError
	message := ""
	switch {
	case errors.As(err, &hostname):
		message = "TLS verification failed: certificate is not valid for the requested server name; check KARAKORAM_NOMAD_TLS_SERVER_NAME and the target certificate SANs"
	case errors.As(err, &authority):
		message = "TLS verification failed: unknown certificate authority; check KARAKORAM_NOMAD_CACERT and the target certificate chain"
	case errors.As(err, &invalid):
		message = "TLS verification failed: invalid certificate; check the target certificate and KARAKORAM_NOMAD_CACERT"
		if invalid.Reason == x509.Expired {
			message = "TLS verification failed: certificate has expired or is not yet valid; check the target certificate validity and system clock"
		}
	case errors.As(err, &verification):
		message = "TLS verification failed; check KARAKORAM_NOMAD_CACERT, KARAKORAM_NOMAD_TLS_SERVER_NAME and the target certificate"
	case errors.As(err, &alert) && (alert == 42 || alert == 43 || alert == 44 || alert == 45 || alert == 46 || alert == 48 || alert == 116):
		message = "TLS client certificate rejected; check KARAKORAM_NOMAD_CLIENT_CERT and KARAKORAM_NOMAD_CLIENT_KEY and the server's client CA trust"
	default:
		// TCP TLS alerts use an unexported alert type, unlike QUIC's AlertError.
		// Match only known remote certificate alerts, never echo transport text.
		for cause := err; cause != nil; cause = errors.Unwrap(cause) {
			switch cause.Error() {
			case "remote error: tls: bad certificate", "remote error: tls: unsupported certificate", "remote error: tls: revoked certificate", "remote error: tls: expired certificate", "remote error: tls: unknown certificate", "remote error: tls: unknown certificate authority", "remote error: tls: certificate required":
				message = "TLS client certificate rejected; check KARAKORAM_NOMAD_CLIENT_CERT and KARAKORAM_NOMAD_CLIENT_KEY and the server's client CA trust"
			}
			if message != "" {
				break
			}
		}
	}
	if message != "" {
		return tlsConfigurationError(message)
	}
	return silentFailure("network request failed")
}
