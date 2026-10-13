// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"fmt"
	"regexp"
	"strings"
)

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)
var resourceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var accountName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.@-]{0,63}$`)

// ValidateClusterName enforces the Zerops-safe 2–12 character cluster naming rule.
func ValidateClusterName(name string) error {
	if !clusterName.MatchString(name) {
		return fmt.Errorf("invalid cluster name %q: use 2–12 lowercase alphanumeric characters starting with a letter", name)
	}
	return nil
}

// ValidateName enforces lowercase DNS-style resource naming.
func ValidateName(name string) error {
	if !resourceName.MatchString(name) {
		return fmt.Errorf("invalid resource name %q", name)
	}
	return nil
}

// ValidateAccountName accepts bounded local or OIDC account identifiers.
func ValidateAccountName(name string) error {
	if !accountName.MatchString(name) {
		return fmt.Errorf("invalid account name %q", name)
	}
	return nil
}

// ValidateDatabaseUserName rejects empty, oversized, or control-character user names.
func ValidateDatabaseUserName(name string) error {
	if name == "" || len(name) > 128 || strings.ContainsAny(name, "\x00\r\n") {
		return fmt.Errorf("invalid database user name")
	}
	return nil
}
