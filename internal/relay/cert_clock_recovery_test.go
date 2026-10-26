// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package relay

import (
	"crypto/x509"
	"testing"
	"time"
)

func TestNeedsRenewalFutureLeaf(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		leaf *x509.Certificate
		want bool
	}{
		{"future", &x509.Certificate{NotBefore: now.Add(time.Second), NotAfter: now.Add(365 * 24 * time.Hour)}, true},
		{"current", &x509.Certificate{NotBefore: now, NotAfter: now.Add(365 * 24 * time.Hour)}, false},
		{"expired", &x509.Certificate{NotBefore: now.Add(-time.Hour), NotAfter: now}, true},
		{"missing", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NeedsRenewal(tc.leaf, now); got != tc.want {
				t.Fatalf("renew=%v want=%v", got, tc.want)
			}
		})
	}
}
