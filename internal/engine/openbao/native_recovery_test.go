// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package openbao

import "testing"

// Run on the independently formed target before snapshot-force. The same real
// OpenBao processes must deny anonymous, ordinary-member and foreign-cluster
// clients at TLS while retaining recovery for the control-plane-only identity.
func (h *nativeCP) verifyNativeRecoveryTLS(t *testing.T) {
	t.Helper()
	h.publish(t)
	for _, m := range ordered(state(t, h.rc)) {
		assertRecoveryTLSBoundary(t, h.p, h.rc, m.Name)
	}
	t.Log("native recovery TLS: dedicated control-plane identity accepted; missing, ordinary-member and foreign-cluster identities denied; ordinary API legacy routes disabled")
}
