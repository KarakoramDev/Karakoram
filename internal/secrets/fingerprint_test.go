// SPDX-License-Identifier: Apache-2.0
package secrets

import "testing"

func TestFingerprintVector(t *testing.T) {
	s, err := New("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Fingerprint(), "kkfp1:51e334a0116c3239ca06153448957f11"; got != want {
		t.Fatalf("Fingerprint()=%q, want %q", got, want)
	}
}
