// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"strings"
	"testing"
)

func TestMySQLRecoveryCredentialNativeLength(t *testing.T) {
	for _, topology := range []string{"single", "group"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _ := setup(t, topology)
			rc.Cluster.Spec.Provider = "mysql"
			s := &state{}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			for _, name := range systemNames {
				value, err := secret(t.Context(), rc, "users/"+name)
				if err != nil {
					t.Fatal(err)
				}
				want := 40
				if name == "kk_repl" {
					want = 32 // MySQL START GROUP_REPLICATION maximum, error 3972.
				}
				if len(value) != want || strings.Trim(string(value), "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" {
					t.Fatalf("%s credential must contain %d alphanumeric characters", name, want)
				}
			}
			before, err := secret(t.Context(), rc, "users/kk_repl")
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			after, err := secret(t.Context(), rc, "users/kk_repl")
			if err != nil || string(before) != string(after) {
				t.Fatal("reconciliation changed the persisted recovery credential")
			}
		})
	}
}

func TestMySQLRecoveryCredentialDoesNotTruncateExistingAccount(t *testing.T) {
	p, rc, _ := setup(t, "group")
	rc.Cluster.Spec.Provider = "mysql"
	const existing = "existing-account-credential-40-characters!"
	if err := putSecret(t.Context(), rc, "users/kk_repl", []byte(existing)); err != nil {
		t.Fatal(err)
	}
	if err := p.ensureSecrets(t.Context(), rc, &state{}); err != nil {
		t.Fatal(err)
	}
	value, err := secret(t.Context(), rc, "users/kk_repl")
	if err != nil || string(value) != existing {
		t.Fatal("existing SQL account credential was silently changed")
	}
}
