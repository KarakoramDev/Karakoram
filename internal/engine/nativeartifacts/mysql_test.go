// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nativeartifacts

import (
	"reflect"
	"strings"
	"testing"
)

func TestPerconaArtifacts(t *testing.T) {
	for _, name := range []string{"percona-server-8.4.11-11", "percona-server-8.4.10-10", "percona-server-8.0.46-37", "percona-server-8.0.45-36", "percona-xtrabackup-8.4.0-7", "percona-xtrabackup-8.0.35-36", "percona-xtradb-cluster-8.4.11-11.1", "percona-xtradb-cluster-8.0.46-38.1"} {
		a, err := Get(name)
		if err != nil {
			t.Fatal(err)
		}
		runtimeDeps := []string{"libaio1t64", "libnuma1", "libev4t64"}
		if strings.HasPrefix(name, "percona-xtradb-cluster-") {
			runtimeDeps = append(runtimeDeps, "socat", "openssl")
		}
		if a.License != "GPL-2.0-only" || !strings.HasSuffix(a.File, "glibc2.35-minimal.tar.gz") || !strings.HasPrefix(a.UpstreamURL, "https://downloads.percona.com/downloads/") || !reflect.DeepEqual(a.RuntimeDeps, runtimeDeps) {
			t.Fatalf("incorrect Percona artifact: %+v", a)
		}
		script := prepareScript(t, PrepareCommands(a, a.UpstreamURL, "/opt/karakoram/"+name, "runtime")[0])
		if !strings.Contains(script, "sha256sum -c -") || !strings.Contains(script, legacyPG17Validator) || !strings.Contains(script, "\"$file\" 'application'") || strings.Contains(script, "--strip-components") {
			t.Fatal("Percona archives must preserve the validated upstream layout")
		}
	}
}
