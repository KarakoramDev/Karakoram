// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nativeartifacts

import (
	"reflect"
	"strings"
	"testing"
)

func TestPerconaArtifacts(t *testing.T) {
	for _, name := range []string{"percona-server-8.4.11-11", "percona-server-8.4.10-10", "percona-server-8.0.46-37", "percona-server-8.0.45-36", "percona-xtrabackup-8.4.0-7", "percona-xtrabackup-8.0.35-36", "percona-xtradb-cluster-8.4.11-12.1", "percona-xtradb-cluster-8.4.11-11.1", "percona-xtradb-cluster-8.0.46-38.1"} {
		a, err := Get(name)
		if err != nil {
			t.Fatal(err)
		}
		runtimeDeps := []string{"libaio1t64", "libnuma1", "libev4t64"}
		if strings.HasPrefix(name, "percona-server-") {
			runtimeDeps = append(runtimeDeps, "libtirpc3t64")
		}
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

func TestPXCReplacementArtifactAndWithdrawnIdentity(t *testing.T) {
	const retained = `  {
    "name": "percona-xtradb-cluster-8.4.11-11.1",
    "version": "8.4.11-11.1",
    "file": "Percona-XtraDB-Cluster_8.4.11-11.1_Linux.x86_64.glibc2.35-minimal.tar.gz",
    "sha256": "739c4a5ff8a356760e370a1d487aa2587eabca31a452b22e8c320af69416f8b0",
    "size": 234385037,
    "upstreamURL": "https://downloads.percona.com/downloads/Percona-XtraDB-Cluster-84/Percona-XtraDB-Cluster-8.4.11/binary/tarball/Percona-XtraDB-Cluster_8.4.11-11.1_Linux.x86_64.glibc2.35-minimal.tar.gz",
    "license": "GPL-2.0-only",
    "source": {
      "repo": "https://github.com/percona/percona-xtradb-cluster",
      "commit": "33f7510189c5719c852837d7ad9ba5e7d6431f4d"
    },
    "build": "official-glibc2.35-minimal",
    "runtimeDeps": [
      "libaio1t64",
      "libnuma1",
      "libev4t64",
      "socat",
      "openssl"
    ]
  },`
	if !strings.Contains(string(lock), retained) {
		t.Fatal("withdrawn artifact identity must remain byte-for-byte unchanged")
	}
	a, err := Get("percona-xtradb-cluster-8.4.11-12.1")
	if err != nil {
		t.Fatal(err)
	}
	if a.Version != "8.4.11-12.1" || a.Size != 234499079 || a.SHA256 != "8ae35d537c467320fd16e6c5858a4701430923f01da8dc6a9a377e653e70bbc4" || a.Source.Commit != "dca75e8faed5348be08b159f7d5b1088427928dc" {
		t.Fatal("replacement provenance differs from verified upstream release", a)
	}
	script := prepareScript(t, PrepareCommands(a, a.UpstreamURL, "/opt/karakoram/"+a.Name, "runtime")[0])
	if !strings.Contains(script, a.UpstreamURL) || !strings.Contains(script, a.SHA256) {
		t.Fatal("replacement preparation lacks pinned download and checksum")
	}
}
