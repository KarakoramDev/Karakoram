//go:build !integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestSQLDataListenerAddressFamilies(t *testing.T) {
	for _, tc := range []struct{ host, bind string }{
		{"", "0.0.0.0"}, {"0.0.0.0", "0.0.0.0"}, {"::", "*"},
	} {
		for _, s := range []myspec.Spec{coreTestSpec(), testGroupSpec(), testGaleraSpec()} {
			s.ListenHost = tc.host
			text := renderConfig(s, dataPaths(t.TempDir()))
			if !strings.Contains(text, "bind_address="+optionQuote(tc.bind)+"\n") {
				t.Errorf("topology=%s host=%q missing bind_address=%s", s.Topology, tc.host, tc.bind)
			}
		}
	}
	s := coreTestSpec()
	s.ListenHost = "127.0.0.1"
	if err := s.Validate(); err == nil {
		t.Fatal("accepted unsupported SQL listener policy")
	}
}

func TestProxySQLFrontendAddressFamilies(t *testing.T) {
	for _, host := range []string{"", "0.0.0.0", "::"} {
		for _, reader := range []bool{false, true} {
			s := testProxySpec()
			s.ListenHost = host
			s.Proxy.ReaderPort = reader
			text, err := RenderProxyConfig(s, "/config", "/data")
			if err != nil {
				t.Fatal(err)
			}
			want := "0.0.0.0:3306"
			if host == "::" {
				want += ";[::]:3306"
			}
			if reader {
				want += ";0.0.0.0:3307"
				if host == "::" {
					want += ";[::]:3307"
				}
			}
			if !strings.Contains(text, " interfaces=\""+want+"\"") {
				t.Errorf("host=%q reader=%t missing %q", host, reader, want)
			}
			if !strings.Contains(text, `mysql_ifaces="127.0.0.1:6032"`) {
				t.Fatal("admin interface no longer loopback-only")
			}
		}
	}
}
