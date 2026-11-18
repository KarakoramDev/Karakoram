// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package tombstone

import (
	"fmt"
	"path/filepath"
	"strings"
)

// FromEnv selects the provider's locally acknowledged resolver projection.
func FromEnv(getenv func(string) string) (Writer, error) {
	value := getenv("KARAKORAM_RESOLVER_TOMBSTONES")
	if value == "" {
		return nil, nil
	}
	if value == "nomad-variable:" {
		return NewNomadVariableWriter("/etc/hosts"), nil
	}
	kind, path, ok := strings.Cut(value, ":")
	if !ok || kind != "hosts-helper" || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
		return nil, fmt.Errorf("invalid resolver tombstone writer configuration")
	}
	return &hostsHelper{path: path, run: runHelper}, nil
}

func hostInput(hosts []string) (string, error) {
	if len(hosts) > 64 {
		return "", fmt.Errorf("too many resolution tombstones (maximum 64)")
	}
	var b strings.Builder
	for _, host := range hosts {
		if !ValidHost(host) {
			return "", fmt.Errorf("invalid retired hostname %q", host)
		}
		b.WriteString(host)
		b.WriteByte('\n')
	}
	return b.String(), nil
}
