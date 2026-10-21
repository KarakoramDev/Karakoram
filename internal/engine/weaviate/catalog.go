// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"regexp"
	"strconv"
	"strings"
)

var catalog = []domain.VersionInfo{{Version: "1.40.1", Major: "1.40", Default: true, Status: "supported"}, {Version: "1.39.12", Major: "1.39", Status: "supported"}}
var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func parseVersion(v string) ([3]int, bool) {
	var n [3]int
	if !versionPattern.MatchString(v) {
		return n, false
	}
	for i, s := range strings.Split(v, ".") {
		x, e := strconv.Atoi(s)
		if e != nil {
			return n, false
		}
		n[i] = x
	}
	return n, true
}
func UpgradeAllowed(from, to string) bool {
	a, ok := parseVersion(from)
	b, bok := parseVersion(to)
	return ok && bok && a[0] == b[0] && (a[1] == b[1] && b[2] >= a[2] || b[1] == a[1]+1)
}
func catalogUpgradePaths() []domain.UpgradePath {
	var out []domain.UpgradePath
	for _, a := range catalog {
		for _, b := range catalog {
			if a.Version != b.Version && UpgradeAllowed(a.Version, b.Version) {
				out = append(out, domain.UpgradePath{From: a.Version, To: b.Version})
			}
		}
	}
	return out
}
func ArtifactName(version string) string {
	for _, v := range catalog {
		if v.Version == version {
			return "weaviate-" + v.Major
		}
	}
	return ""
}
func PrepareCommands(version string, layout infra.Layout) ([]string, error) {
	name := ArtifactName(version)
	if name == "" {
		return nil, fmt.Errorf("unsupported Weaviate version %q", version)
	}
	a, err := nativeartifacts.Get(name)
	if err != nil {
		return nil, err
	}
	if a.Version != version {
		return nil, fmt.Errorf("artifact %s version %s does not match %s", name, a.Version, version)
	}
	u, err := nativeartifacts.ResolveURL(a)
	if err != nil {
		return nil, err
	}
	commands := []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl python3"}
	return append(commands, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram/weaviate-"+version, layout.User)...), nil
}
