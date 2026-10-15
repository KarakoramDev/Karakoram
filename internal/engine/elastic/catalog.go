// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

var catalog = []domain.VersionInfo{
	{Version: "9.5.5", Major: "9.5", Default: true, Status: "supported"},
	{Version: "9.5.4", Major: "9.5", Status: "supported"},
	{Version: "9.4.8", Major: "9.4", Status: "supported"},
	{Version: "8.19.23", Major: "8.19", Status: "supported"},
}

var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func parseVersion(v string) ([3]int, bool) {
	var n [3]int
	if !versionPattern.MatchString(v) {
		return n, false
	}
	for i, s := range strings.Split(v, ".") {
		var err error
		n[i], err = strconv.Atoi(s)
		if err != nil {
			return n, false
		}
	}
	return n, true
}

// UpgradeAllowed accepts strictly newer releases of the same major, or the
// supported 8.19.x to 9.1+ bridge. Cross-major upgrades require a fresh backup.
// It describes compatibility, not membership in the offered version catalog.
func UpgradeAllowed(from, to string) (ok bool, requiresBackup bool) {
	a, aOK := parseVersion(from)
	b, bOK := parseVersion(to)
	if !aOK || !bOK {
		return false, false
	}
	newer := false
	for i := range a {
		if a[i] != b[i] {
			newer = b[i] > a[i]
			break
		}
	}
	if !newer {
		return false, false
	}
	if a[0] == b[0] {
		return true, false
	}
	if a[0] == 8 && a[1] == 19 && b[0] == 9 && b[1] >= 1 {
		return true, true
	}
	return false, false
}

func catalogUpgradePaths() []domain.UpgradePath {
	var paths []domain.UpgradePath
	for _, from := range catalog {
		for _, to := range catalog {
			if ok, backup := UpgradeAllowed(from.Version, to.Version); ok {
				paths = append(paths, domain.UpgradePath{From: from.Version, To: to.Version, RequiresBackup: backup})
			}
		}
	}
	return paths
}

// ArtifactURL returns the official Linux x86_64 archive URL. Invalid product or
// version text returns an empty URL and is never embedded in shell commands.
func ArtifactURL(component, version string) string {
	if _, ok := parseVersion(version); !ok {
		return ""
	}
	switch component {
	case agentspec.ComponentElasticsearch, agentspec.ComponentKibana, agentspec.ComponentLogstash, agentspec.ComponentAPM:
	default:
		return ""
	}
	product := agentspec.Product(component)
	return fmt.Sprintf("https://artifacts.elastic.co/downloads/%s/%s-%s-linux-x86_64.tar.gz", product, product, version)
}

func prepareCommands(component, version string) []string {
	url := ArtifactURL(component, version)
	if url == "" {
		return nil
	}
	file := url[strings.LastIndex(url, "/")+1:]
	return []string{fmt.Sprintf("sudo mkdir -p /opt/elastic && cd /tmp && curl -fsSLO %[1]s && curl -fsSLO %[1]s.sha512 && sha512sum -c %[2]s.sha512 && sudo tar -xzf %[2]s -C /opt/elastic && sudo chown -R zerops:zerops /opt/elastic && rm -f %[2]s %[2]s.sha512", url, file)}
}
