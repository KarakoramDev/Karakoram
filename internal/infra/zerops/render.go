// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/hoststombstones"
	"gopkg.in/yaml.v3"
)

var zeropsLayout = infra.Layout{AppDir: "/var/www", VolumeRoot: "/mnt", User: "zerops", PrivateAddressFamily: infra.AddressFamilyIPv6}

// New-build deployments do not advertise ProxyDeploymentPreservesServing:
// temporaryShutdown=false permits overlap, but neither serial replacement nor
// serving-probe admission before traffic/predecessor retirement is established.
var zeropsCapabilities = infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Logs: true, ScopedTokens: true, DedicatedCPU: true, HorizontalScaling: true, ReplicaScalePreservesServing: true, ResourceUpdatePreservesServing: true}

// RenderYAML translates a neutral runtime description into a Zerops deployment.
func RenderYAML(hostname string, layout infra.Layout, binary string, spec infra.RuntimeSpec) (string, error) {
	base, err := serviceType(infra.KindRuntime, spec.Base)
	if err != nil {
		return "", err
	}
	ports := make([]map[string]any, 0, len(spec.Ports))
	for _, p := range spec.Ports {
		port := map[string]any{"port": p.Port, "protocol": p.Protocol}
		if p.HTTP {
			port["httpSupport"] = true
		}
		ports = append(ports, port)
	}
	// Stateless replacements may overlap; shutting all predecessors down first
	// defeats rolling availability. Volume writers retain the explicit fence.
	// Relays also remain singleton: a second live session evicts the first,
	// whose reconnect can then evict the replacement.
	singleton := spec.Volume != "" || (len(spec.Args) > 0 && spec.Args[0] == "relay")
	deploy := map[string]any{"temporaryShutdown": spec.StopBeforeStart && singleton}
	if r := spec.Readiness; r != nil {
		deploy["readinessCheck"] = map[string]any{
			"exec":           map[string]any{"command": path.Join(layout.AppDir, binary) + " " + strings.Join(r.Args, " ")},
			"failureTimeout": fmt.Sprintf("%ds", r.FailureTimeout/time.Second),
			"retryPeriod":    fmt.Sprintf("%ds", r.RetryPeriod/time.Second),
		}
	}
	env := spec.Env
	prepare := spec.Prepare
	if spec.Env["KARAKORAM_ENGINE"] == "mysql" {
		// Do not mutate the caller's runtime specification or backing slices.
		env = make(map[string]string, len(spec.Env)+1)
		for key, value := range spec.Env {
			env[key] = value
		}
		env["KARAKORAM_RESOLVER_TOMBSTONES"] = "hosts-helper:/usr/local/libexec/karakoram-hosts-tombstones"
		prepare = append(append([]string(nil), spec.Prepare...), prepareHostsTombstones())
	}
	run := map[string]any{"base": base, "ports": ports, "envVariables": env, "start": "./" + binary + " " + strings.Join(spec.Args, " ")}
	if len(prepare) > 0 {
		run["prepareCommands"] = prepare
	}
	if spec.Volume != "" {
		run["volume"] = map[string]any{"hostname": spec.Volume}
	}
	data, err := yaml.Marshal(map[string]any{"zerops": []any{map[string]any{"setup": hostname, "deploy": deploy, "run": run}}})
	return string(data), err
}

// The quoted heredoc carries the same fixed helper used by Dokploy. All target
// paths and policy text are generated constants, never runtime input.
func prepareHostsTombstones() string {
	return "/usr/bin/sudo -n /bin/sh <<'KARAKORAM_HOSTS_INSTALL'\n" +
		"set -eu\n" +
		"PATH=/usr/sbin:/usr/bin:/sbin:/bin\nexport PATH\numask 077\n" +
		"helper=/usr/local/libexec/karakoram-hosts-tombstones\n" +
		"policy=/etc/sudoers.d/karakoram-hosts-tombstones\n" +
		"helperStage= policyStage= helperBackup= activated=0 committed=0 hadHelper=0\n" +
		"cleanup() {\n" +
		"    status=$?\n" +
		"    trap - EXIT HUP INT TERM\n" +
		"    if [ \"$activated\" -eq 1 ] && [ \"$committed\" -eq 0 ]; then\n" +
		"        if [ \"$hadHelper\" -eq 1 ]; then\n" +
		"            /usr/bin/mv -f -- \"$helperBackup\" \"$helper\" || status=1\n" +
		"        else\n" +
		"            /usr/bin/rm -f -- \"$helper\" || status=1\n" +
		"        fi\n" +
		"    fi\n" +
		"    /usr/bin/rm -f -- \"$helperStage\" \"$policyStage\" \"$helperBackup\" || status=1\n" +
		"    exit \"$status\"\n" +
		"}\n" +
		"trap cleanup EXIT\ntrap 'exit 1' HUP INT TERM\n" +
		"/usr/bin/install -d -o root -g root -m 0755 /usr/local/libexec\n" +
		"helperStage=$(/usr/bin/mktemp /usr/local/libexec/.karakoram-hosts-tombstones.XXXXXXXX)\n" +
		"policyStage=$(/usr/bin/mktemp /etc/sudoers.d/.karakoram-hosts-tombstones.XXXXXXXX)\n" +
		"/usr/bin/cat > \"$helperStage\" <<'KARAKORAM_HOSTS_HELPER'\n" +
		hoststombstones.Script +
		"KARAKORAM_HOSTS_HELPER\n" +
		"/usr/bin/chown root:root \"$helperStage\"\n" +
		"/usr/bin/chmod 0755 \"$helperStage\"\n" +
		"/usr/bin/cat > \"$policyStage\" <<'KARAKORAM_HOSTS_POLICY'\n" +
		"Defaults:zerops !setenv\n" +
		"zerops ALL=(root) NOPASSWD: NOSETENV: /usr/local/libexec/karakoram-hosts-tombstones \"\"\n" +
		"KARAKORAM_HOSTS_POLICY\n" +
		"/usr/bin/chown root:root \"$policyStage\"\n" +
		"/usr/bin/chmod 0440 \"$policyStage\"\n" +
		"/usr/sbin/visudo -cf \"$policyStage\"\n" +
		"if [ -e \"$helper\" ]; then\n" +
		"    helperBackup=$(/usr/bin/mktemp /usr/local/libexec/.karakoram-hosts-backup.XXXXXXXX)\n" +
		"    /usr/bin/cp -p -- \"$helper\" \"$helperBackup\"\n" +
		"    hadHelper=1\n" +
		"fi\n" +
		"activated=1\n" +
		"/usr/bin/mv -f -- \"$helperStage\" \"$helper\"\n" +
		"/usr/bin/mv -f -- \"$policyStage\" \"$policy\"\n" +
		"committed=1\n" +
		"KARAKORAM_HOSTS_INSTALL\n"
}
