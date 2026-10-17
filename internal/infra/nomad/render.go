// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var execCapabilities = []string{"chown", "dac_override", "fowner", "fsetid", "setgid", "setuid", "setpcap", "sys_chroot", "audit_write"}
var execCapabilityBound = "--bounding-set=-all,+" + strings.Join(execCapabilities, ",+")

// renderJob returns the complete /v1/jobs request body. Environment values are
// deliberately absent: Deploy merges even Runtime.Env into the job variable so
// subsequent environment edits take effect on the next restart, not at render.
func (b *Backend) renderJob(ctx context.Context, r *record) (map[string]any, error) {
	if r.Deployment == nil {
		return nil, reject("runtime has no deployment")
	}
	volumeName := ""
	if r.Volume != "" {
		volume, err := b.require(ctx, r.Volume)
		if err != nil {
			return nil, err
		}
		if volume.VolumeName == "" {
			return nil, reject("volume has no physical name")
		}
		volumeName = volume.VolumeName
	}
	cores := r.Resources.MaxCPU
	if cores == 0 {
		cores = r.Resources.MinCPU
	}
	if cores == 0 {
		cores = 0.1
	}
	ram := r.Resources.MaxRAM
	if ram == 0 {
		ram = r.Resources.MinRAM
	}
	resources := map[string]any{"MemoryMB": max(32, int(math.Ceil(ram*1024)))}
	if r.Resources.CPUMode == infra.CPUDedicated {
		resources["Cores"] = int(cores)
	} else {
		mhz, err := b.cpuMHz(ctx, r.Node)
		if err != nil {
			return nil, err
		}
		resources["CPU"] = max(1, int(math.Ceil(cores*float64(mhz))))
	}
	count := r.Max
	if count < 1 {
		count = r.Min
	}
	if count < 1 || r.Volume != "" {
		count = 1
	}
	dcs := []string{"*"}
	if configured := b.target.Config["datacenters"]; configured != "" {
		dcs = strings.Split(configured, ",")
		for i := range dcs {
			dcs[i] = strings.TrimSpace(dcs[i])
		}
	}
	pool := b.target.Config["nodePool"]
	if pool == "" {
		pool = "default"
	}
	network := b.target.Config["network"]
	if network == "" {
		network = "karakoram"
	}
	meta := map[string]string{"karakoram.id": r.ID, "karakoram.kind": "runtime"}
	if owner, ok := r.Vars["KARAKORAM_OWNER"]; ok {
		meta["KARAKORAM_OWNER"] = owner
	}
	if r.Pending != nil {
		meta["karakoram.operation"] = r.Pending.Issued.Format(time.RFC3339Nano)
	}
	task := map[string]any{
		"Name": "member", "Driver": "exec", "User": "root",
		"Config": map[string]any{
			"command": "/local/karakoram-entrypoint", "cap_drop": []string{"all"}, "cap_add": execCapabilities,
		},
		"Artifacts": []any{map[string]any{
			"GetterSource":  r.Deployment.Source,
			"GetterOptions": map[string]string{"checksum": r.Deployment.Checksum, "archive": "tar.gz"},
			"RelativeDest":  "local/artifact",
		}},
		"Templates": []any{
			renderTemplate("local/karakoram-entrypoint", renderEntrypoint(r), "0755"),
			renderTemplate("secrets/karakoram.env", renderEnvTemplate(r.Hostname), "0600"),
			renderTemplate("etc/hosts", renderHostsTemplate(), "0644"),
		},
		"Resources":  resources,
		"KillSignal": "SIGTERM", "KillTimeout": int64(120 * time.Second),
	}
	net := map[string]any{"Mode": "cni/" + network}
	reserved := make([]any, 0, len(r.PublicPorts))
	for _, port := range r.PublicPorts {
		reserved = append(reserved, map[string]any{
			"Label": "public" + strconv.Itoa(port.Port), "Value": port.Port, "To": port.TargetPort,
		})
	}
	if len(reserved) != 0 {
		net["ReservedPorts"] = reserved
	}
	// Private listeners use the routable allocation IP directly. Declaring
	// even a dynamic agent port would publish it through CNI portmap.
	group := map[string]any{
		"Name": "member", "Count": count, "Networks": []any{net}, "Tasks": []any{task},
		// Native discovery must be available before application readiness: a
		// readiness check here would prevent distributed engines bootstrapping.
		// Nomad 2.0.7 supports a numeric alloc service port without a network
		// label; this registers discovery without reserving/publishing a port.
		"Services": []any{map[string]any{"Name": r.Hostname, "Provider": "nomad", "AddressMode": "alloc", "PortLabel": "9443"}},
		"Update": map[string]any{
			"MaxParallel": 1, "Canary": 0, "HealthCheck": "task_states",
			"MinHealthyTime": int64(time.Second), "HealthyDeadline": int64(15 * time.Minute),
			"ProgressDeadline": int64(20 * time.Minute), "AutoRevert": false,
		},
		"RestartPolicy": map[string]any{
			"Attempts": 2, "Interval": int64(30 * time.Minute), "Delay": int64(15 * time.Second), "Mode": "fail",
		},
		"ReschedulePolicy": map[string]any{
			"Attempts": 0, "Unlimited": true, "Delay": int64(30 * time.Second),
			"DelayFunction": "exponential", "MaxDelay": int64(time.Hour),
		},
	}
	if r.Volume != "" {
		// Sticky single-writer storage pins rescheduled allocations to the
		// original node; the lifetime flock serializes any overlapping writers.
		// Retry failed allocations there automatically rather than leaving a
		// recovered node's member permanently dead.
		group["Disconnect"] = map[string]any{
			"LostAfter": int64(12 * time.Hour),
			"Replace":   false, "Reconcile": "keep_original",
		}
		group["Volumes"] = map[string]any{"data": map[string]any{
			"Type": "host", "Source": volumeName, "ReadOnly": false, "Sticky": true,
			"AccessMode": "single-node-single-writer", "AttachmentMode": "file-system",
		}}
		task["VolumeMounts"] = []any{map[string]any{"Volume": "data", "Destination": layout.VolumePath(r.Volume), "ReadOnly": false}}
	} else {
		group["Disconnect"] = map[string]any{
			"LostAfter": int64(time.Minute),
			"Replace":   true, "Reconcile": "keep_replacement",
		}
		group["Spreads"] = []any{map[string]any{"Attribute": "${node.unique.id}", "Weight": 100}}
	}
	job := map[string]any{
		"ID": r.Hostname, "Name": r.Hostname, "Namespace": b.target.ID,
		"Type": "service", "Datacenters": dcs, "NodePool": pool,
		"Meta": meta, "TaskGroups": []any{group},
	}
	return map[string]any{"Job": job}, nil
}

func renderTemplate(destination, data, permissions string) map[string]any {
	return map[string]any{
		"DestPath": destination, "EmbeddedTmpl": data, "Perms": permissions,
		"ChangeMode": "noop", "LeftDelim": "{{", "RightDelim": "}}",
	}
}

// renderShellQuote is shared by every generated shell literal. Template values
// use the same single-quote escape, performed by Nomad when reading the variable.
func renderShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

// renderEntrypoint requires an operator-provisioned Ubuntu rootfs copied from a
// separate read-only filesystem. Default exec host hardlinks are unsafe for
// package installation or sudo permission changes; see the Nomad operator docs.
func renderEntrypoint(r *record) string {
	d := r.Deployment
	var script strings.Builder
	script.WriteString("#!/bin/sh\nset -eu\n")
	// Nomad 2.0.7 accepts the capability configuration but does not actually
	// bound a root exec task. Apply the kernel bound before any prepare work.
	fmt.Fprintf(&script, "if [ \"$#\" -eq 0 ]; then\n    exec /usr/bin/setpriv %s \"$0\" bounded\nfi\n", renderShellQuote(execCapabilityBound))
	script.WriteString("export DEBIAN_FRONTEND=noninteractive\n")
	if r.Volume != "" {
		// Single-writer volume claims do not fence processes after a Nomad
		// node-loss reconciliation. Hold this descriptor across the final exec,
		// before prepare or ownership changes can touch the mounted volume.
		fmt.Fprintf(&script, "exec 9> %s\nif ! flock -n 9; then\n    printf '%%s\\n' 'waiting for previous writer' >&2\n    flock 9\nfi\n", renderShellQuote(path.Join(layout.VolumePath(r.Volume), ".karakoram-writer.lock")))
	}
	script.WriteString("self_wait=0\nwhile ! getent hosts \"$NOMAD_JOB_ID\" >/dev/null; do\n    if [ \"$self_wait\" -ge 120 ]; then\n        printf '%s\\n' 'warning: own Nomad service hostname did not resolve within 120 seconds; continuing' >&2\n        break\n    fi\n    sleep 1\n    self_wait=$((self_wait + 1))\ndone\n")
	script.WriteString("getent group karakoram >/dev/null || groupadd --gid 10001 karakoram\n")
	script.WriteString("id karakoram >/dev/null 2>&1 || useradd --uid 10001 --gid 10001 --create-home --shell /bin/bash karakoram\n")
	script.WriteString("test \"$(id -u karakoram)\" = 10001\ntest \"$(id -g karakoram)\" = 10001\n")
	script.WriteString("mkdir -p /etc/sudoers.d\nprintf '%s\\n' 'karakoram ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/karakoram\nchmod 0440 /etc/sudoers.d/karakoram\n")
	// Exec's rootfs copy strips setuid bits. Restore sudo only inside the
	// allocation's isolated rootfs so prepare retains the neutral sudo contract.
	script.WriteString("chmod 4755 /usr/bin/sudo\n")
	fmt.Fprintf(&script, "mkdir -p %s\nchown 10001:10001 %s\ncd %s\n", renderShellQuote(layout.AppDir), renderShellQuote(layout.AppDir), renderShellQuote(layout.AppDir))
	// Hash the encoded list, not concatenated commands: command boundaries matter.
	prepare, _ := json.Marshal(d.Runtime.Prepare)
	digest := sha256.Sum256(prepare)
	marker := fmt.Sprintf("/local/.karakoram-prepare-%x", digest)
	fmt.Fprintf(&script, "if [ ! -f %s ]; then\n", renderShellQuote(marker))
	for i, command := range d.Runtime.Prepare {
		file := fmt.Sprintf("/local/karakoram-prepare-%d.sh", i)
		// Base64 also prevents arbitrary engine commands from becoming Nomad
		// template instructions (including literal {{ ... }} in engine configs).
		fmt.Fprintf(&script, "    printf '%%s' %s | base64 -d > %s\n    chmod 0644 %s\n    sudo -u %s -- /bin/bash -e %s\n", renderShellQuote(base64.StdEncoding.EncodeToString([]byte(command))), renderShellQuote(file), renderShellQuote(file), renderShellQuote(layout.User), renderShellQuote(file))
	}
	fmt.Fprintf(&script, "    touch %s\nfi\n", renderShellQuote(marker))
	binary := path.Join(layout.AppDir, d.Binary)
	fmt.Fprintf(&script, "install -m 0755 -o 10001 -g 10001 %s %s\n", renderShellQuote(path.Join("/local/artifact", d.Binary)), renderShellQuote(binary))
	// Sudo is a prepare-only contract. Restore it on each start above, then
	// revoke privilege escalation before secrets or the runtime are available.
	script.WriteString("rm -f /etc/sudoers.d/karakoram\nchmod 0755 /usr/bin/sudo\n")
	if r.Volume != "" {
		volume := renderShellQuote(layout.VolumePath(r.Volume))
		fmt.Fprintf(&script, "mkdir -p %s\nif [ \"$(stat -c '%%u:%%g' %s)\" != '10001:10001' ]; then\n    chown 10001:10001 %s\nfi\n", volume, volume, volume)
	}
	script.WriteString(". /secrets/karakoram.env\nexec setpriv --reuid=10001 --regid=10001 --init-groups -- ")
	script.WriteString(renderShellQuote(binary))
	for _, arg := range d.Runtime.Args {
		script.WriteByte(' ')
		script.WriteString(renderShellQuote(arg))
	}
	script.WriteByte('\n')
	return script.String()
}

func renderEnvTemplate(hostname string) string {
	variable := strconv.Quote("nomad/jobs/" + hostname)
	return "{{ if nomadVarExists " + variable + " }}{{ with nomadVar " + variable + " }}{{ range .Tuples }}export {{ .K }}='{{ .V | replaceAll \"'\" \"'\\\\''\" }}'\n{{ end }}{{ end }}{{ end }}"
}

func renderHostsTemplate() string {
	return "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n{{ range nomadServices }}{{ $name := .Name }}{{ range nomadService $name }}{{ .Address }} {{ $name }}\n{{ end }}{{ end }}"
}
