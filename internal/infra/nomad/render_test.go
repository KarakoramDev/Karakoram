// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"text/template"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func renderFixture(t *testing.T) (*Backend, *record) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/nodes":
			fmt.Fprint(w, `[{"ID":"node-a","NodePool":"default","Datacenter":"dc1","Status":"ready","SchedulingEligibility":"eligible"}]`)
		case "/v1/node/node-a":
			fmt.Fprint(w, `{"ID":"node-a","Attributes":{"cpu.totalcompute":"9600","cpu.numcores":"4"}}`)
		case "/v1/var/karakoram/services/examplem0vol":
			fmt.Fprint(w, `{"Path":"karakoram/services/examplem0vol","ModifyIndex":1,"Items":{"record":"{\"ID\":\"volume-id\",\"Hostname\":\"examplem0vol\",\"Kind\":\"volume\",\"VolumeName\":\"kk-a1b2c3d4-examplem0vol\"}"}}`)
		default:
			t.Errorf("unexpected render request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	backend, err := NewFactory(WithAllowedAddrs(server.URL)).New(infra.Target{
		Kind: "nomad", ID: "example", Token: "test-token",
		Config: map[string]string{"address": server.URL, "network": "routed", "datacenters": "dc1, dc2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return backend.(*Backend), &record{
		ID: "runtime-id", Hostname: "examplem0", Kind: infra.KindRuntime,
		Volume: "examplem0vol", VolumeID: "volume-id", Node: "node-a", Min: 1, Max: 5,
		Vars:        map[string]string{"KARAKORAM_OWNER": "owner-id", "VISIBLE": "nonsecret-variable-value"},
		SecretKeys:  []string{"TOKEN", "CERT"},
		Resources:   infra.Resources{MinCPU: 0.25, MaxCPU: 1.5, MinRAM: 0.125, MaxRAM: 0.5},
		PublicPorts: []infra.PublicPort{{Port: 37017, TargetPort: 27017, Protocol: "tcp"}},
		Deployment: &storedDeployment{
			Source: "https://artifacts.example.com/karakoram.tar.gz", Checksum: "sha256:" + strings.Repeat("a", 64), Binary: "karakoram",
			Runtime: infra.RuntimeSpec{
				Base: infra.BaseUbuntu2404, Volume: "examplem0vol", StopBeforeStart: true,
				Args: []string{"agent", "--mode=test"}, Prepare: []string{"printf '%s\\n' 'prepare one'", "sudo mkdir -p /opt/engine && echo ready"},
				Env:       map[string]string{"DEPLOYMENT_ENV": "deployment-environment-value"},
				Ports:     []infra.Port{{Port: 27017, Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}},
				Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}, RetryPeriod: time.Second, FailureTimeout: time.Minute},
			},
		},
	}
}

func renderJSONEqual(t *testing.T, got any, want string) {
	t.Helper()
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var actual, expected any
	if err := json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("JSON mismatch\ngot:  %s\nwant: %s", data, want)
	}
}

func renderParts(t *testing.T, b *Backend, r *record) (map[string]any, map[string]any, map[string]any) {
	t.Helper()
	body, err := b.renderJob(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	job := body["Job"].(map[string]any)
	group := job["TaskGroups"].([]any)[0].(map[string]any)
	task := group["Tasks"].([]any)[0].(map[string]any)
	return job, group, task
}

// This fixture asserts the portable wire fields from the lab volume-member
// reference, with private topology, artifact mirror and identities replaced.
func TestRenderVolumeMemberReference(t *testing.T) {
	b, r := renderFixture(t)
	job, group, task := renderParts(t, b, r)
	for key, want := range map[string]string{
		"ID": `"examplem0"`, "Name": `"examplem0"`, "Namespace": `"example"`, "Type": `"service"`,
		"Datacenters": `["dc1","dc2"]`, "NodePool": `"default"`,
		"Meta": `{"KARAKORAM_OWNER":"owner-id","karakoram.id":"runtime-id","karakoram.kind":"runtime"}`,
	} {
		t.Run(key, func(t *testing.T) { renderJSONEqual(t, job[key], want) })
	}
	renderJSONEqual(t, group["Count"], `1`)
	renderJSONEqual(t, group["Volumes"], `{"data":{"Type":"host","Source":"kk-a1b2c3d4-examplem0vol","ReadOnly":false,"Sticky":true,"AccessMode":"single-node-single-writer","AttachmentMode":"file-system"}}`)
	renderJSONEqual(t, task["VolumeMounts"], `[{"Volume":"data","Destination":"/var/lib/karakoram/volumes/examplem0vol","ReadOnly":false}]`)
	renderJSONEqual(t, group["Networks"], `[{"Mode":"cni/routed","ReservedPorts":[{"Label":"public37017","Value":37017,"To":27017}]}]`)
	renderJSONEqual(t, group["Services"], `[{"Name":"examplem0","Provider":"nomad","AddressMode":"alloc","PortLabel":"9443"}]`)
	renderJSONEqual(t, group["Update"], `{"MaxParallel":1,"Canary":0,"HealthCheck":"task_states","MinHealthyTime":1000000000,"HealthyDeadline":900000000000,"ProgressDeadline":1200000000000,"AutoRevert":false}`)
	renderJSONEqual(t, group["RestartPolicy"], `{"Attempts":2,"Interval":1800000000000,"Delay":15000000000,"Mode":"fail"}`)
	renderJSONEqual(t, group["ReschedulePolicy"], `{"Attempts":0,"Unlimited":true,"Delay":30000000000,"DelayFunction":"exponential","MaxDelay":3600000000000}`)
	renderJSONEqual(t, group["Disconnect"], `{"LostAfter":43200000000000,"Replace":false,"Reconcile":"keep_original"}`)
	renderJSONEqual(t, task["Config"], `{"command":"/local/karakoram-entrypoint","cap_drop":["all"],"cap_add":["chown","dac_override","fowner","fsetid","setgid","setuid","setpcap","sys_chroot","audit_write"]}`)
	renderJSONEqual(t, task["Artifacts"], `[{"GetterSource":"https://artifacts.example.com/karakoram.tar.gz","GetterOptions":{"checksum":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","archive":"tar.gz"},"RelativeDest":"local/artifact"}]`)
	renderJSONEqual(t, task["Resources"], `{"CPU":3600,"MemoryMB":512}`)
	if task["Driver"] != "exec" || task["User"] != "root" || task["KillSignal"] != "SIGTERM" || task["KillTimeout"] != int64(120*time.Second) {
		t.Fatalf("task execution settings: %#v", task)
	}
	if _, ok := group["Spreads"]; ok {
		t.Fatal("volume member must not spread")
	}
	for _, scope := range []map[string]any{job, group, task} {
		if _, ok := scope["Checks"]; ok {
			t.Fatal("application readiness must not gate native service discovery")
		}
	}
}

func TestRenderStatelessAndResourceFallbacks(t *testing.T) {
	b, r := renderFixture(t)
	r.Volume, r.Node = "", ""
	r.Deployment.Runtime.Volume = ""
	r.Resources = infra.Resources{MinCPU: 0.1001, MinRAM: 0.0625}
	r.Max = 3
	_, group, task := renderParts(t, b, r)
	renderJSONEqual(t, group["Count"], `3`)
	renderJSONEqual(t, group["Spreads"], `[{"Attribute":"${node.unique.id}","Weight":100}]`)
	renderJSONEqual(t, task["Resources"], `{"CPU":241,"MemoryMB":64}`)
	renderJSONEqual(t, group["ReschedulePolicy"], `{"Attempts":0,"Unlimited":true,"Delay":30000000000,"DelayFunction":"exponential","MaxDelay":3600000000000}`)
	renderJSONEqual(t, group["Disconnect"], `{"LostAfter":60000000000,"Replace":true,"Reconcile":"keep_replacement"}`)
	if group["Volumes"] != nil || task["VolumeMounts"] != nil {
		t.Fatal("stateless member has a volume")
	}
	if script := renderEntrypoint(r); strings.Contains(script, "flock") || strings.Contains(script, ".karakoram-writer.lock") {
		t.Fatal("stateless entrypoint has a writer fence")
	}
	delete(b.target.Config, "datacenters")
	delete(b.target.Config, "network")
	r.Min, r.Max = 2, 0
	r.Resources = infra.Resources{}
	var job map[string]any
	job, group, task = renderParts(t, b, r)
	renderJSONEqual(t, job["Datacenters"], `["*"]`)
	renderJSONEqual(t, group["Count"], `2`)
	// Deliberately bypassing factory defaults above also leaves node discovery
	// with no datacenter filter; its documented fallback is 1000 MHz per core.
	renderJSONEqual(t, task["Resources"], `{"CPU":100,"MemoryMB":32}`)
	network := group["Networks"].([]any)[0].(map[string]any)
	if network["Mode"] != "cni/karakoram" {
		t.Fatalf("default network = %v", network["Mode"])
	}
	r.Min = 0
	_, group, _ = renderParts(t, b, r)
	renderJSONEqual(t, group["Count"], `1`)
}

func TestRenderDedicatedCPU(t *testing.T) {
	b, r := renderFixture(t)
	r.Volume, r.Node = "", ""
	r.Deployment.Runtime.Volume = ""
	r.Resources = infra.Resources{CPUMode: infra.CPUDedicated, MinCPU: 1, MaxCPU: 2, MaxRAM: 0.5}
	// Dedicated placement uses Nomad's core reservation, without querying a
	// node frequency or emitting an incompatible MHz reservation alongside it.
	b.c = nil
	_, _, task := renderParts(t, b, r)
	renderJSONEqual(t, task["Resources"], `{"Cores":2,"MemoryMB":512}`)
	r.Resources.MaxCPU, r.Resources.MinCPU = 0, 3
	_, _, task = renderParts(t, b, r)
	renderJSONEqual(t, task["Resources"], `{"Cores":3,"MemoryMB":512}`)
}

func TestRenderEnvironmentNeverEmbedded(t *testing.T) {
	b, r := renderFixture(t)
	r.Deployment.Runtime.Env["TOKEN"] = "secret-token-value-never-in-json"
	r.Deployment.Runtime.Env["CERT"] = "secret-cert-value-never-in-json"
	body, err := b.renderJob(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"secret-token-value-never-in-json", "secret-cert-value-never-in-json", "deployment-environment-value", "nonsecret-variable-value"} {
		if bytes.Contains(data, []byte(value)) {
			t.Fatalf("job JSON embeds environment value %q", value)
		}
	}
}

func TestRenderTemplatesAndEntrypoint(t *testing.T) {
	b, r := renderFixture(t)
	_, _, task := renderParts(t, b, r)
	templates := task["Templates"].([]any)
	if len(templates) != 3 {
		t.Fatalf("templates = %d", len(templates))
	}
	for i, expected := range []struct{ dest, perms string }{{"local/karakoram-entrypoint", "0755"}, {"secrets/karakoram.env", "0600"}, {"etc/hosts", "0644"}} {
		item := templates[i].(map[string]any)
		if item["DestPath"] != expected.dest || item["Perms"] != expected.perms || item["ChangeMode"] != "noop" || item["LeftDelim"] != "{{" || item["RightDelim"] != "}}" {
			t.Fatalf("template %d settings = %#v", i, item)
		}
	}
	script := templates[0].(map[string]any)["EmbeddedTmpl"].(string)
	for _, fragment := range []string{
		"#!/bin/sh\nset -eu\n", "groupadd --gid 10001 karakoram", "useradd --uid 10001 --gid 10001", "karakoram ALL=(ALL) NOPASSWD:ALL",
		"exec /usr/bin/setpriv '--bounding-set=-all,+chown,+dac_override,+fowner,+fsetid,+setgid,+setuid,+setpcap,+sys_chroot,+audit_write' \"$0\" bounded",
		"chmod 4755 /usr/bin/sudo", "export DEBIAN_FRONTEND=noninteractive",
		"cd '/opt/karakoram'", "if [ ! -f '/local/.karakoram-prepare-", "sudo -u 'karakoram' -- /bin/bash -e '/local/karakoram-prepare-0.sh'",
		"install -m 0755 -o 10001 -g 10001 '/local/artifact/karakoram' '/opt/karakoram/karakoram'",
		"mkdir -p '/var/lib/karakoram/volumes/examplem0vol'", "chown 10001:10001 '/var/lib/karakoram/volumes/examplem0vol'",
		". /secrets/karakoram.env\nexec setpriv --reuid=10001 --regid=10001 --init-groups -- '/opt/karakoram/karakoram' 'agent' '--mode=test'\n",
	} {
		if !strings.Contains(script, fragment) {
			t.Errorf("entrypoint missing %q", fragment)
		}
	}
	first := strings.Index(script, renderShellQuote(base64.StdEncoding.EncodeToString([]byte(r.Deployment.Runtime.Prepare[0]))))
	second := strings.Index(script, renderShellQuote(base64.StdEncoding.EncodeToString([]byte(r.Deployment.Runtime.Prepare[1]))))
	if first < 0 || second <= first || strings.Index(script, "touch '/local/.karakoram-prepare-") <= second {
		t.Fatal("prepare commands or success marker are out of order")
	}
	changed := *r.Deployment
	changed.Runtime.Prepare = []string{"different prepare"}
	copy := *r
	copy.Deployment = &changed
	marker := strings.SplitN(script[strings.Index(script, "touch "):], "\n", 2)[0]
	if strings.Contains(renderEntrypoint(&copy), marker) {
		t.Fatal("prepare hash did not change")
	}
	renderJSONEqual(t, templates[1].(map[string]any)["EmbeddedTmpl"], `"{{ if nomadVarExists \"nomad/jobs/examplem0\" }}{{ with nomadVar \"nomad/jobs/examplem0\" }}{{ range .Tuples }}export {{ .K }}='{{ .V | replaceAll \"'\" \"'\\\\''\" }}'\n{{ end }}{{ end }}{{ end }}"`)
	renderJSONEqual(t, templates[2].(map[string]any)["EmbeddedTmpl"], `"127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n{{ range nomadServices }}{{ $name := .Name }}{{ range nomadService $name }}{{ .Address }} {{ $name }}\n{{ end }}{{ end }}"`)
}

func TestRenderEntrypointPrivilegeAndDiscoveryOrder(t *testing.T) {
	_, r := renderFixture(t)
	script := renderEntrypoint(r)
	last := -1
	for _, fragment := range []string{
		"exec /usr/bin/setpriv", "flock 9", "while ! getent hosts \"$NOMAD_JOB_ID\"",
		"chmod 4755 /usr/bin/sudo", "sudo -u 'karakoram'",
		"install -m 0755", "rm -f /etc/sudoers.d/karakoram",
		"chmod 0755 /usr/bin/sudo", ". /secrets/karakoram.env",
		"exec setpriv --reuid=10001",
	} {
		index := strings.Index(script, fragment)
		if index <= last {
			t.Fatalf("entrypoint order for %q: %d after %d", fragment, index, last)
		}
		last = index
	}
}

func TestRenderEntrypointSelfDiscoveryWait(t *testing.T) {
	_, r := renderFixture(t)
	script := renderEntrypoint(r)
	start := strings.Index(script, "self_wait=0\n")
	end := start + strings.Index(script[start:], "\ndone\n") + len("\ndone\n")
	for _, tc := range []struct {
		name                    string
		failures, calls, sleeps int
		warning                 bool
	}{
		{"already registered", 0, 1, 0, false},
		{"converges", 3, 4, 3, false},
		{"times out and continues", 121, 121, 120, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prelude := fmt.Sprintf(`set -eu
NOMAD_JOB_ID=examplem0
calls=0
sleeps=0
getent() {
    test "$1" = hosts && test "$2" = "$NOMAD_JOB_ID"
    calls=$((calls + 1))
    test "$calls" -gt %d
}
sleep() {
    test "$1" = 1
    sleeps=$((sleeps + 1))
}
`, tc.failures)
			cmd := exec.Command("/bin/sh", "-c", prelude+script[start:end]+`printf '%s %s continued' "$calls" "$sleeps"`)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			got, err := cmd.Output()
			want := fmt.Sprintf("%d %d continued", tc.calls, tc.sleeps)
			if err != nil || string(got) != want || strings.Contains(stderr.String(), "warning:") != tc.warning {
				t.Fatalf("wait = %q stderr=%q err=%v; want %q warning=%v", got, stderr.String(), err, want, tc.warning)
			}
		})
	}
}

func TestRenderEntrypointGolden(t *testing.T) {
	_, r := renderFixture(t)
	script := renderEntrypoint(r)
	prepare, err := json.Marshal(r.Deployment.Runtime.Prepare)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(prepare)
	script = strings.ReplaceAll(script, fmt.Sprintf("%x", digest), "<prepare-digest>")
	for i, command := range r.Deployment.Runtime.Prepare {
		script = strings.ReplaceAll(script, base64.StdEncoding.EncodeToString([]byte(command)), fmt.Sprintf("<prepare-%d>", i))
	}
	const want = `#!/bin/sh
set -eu
if [ "$#" -eq 0 ]; then
    exec /usr/bin/setpriv '--bounding-set=-all,+chown,+dac_override,+fowner,+fsetid,+setgid,+setuid,+setpcap,+sys_chroot,+audit_write' "$0" bounded
fi
export DEBIAN_FRONTEND=noninteractive
exec 9> '/var/lib/karakoram/volumes/examplem0vol/.karakoram-writer.lock'
if ! flock -n 9; then
    printf '%s\n' 'waiting for previous writer' >&2
    flock 9
fi
self_wait=0
while ! getent hosts "$NOMAD_JOB_ID" >/dev/null; do
    if [ "$self_wait" -ge 120 ]; then
        printf '%s\n' 'warning: own Nomad service hostname did not resolve within 120 seconds; continuing' >&2
        break
    fi
    sleep 1
    self_wait=$((self_wait + 1))
done
getent group karakoram >/dev/null || groupadd --gid 10001 karakoram
id karakoram >/dev/null 2>&1 || useradd --uid 10001 --gid 10001 --create-home --shell /bin/bash karakoram
test "$(id -u karakoram)" = 10001
test "$(id -g karakoram)" = 10001
mkdir -p /etc/sudoers.d
printf '%s\n' 'karakoram ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/karakoram
chmod 0440 /etc/sudoers.d/karakoram
chmod 4755 /usr/bin/sudo
mkdir -p '/opt/karakoram'
chown 10001:10001 '/opt/karakoram'
cd '/opt/karakoram'
if [ ! -f '/local/.karakoram-prepare-<prepare-digest>' ]; then
    printf '%s' '<prepare-0>' | base64 -d > '/local/karakoram-prepare-0.sh'
    chmod 0644 '/local/karakoram-prepare-0.sh'
    sudo -u 'karakoram' -- /bin/bash -e '/local/karakoram-prepare-0.sh'
    printf '%s' '<prepare-1>' | base64 -d > '/local/karakoram-prepare-1.sh'
    chmod 0644 '/local/karakoram-prepare-1.sh'
    sudo -u 'karakoram' -- /bin/bash -e '/local/karakoram-prepare-1.sh'
    touch '/local/.karakoram-prepare-<prepare-digest>'
fi
install -m 0755 -o 10001 -g 10001 '/local/artifact/karakoram' '/opt/karakoram/karakoram'
rm -f /etc/sudoers.d/karakoram
chmod 0755 /usr/bin/sudo
mkdir -p '/var/lib/karakoram/volumes/examplem0vol'
if [ "$(stat -c '%u:%g' '/var/lib/karakoram/volumes/examplem0vol')" != '10001:10001' ]; then
    chown 10001:10001 '/var/lib/karakoram/volumes/examplem0vol'
fi
. /secrets/karakoram.env
exec setpriv --reuid=10001 --regid=10001 --init-groups -- '/opt/karakoram/karakoram' 'agent' '--mode=test'
`
	if script != want {
		t.Fatalf("entrypoint golden mismatch\ngot:\n%s\nwant:\n%s", script, want)
	}
}

func TestRenderShellQuoteProperty(t *testing.T) {
	property := func(value string) bool {
		value = strings.ReplaceAll(value, "\x00", "")
		got, err := exec.Command("/bin/sh", "-c", "printf '%s' "+renderShellQuote(value)).Output()
		return err == nil && string(got) == value
	}
	for _, value := range []string{"", "' ; printf ESCAPED; #", "$(printf ESCAPED)", "`printf ESCAPED`", "\n'\\\";${HOME}\n", "-----BEGIN CERTIFICATE-----\nquoted'value\n-----END CERTIFICATE-----"} {
		if !property(value) {
			t.Fatalf("shell quoting changed %q", value)
		}
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestRenderPrepareTemplateProperty(t *testing.T) {
	_, r := renderFixture(t)
	property := func(command string) bool {
		r.Deployment.Runtime.Prepare = []string{command}
		script := renderEntrypoint(r)
		parsed, err := template.New("entrypoint").Parse(script)
		if err != nil {
			return false
		}
		var output bytes.Buffer
		if parsed.Execute(&output, nil) != nil || output.String() != script {
			return false
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(command))
		decode := "printf '%s' " + renderShellQuote(encoded) + " | base64 -d"
		decoded, err := exec.Command("/bin/sh", "-c", decode).Output()
		return err == nil && string(decoded) == command && strings.Contains(script, decode)
	}
	for _, command := range []string{"{{ nomadVar \"secret\" }}", "echo '${HOME}' && printf '%s' '\"\\'", "cat <<'EOF'\n{{ .GoTemplate }}\n${shell}\nEOF\n", "'; rm -rf /; #"} {
		if !property(command) {
			t.Fatalf("prepare command broke rendering: %q", command)
		}
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestRenderEnvTemplateQuoteProperty(t *testing.T) {
	type tuple struct{ K, V string }
	property := func(value string) bool {
		value = strings.ReplaceAll(value, "\x00", "")
		functions := template.FuncMap{
			"nomadVarExists": func(name string) bool { return name == "nomad/jobs/examplem0" },
			"nomadVar": func(name string) any {
				if name != "nomad/jobs/examplem0" {
					return nil
				}
				return struct{ Tuples []tuple }{[]tuple{{"VALUE", value}}}
			},
			"replaceAll": func(old, replacement, value string) string { return strings.ReplaceAll(value, old, replacement) },
		}
		parsed, err := template.New("env").Funcs(functions).Parse(renderEnvTemplate("examplem0"))
		if err != nil {
			return false
		}
		var output bytes.Buffer
		if parsed.Execute(&output, nil) != nil {
			return false
		}
		got, err := exec.Command("/bin/sh", "-c", output.String()+"printf '%s' \"$VALUE\"").Output()
		return err == nil && string(got) == value
	}
	for _, value := range []string{"' ; printf ESCAPED; #", "$(printf ESCAPED)", "\n'\\\";${HOME}\n", "line 1\nline '2'\n"} {
		if !property(value) {
			t.Fatalf("env template quoting changed %q", value)
		}
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestRenderAbsentEnvVariable(t *testing.T) {
	parsed, err := template.New("env").Funcs(template.FuncMap{
		"nomadVarExists": func(string) bool { return false },
		"nomadVar":       func(string) any { t.Fatal("absent variable must not be fetched"); return nil },
		"replaceAll":     func(old, replacement, value string) string { return strings.ReplaceAll(value, old, replacement) },
	}).Parse(renderEnvTemplate("examplem0"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, nil); err != nil || output.Len() != 0 {
		t.Fatalf("absent env variable = %q, %v", output.String(), err)
	}
}

func TestRenderHostsTemplateSelfAndPeers(t *testing.T) {
	type service struct{ Name string }
	type registration struct{ Address string }
	functions := template.FuncMap{
		"nomadServices": func() []service {
			return []service{{Name: "examplem0"}, {Name: "examplem1"}}
		},
		"nomadService": func(name string) []registration {
			if name == "examplem0" {
				return []registration{{Address: "192.0.2.10"}, {Address: "192.0.2.13"}}
			}
			return []registration{{Address: "192.0.2.11"}, {Address: "192.0.2.12"}}
		},
	}
	parsed, err := template.New("hosts").Funcs(functions).Parse(renderHostsTemplate())
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, nil); err != nil {
		t.Fatal(err)
	}
	want := "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n192.0.2.10 examplem0\n192.0.2.13 examplem0\n192.0.2.11 examplem1\n192.0.2.12 examplem1\n"
	if output.String() != want {
		t.Fatalf("hosts = %q, want %q", output.String(), want)
	}
}

func TestRenderOperationMarker(t *testing.T) {
	b, r := renderFixture(t)
	issued := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	r.Pending = &operation{Issued: issued}
	job, _, _ := renderParts(t, b, r)
	meta := job["Meta"].(map[string]string)
	if meta["karakoram.operation"] != issued.Format(time.RFC3339Nano) {
		t.Fatalf("operation marker = %q", meta["karakoram.operation"])
	}
}

func TestRenderRejectsMissingDeployment(t *testing.T) {
	b, r := renderFixture(t)
	r.Deployment = nil
	if _, err := b.renderJob(t.Context(), r); err == nil {
		t.Fatal("missing deployment accepted")
	}
}
