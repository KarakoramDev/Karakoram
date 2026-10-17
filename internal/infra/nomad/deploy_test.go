// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"text/template"
)

func deploymentFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("../../../deploy/nomad/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func deploymentTemplate(t *testing.T, job, destination string) string {
	t.Helper()
	pattern := `(?s)destination\s*=\s*"` + regexp.QuoteMeta(destination) + `".*?data\s*=\s*<<EOF\n(.*?)\nEOF`
	match := regexp.MustCompile(pattern).FindStringSubmatch(job)
	if match == nil {
		t.Fatalf("missing deployment template %q", destination)
	}
	return match[1]
}

func quotedPolicyValues(text string) []string {
	matches := regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(text, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}
	slices.Sort(values)
	return values
}

// These assertions pin security-sensitive shipped examples; Nomad jobspec/ACL
// parsing and authorization are separately re-verified against the live version.
func TestDeploymentLeastPrivilegePolicy(t *testing.T) {
	policy := deploymentFile(t, "karakoram-policy.hcl")
	want := []string{"list-jobs", "read-job", "submit-job", "read-logs", "alloc-lifecycle", "host-volume-create", "host-volume-read", "host-volume-delete"}
	slices.Sort(want)
	caps := regexp.MustCompile(`(?s)namespace\s+"karakoram"\s*\{\s*capabilities\s*=\s*\[([^]]*)\]`).FindStringSubmatch(policy)
	if caps == nil || !reflect.DeepEqual(quotedPolicyValues(caps[1]), want) {
		t.Fatalf("namespace capabilities differ from least privilege: %v", caps)
	}
	paths := regexp.MustCompile(`(?s)path\s+"([^"]+)"\s*\{\s*capabilities\s*=\s*\[([^]]*)\]`).FindAllStringSubmatch(policy, -1)
	got := map[string][]string{}
	for _, path := range paths {
		got[path[1]] = quotedPolicyValues(path[2])
	}
	wantPaths := map[string][]string{
		"karakoram/*":          {"destroy", "list", "read", "write"},
		"nomad/jobs/*":         {"destroy", "list", "read", "write"},
		"nomad/jobs/karakoram": {"deny"},
	}
	if !reflect.DeepEqual(got, wantPaths) {
		t.Fatalf("variable ACLs = %v, want %v", got, wantPaths)
	}
	mounts := regexp.MustCompile(`(?m)^host_volume\s+"([^"]+)"`).FindAllStringSubmatch(policy, -1)
	if len(mounts) != 1 || mounts[0][1] != "kk-5932d69c-*" {
		t.Fatalf("unexpected host volume mount grants: %v", mounts)
	}
}

func TestDeploymentControlPlaneNetworkAndEntrypoint(t *testing.T) {
	job := deploymentFile(t, "karakoram.nomad.hcl")
	if !regexp.MustCompile(`(?s)artifact\s*\{.*?options\s*\{[^}]*archive\s*=\s*"tar.gz"`).MatchString(job) {
		t.Fatal("control-plane artifact extraction depends on its URL extension")
	}
	if strings.Contains(job, `port "agent"`) || strings.Contains(job, "NOMAD_ALLOC_IP_") {
		t.Fatal("control plane publishes a private port or relies on its label")
	}
	for _, pattern := range []string{
		`(?s)variable "public_port"\s*\{\s*type\s*=\s*number\s*default\s*=\s*0`,
		`(?s)dynamic "port"\s*\{\s*for_each\s*=\s*var.public_port == 0 \? \[\] : \[var.public_port\].*?labels\s*=\s*\["public"\].*?static\s*=\s*port.value.*?to\s*=\s*8080`,
		`(?s)service\s*\{\s*name\s*=\s*"karakoram"\s*provider\s*=\s*"nomad"\s*address_mode\s*=\s*"alloc".*?port\s*=\s*"9443"`,
	} {
		if !regexp.MustCompile(pattern).MatchString(job) {
			t.Fatalf("control-plane network invariant missing: %s", pattern)
		}
	}
	if hosts := deploymentTemplate(t, job, "etc/hosts"); hosts != renderHostsTemplate() {
		t.Fatalf("control plane and member hosts templates differ: %q", hosts)
	}
	entrypoint := deploymentTemplate(t, job, "local/karakoram-entrypoint")
	last := -1
	for _, part := range []string{"exec /usr/bin/setpriv", "flock 9", `while ! getent hosts "$NOMAD_JOB_ID"`, `"$self_wait" -ge 120`, "sleep 1", "install -m 0755", "rm -f /etc/sudoers.d/karakoram", "chmod 0755 /usr/bin/sudo", ". '/secrets/karakoram.env'", "exec /usr/bin/setpriv --reuid=10001"} {
		index := strings.Index(entrypoint, part)
		if index <= last {
			t.Fatalf("control-plane entrypoint order for %q: %d after %d", part, index, last)
		}
		last = index
	}
}

func TestDeploymentEnvironmentExcludesTLSPEMs(t *testing.T) {
	job := deploymentFile(t, "karakoram.nomad.hcl")
	type tuple struct{ K, V string }
	data := struct{ Tuples []tuple }{[]tuple{
		{"KARAKORAM_MASTER_KEY", "test-master"}, {"KARAKORAM_NOMAD_TOKEN", "test-token"},
		{"NOMAD_CA_PEM", "test-ca-pem"}, {"NOMAD_CLIENT_CERT_PEM", "test-cert-pem"},
		{"NOMAD_CLIENT_KEY_PEM", "test-private-key-pem"}, {"OTHER", "test-other"},
	}}
	parsed, err := template.New("env").Funcs(template.FuncMap{
		"nomadVar":   func(string) any { return data },
		"regexMatch": regexp.MatchString,
		"replaceAll": func(old, replacement, value string) string { return strings.ReplaceAll(value, old, replacement) },
	}).Parse(deploymentTemplate(t, job, "secrets/karakoram.env"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "export KARAKORAM_MASTER_KEY='test-master'\nexport KARAKORAM_NOMAD_TOKEN='test-token'\n"; got != want {
		t.Fatalf("control-plane environment = %q, want %q", got, want)
	}
	for file, key := range map[string]string{
		"nomad-ca.pem": "NOMAD_CA_PEM", "nomad-client-cert.pem": "NOMAD_CLIENT_CERT_PEM", "nomad-client-key.pem": "NOMAD_CLIENT_KEY_PEM",
	} {
		if !strings.Contains(deploymentTemplate(t, job, "secrets/"+file), "."+key) {
			t.Fatalf("missing TLS file template %s", file)
		}
	}
}
