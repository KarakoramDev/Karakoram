// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra_test

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func validDeployment() infra.Deployment {
	return infra.Deployment{Artifact: infra.Artifact{Reader: strings.NewReader("archive"), Name: "karakoram.tar.gz", Binary: "karakoram"}, Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent", "--port=8080"}, Ports: []infra.Port{{Port: 8080, Protocol: "tcp"}, {Port: 53, Protocol: "udp"}}, Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}, FailureTimeout: time.Minute, RetryPeriod: time.Second}}}
}
func TestDeploymentValidate(t *testing.T) {
	cases := []struct {
		name   string
		change func(*infra.Deployment)
		valid  bool
	}{
		{"valid", func(*infra.Deployment) {}, true},
		{"no_readiness", func(d *infra.Deployment) { d.Runtime.Readiness = nil }, true},
		{"readiness_one_second", func(d *infra.Deployment) {
			d.Runtime.Readiness.FailureTimeout = time.Second
			d.Runtime.Readiness.RetryPeriod = time.Second
		}, true},
		{"boundary_ports", func(d *infra.Deployment) {
			d.Runtime.Ports = []infra.Port{{Port: 1, Protocol: "tcp"}, {Port: 65535, Protocol: "udp"}}
		}, true},
		{"nil_reader", func(d *infra.Deployment) { d.Artifact.Reader = nil }, false},
		{"empty_archive", func(d *infra.Deployment) { d.Artifact.Name = "" }, false},
		{"empty_binary", func(d *infra.Deployment) { d.Artifact.Binary = "" }, false},
		{"unsafe_binary", func(d *infra.Deployment) { d.Artifact.Binary = "binary;echo" }, false},
		{"empty_base", func(d *infra.Deployment) { d.Runtime.Base = "" }, false},
		{"no_args", func(d *infra.Deployment) { d.Runtime.Args = nil }, false},
		{"empty_arg", func(d *infra.Deployment) { d.Runtime.Args = []string{""} }, false},
		{"whitespace_arg", func(d *infra.Deployment) { d.Runtime.Args = []string{"two words"} }, false},
		{"shell_arg", func(d *infra.Deployment) { d.Runtime.Args = []string{"$(command)"} }, false},
		{"readiness_no_args", func(d *infra.Deployment) { d.Runtime.Readiness.Args = nil }, false},
		{"readiness_unsafe_arg", func(d *infra.Deployment) { d.Runtime.Readiness.Args = []string{"ready;echo"} }, false},
		{"readiness_zero_timeout", func(d *infra.Deployment) { d.Runtime.Readiness.FailureTimeout = 0 }, false},
		{"readiness_negative_timeout", func(d *infra.Deployment) { d.Runtime.Readiness.FailureTimeout = -time.Second }, false},
		{"readiness_subsecond_timeout", func(d *infra.Deployment) { d.Runtime.Readiness.FailureTimeout = time.Millisecond }, false},
		{"readiness_fractional_timeout", func(d *infra.Deployment) { d.Runtime.Readiness.FailureTimeout = time.Second + time.Nanosecond }, false},
		{"readiness_zero_retry", func(d *infra.Deployment) { d.Runtime.Readiness.RetryPeriod = 0 }, false},
		{"readiness_negative_retry", func(d *infra.Deployment) { d.Runtime.Readiness.RetryPeriod = -time.Second }, false},
		{"readiness_subsecond_retry", func(d *infra.Deployment) { d.Runtime.Readiness.RetryPeriod = time.Millisecond }, false},
		{"readiness_fractional_retry", func(d *infra.Deployment) { d.Runtime.Readiness.RetryPeriod = time.Second + time.Nanosecond }, false},
		{"zero_port", func(d *infra.Deployment) { d.Runtime.Ports[0].Port = 0 }, false},
		{"negative_port", func(d *infra.Deployment) { d.Runtime.Ports[0].Port = -1 }, false},
		{"large_port", func(d *infra.Deployment) { d.Runtime.Ports[0].Port = 65536 }, false},
		{"unknown_protocol", func(d *infra.Deployment) { d.Runtime.Ports[0].Protocol = "TCP" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := validDeployment()
			tc.change(&d)
			if err := d.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate = %v, valid=%v", err, tc.valid)
			}
		})
	}
}
func TestDeploymentValidationDoesNotOverwriteArguments(t *testing.T) {
	d := validDeployment()
	args := []string{"agent", "sentinel", "sentinel"}
	d.Runtime.Args = args[:1]
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	if args[1] != "sentinel" || args[2] != "sentinel" {
		t.Fatal("validation mutated arguments backing array")
	}
}
func TestLayoutPaths(t *testing.T) {
	l := infra.Layout{AppDir: "/srv/app", VolumeRoot: "/srv/volumes", User: "app"}
	if got := l.VolumePath("member"); got != "/srv/volumes/member" {
		t.Fatal(got)
	}
	if got := l.DataDir("member"); got != l.VolumePath("member") {
		t.Fatal(got)
	}
	if got := l.DataDir(""); got != "/srv/app/karakoram-data" {
		t.Fatal(got)
	}
}
