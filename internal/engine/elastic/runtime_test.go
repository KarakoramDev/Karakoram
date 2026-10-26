// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRuntimeSpec(t *testing.T) {
	layout := infra.Layout{AppDir: "/srv/app", VolumeRoot: "/volumes", User: "elasticuser"}
	for _, tt := range []struct {
		component, hostname, volume string
		timeout                     time.Duration
		ports                       []infra.Port
	}{
		{"elasticsearch", "demon0", "demon0vol", 600 * time.Second, []infra.Port{{Port: 9200, Protocol: "tcp"}, {Port: 9300, Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}}},
		{"kibana", "demokb", "", 300 * time.Second, []infra.Port{{Port: 5601, Protocol: "tcp", HTTP: true}, {Port: 9443, Protocol: "tcp"}}},
		{"logstash", "demols", "", 300 * time.Second, []infra.Port{{Port: 5044, Protocol: "tcp"}, {Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9443, Protocol: "tcp"}}},
		{"apm-server", "demoapm", "", 300 * time.Second, []infra.Port{{Port: 8200, Protocol: "tcp", HTTP: true}, {Port: 9443, Protocol: "tcp"}}},
	} {
		t.Run(tt.component, func(t *testing.T) {
			in := RuntimeInput{Hostname: tt.hostname, Cluster: "demo", Component: tt.component, Version: "9.5.5", Volume: tt.volume, AgentURL: "https://control.example.com", InternalCAPEM: "certificate\nline\n"}
			spec, err := RuntimeSpec(layout, in)
			if err != nil {
				t.Fatal(err)
			}
			if spec.Base != infra.BaseUbuntu2404 || !reflect.DeepEqual(spec.Args, []string{"agent"}) || !spec.StopBeforeStart || spec.Volume != tt.volume {
				t.Fatal("runtime configuration", spec)
			}
			if !reflect.DeepEqual(spec.Prepare, PrepareCommands(tt.component, in.Version, layout.User)) || !reflect.DeepEqual(spec.Ports, tt.ports) {
				t.Fatal("prepare or ports", spec)
			}
			wantEnv := map[string]string{"KARAKORAM_URL": in.AgentURL, "KARAKORAM_ENGINE": "elasticsearch", "KARAKORAM_CLUSTER": "demo", "KARAKORAM_MEMBER": tt.hostname, "KARAKORAM_INTERNAL_CA": in.InternalCAPEM, engine.DataDirVariable: layout.DataDir(tt.volume)}
			if !reflect.DeepEqual(spec.Env, wantEnv) {
				t.Fatal("environment", spec.Env)
			}
			wantArgs := []string{"agent", "readiness", "--startup", "--data-dir=" + layout.DataDir(tt.volume)}
			if spec.Readiness == nil || !reflect.DeepEqual(spec.Readiness.Args, wantArgs) || spec.Readiness.FailureTimeout != tt.timeout || spec.Readiness.RetryPeriod != 10*time.Second {
				t.Fatal("readiness", spec.Readiness)
			}
		})
	}
}

func TestRuntimeSpecInvalidInputs(t *testing.T) {
	for _, change := range []func(*RuntimeInput){
		func(in *RuntimeInput) { in.Hostname = "invalid-host" }, func(in *RuntimeInput) { in.Cluster = "x" }, func(in *RuntimeInput) { in.Volume = "bad-volume" }, func(in *RuntimeInput) { in.Component = "unknown" }, func(in *RuntimeInput) { in.Version = "9.5.5;false" },
	} {
		in := RuntimeInput{Hostname: "demon0", Cluster: "demo", Component: "elasticsearch", Version: "9.5.5"}
		change(&in)
		if spec, err := RuntimeSpec(infra.Layout{User: "elasticuser"}, in); err == nil || !reflect.DeepEqual(spec, infra.RuntimeSpec{}) {
			t.Fatal("invalid deployment accepted")
		}
	}
}
