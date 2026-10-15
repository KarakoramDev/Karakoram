// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestZeropsYAML(t *testing.T) {
	for _, tt := range []struct {
		component, hostname, volume, timeout string
		ports                                []int
		http                                 []bool
	}{
		{"elasticsearch", "demon0", "demon0v", "600s", []int{9200, 9300, 9443}, []bool{false, false, false}},
		{"elasticsearch", "demom0", "demom0v", "600s", []int{9200, 9300, 9443}, []bool{false, false, false}},
		{"kibana", "demokb", "", "300s", []int{5601, 9443}, []bool{true, false}},
		{"logstash", "demols", "", "300s", []int{5044, 8080, 9443}, []bool{false, true, false}},
		{"apm-server", "demoapm", "", "300s", []int{8200, 9443}, []bool{true, false}},
	} {
		t.Run(tt.hostname, func(t *testing.T) {
			in := YAMLInput{Hostname: tt.hostname, Cluster: "demo", Component: tt.component, Version: "9.5.5", Volume: tt.volume, AgentURL: "https://control.example.com", InternalCAPEM: "certificate\nline\n"}
			text, err := ZeropsYAML(in)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Zerops []struct {
					Setup string `yaml:"setup"`
					Run   struct {
						Base            string   `yaml:"base"`
						Start           string   `yaml:"start"`
						PrepareCommands []string `yaml:"prepareCommands"`
						Ports           []struct {
							Port     int    `yaml:"port"`
							Protocol string `yaml:"protocol"`
							HTTP     bool   `yaml:"httpSupport"`
						} `yaml:"ports"`
						Env    map[string]string `yaml:"envVariables"`
						Volume *struct {
							Hostname string `yaml:"hostname"`
						} `yaml:"volume"`
					} `yaml:"run"`
					Deploy struct {
						Shutdown  bool `yaml:"temporaryShutdown"`
						Readiness struct {
							Exec struct {
								Command string `yaml:"command"`
							} `yaml:"exec"`
							FailureTimeout string `yaml:"failureTimeout"`
							RetryPeriod    string `yaml:"retryPeriod"`
						} `yaml:"readinessCheck"`
					} `yaml:"deploy"`
				} `yaml:"zerops"`
			}
			if err = yaml.Unmarshal([]byte(text), &doc); err != nil {
				t.Fatal(err)
			}
			if len(doc.Zerops) != 1 {
				t.Fatal("setup count")
			}
			setup := doc.Zerops[0]
			if setup.Setup != tt.hostname || setup.Run.Base != "ubuntu@24.04" || setup.Run.Start != "./karakoram agent" {
				t.Fatal("run configuration")
			}
			if !reflect.DeepEqual(setup.Run.PrepareCommands, prepareCommands(tt.component, "9.5.5")) {
				t.Fatal("prepare commands")
			}
			if len(setup.Run.Ports) != len(tt.ports) {
				t.Fatal("port count")
			}
			for i, p := range setup.Run.Ports {
				if p.Port != tt.ports[i] || p.Protocol != "tcp" || p.HTTP != tt.http[i] {
					t.Fatalf("port %+v", p)
				}
			}
			dir := "/var/www/karakoram-data"
			if tt.volume != "" {
				dir = "/mnt/" + tt.volume
			}
			env := map[string]string{"KARAKORAM_URL": in.AgentURL, "KARAKORAM_ENGINE": "elasticsearch", "KARAKORAM_CLUSTER": "demo", "KARAKORAM_MEMBER": tt.hostname, "KARAKORAM_INTERNAL_CA": in.InternalCAPEM, "KARAKORAM_DATA_DIR": dir}
			if !reflect.DeepEqual(setup.Run.Env, env) {
				t.Fatal("deployment env")
			}
			if strings.Contains(text, "TOKEN") || strings.Contains(text, "envSecrets") {
				t.Fatal("agent token must not appear in YAML")
			}
			if tt.volume == "" {
				if setup.Run.Volume != nil {
					t.Fatal("unexpected stateless volume")
				}
			} else if setup.Run.Volume == nil || setup.Run.Volume.Hostname != tt.volume {
				t.Fatal("missing volume")
			}
			r := setup.Deploy.Readiness
			if !setup.Deploy.Shutdown || r.Exec.Command != "/var/www/karakoram agent readiness --engine=elasticsearch --data-dir="+dir+" "+tt.component || r.FailureTimeout != tt.timeout || r.RetryPeriod != "10s" {
				t.Fatal("deployment readiness")
			}
		})
	}
}

func TestZeropsYAMLInvalidInputs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*YAMLInput)
	}{
		{"hostname", func(in *YAMLInput) { in.Hostname = "invalid-host" }}, {"cluster", func(in *YAMLInput) { in.Cluster = "x" }}, {"volume", func(in *YAMLInput) { in.Volume = "bad-volume" }}, {"component", func(in *YAMLInput) { in.Component = "unknown" }}, {"version", func(in *YAMLInput) { in.Version = "9.5.5;false" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := YAMLInput{Hostname: "demon0", Cluster: "demo", Component: "elasticsearch", Version: "9.5.5"}
			tt.change(&in)
			if text, err := ZeropsYAML(in); err == nil || text != "" {
				t.Fatal("invalid deployment accepted")
			}
		})
	}
}
