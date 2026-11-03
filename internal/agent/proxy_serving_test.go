// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type servingProxyEngine struct {
	fakeEngine
	name string
}

func (e *servingProxyEngine) ProxyServingReadiness(context.Context) error { return e.readinessErr }
func (e *servingProxyEngine) Name() string                                { return e.name }

func TestProxyServingReadinessFailsClosed(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		for _, condition := range []string{"serving", "no-revision", "unapplied-revision", "latest-apply-failed", "not-proxy", "maintenance", "local-maintenance", "stopped", "not-desired", "not-running", "apply-failed", "writer-offline"} {
			t.Run(engine+"/"+condition, func(t *testing.T) {
				e := &servingProxyEngine{name: engine}
				a := &nodeAgent{o: Options{Engine: engine, DataDir: t.TempDir()}, engine: e, sup: newSupervisor(io.Discard), c: agentapi.MemberConfig{Role: "proxy", ConfigVersion: 4, DesiredProcesses: []string{"proxysql"}}}
				a.acceptedVersion = 4
				a.sup.children["proxysql"] = &child{status: agentapi.ProcessStatus{State: "running", PID: 123}}
				switch condition {
				case "no-revision":
					a.c.ConfigVersion = 0
				case "unapplied-revision":
					a.acceptedVersion = 5
				case "latest-apply-failed":
					a.lastError = "native config apply failed"
				case "not-proxy":
					a.c.Role = "data"
				case "maintenance":
					a.c.Maintenance = true
				case "local-maintenance":
					a.controls.Maintenance = true
				case "stopped":
					a.controls.Stopped = map[string]bool{"proxysql": true}
				case "not-desired":
					a.c.DesiredProcesses = []string{}
				case "not-running":
					a.sup.children["proxysql"].status.PID = 0
				case "apply-failed":
					a.startupErr = errors.New("configuration apply failed")
				case "writer-offline":
					e.readinessErr = errors.New("no online writer")
				}
				ln, err := a.serveProxyServing()
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				err = Readiness(context.Background(), Options{}, []string{"--serving-proxy", "--data-dir=" + a.o.DataDir})
				if (err == nil) != (condition == "serving") {
					t.Fatalf("serving check = %v", err)
				}
			})
		}
	}
}

func TestProxyServingReadinessOldAgentAndArgumentsFailClosed(t *testing.T) {
	for _, args := range [][]string{
		{"--serving-proxy", "--data-dir=" + t.TempDir()},
		{"--serving-proxy"},
		{"--serving-proxy", "--data-dir=relative"},
		{"--serving-proxy", "--data-dir=/tmp", "extra"},
	} {
		if err := Readiness(t.Context(), Options{}, args); err == nil {
			t.Fatalf("unsafe serving probe accepted %v", args)
		}
	}
}
