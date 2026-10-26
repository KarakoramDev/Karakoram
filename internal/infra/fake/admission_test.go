// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// The native-probe control deliberately remains stuck: platform progress must
// not invent readiness, fail a deployment, or spend retries to free capacity.
func TestAdmissionPeerDependentReadiness(t *testing.T) {
	for _, members := range []int{3, 5} {
		for _, cap := range []int{1, 2, members} {
			for _, startup := range []bool{false, true} {
				t.Run(fmt.Sprintf("members%d/cap%d/startup%t", members, cap, startup), func(t *testing.T) {
					base := New(Options{})
					peers := make([]string, members)
					for i := range members {
						peers[i] = fmt.Sprintf("member%d", i)
					}
					b := &Admission{Backend: base, Limit: cap, Peers: peers}
					started := 0
					for range members * 3 {
						b.Advance()
						if started == members {
							break
						}
						name := fmt.Sprintf("member%d", started)
						if service, _ := base.GetService(t.Context(), name); service == nil {
							if _, err := base.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}}); err != nil {
								t.Fatal(err)
							}
						}
						args := []string{"agent", "readiness"}
						if startup {
							args = append(args, "--startup", "--data-dir=/data")
						}
						_, err := b.Deploy(t.Context(), name, infra.Deployment{Artifact: infra.Artifact{Reader: strings.NewReader("artifact"), Name: "karakoram.tar.gz", Binary: "karakoram"}, Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Readiness: &infra.Readiness{Args: args, FailureTimeout: time.Minute, RetryPeriod: time.Second}}})
						if err == nil {
							started++
						} else {
							var wait *infra.DeploymentWaitError
							if !errors.As(err, &wait) {
								t.Fatal(err)
							}
						}
					}
					if startup && started != members {
						t.Fatalf("startup retained admission: started=%d", started)
					}
					if !startup && cap < members && started != cap {
						t.Fatalf("native peer dependency did not reproduce deadlock: started=%d cap=%d", started, cap)
					}
					if b.Peak > cap {
						t.Fatalf("admission bypassed: %d > %d", b.Peak, cap)
					}
				})
			}
		}
	}
}

func TestAdmissionEvaluatesMemberStateAndQueuedPeerDNS(t *testing.T) {
	member := &AdmissionMember{AgentRunning: true, Configured: true, ProcessStarted: true, DNSPublished: true}
	b := &Admission{Peers: []string{"first", "queued"}, Members: map[string]*AdmissionMember{"first": member}, Commands: map[string]string{"first": "karakoram agent readiness --startup --data-dir=/data"}}
	if err := b.LookupHost("queued"); err == nil {
		t.Fatal("undeployed queued peer resolved")
	}
	if err := b.readiness("first"); err != nil {
		t.Fatal("healthy initialising agent waited for DNS/quorum", err)
	}
	member.AgentRunning = false
	if err := b.readiness("first"); err == nil {
		t.Fatal("startup flag bypassed absent agent")
	}
	member.AgentRunning = true
	member.Configured = false
	if err := b.readiness("first"); err != nil {
		t.Fatal("pending configuration rejected", err)
	}
	member.LocalInitError = errors.New("invalid local safety controls")
	if err := b.readiness("first"); err == nil {
		t.Fatal("known local failure passed startup")
	}
	member.LocalInitError = nil
	b.Commands["first"] = "karakoram agent readiness --engine=postgres --data-dir=/data"
	if err := b.readiness("first"); err == nil {
		t.Fatal("native SQL probe passed absent configuration")
	}
	member.Configured = true
	if err := b.readiness("first"); err == nil || !strings.Contains(err.Error(), "no such host") {
		t.Fatal("native SQL probe ignored queued-peer DNS", err)
	}
	b.Members["queued"] = &AdmissionMember{DNSPublished: true}
	if err := b.readiness("first"); err == nil || !strings.Contains(err.Error(), "quorum") {
		t.Fatal("native probe ignored missing quorum", err)
	}
	b.Members["queued"].ProcessStarted = true
	if err := b.readiness("first"); err != nil {
		t.Fatal("native SQL probe did not recover after peer startup", err)
	}
}
