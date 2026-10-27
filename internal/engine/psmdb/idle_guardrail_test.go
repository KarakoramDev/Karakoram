// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type idleNativeDialer map[string]*replacementMongo

func (d idleNativeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	server := d[address]
	if server == nil {
		return nil, fmt.Errorf("unexpected native fixture address %q", address)
	}
	return server.DialContext(ctx, network, address)
}

type idleMongoBackend struct{ *fakeInfra }

func (b idleMongoBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	instances, err := b.fakeInfra.Containers(ctx, name)
	for i := range instances {
		instances[i].ID = name + "-instance"
		instances[i].Hostname = name + "-instance"
		instances[i].IPv6 = "2001:db8::1"
	}
	return instances, err
}

type idleMongoAgents struct{ testDialer }

func (d idleMongoAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(d.url)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, Token: "test-token", CAPEM: d.ca, DialInstanceContext: func(ctx context.Context, _, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint.Host)
	}})
}

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"single", "replicaset", "sharded"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Backend = idleMongoBackend{rc.Backend.(*fakeInfra)}
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			if topology == "single" {
				rc.Cluster.Spec.Components.Engine.Replicas = 1
			}
			if topology == "sharded" {
				rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
				p.ApplyDefaults(&rc.Cluster.Spec)
			}
			rc.Cluster.Spec.TLS.Mode = "disabled"
			s, _ := load(rc.Cluster)
			plan(rc.Cluster, s)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			dialer := idleNativeDialer{}
			for set := range s.Sets {
				native := nativeHealingFixture(t, rc, s)
				members := s.setMembers(set)
				native.config = mongoadmin.Config{ID: set, Version: 1}
				for _, m := range members {
					native.config.Members = append(native.config.Members, mongoadmin.ConfigMember{ID: m.Index, Host: m.Name + ":27017", Votes: 1, Priority: 1})
					dialer[m.Name+":27017"] = native
				}
				observed, err := healthyTestReplication(t.Context(), rc, s, members[0])
				if err != nil {
					t.Fatal(err)
				}
				native.observed = &observed
				primary := members[0].Name + ":27017"
				native.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "setName": set, "hosts": bson.A{primary}, "primary": primary, "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
				native.command = func(bson.Raw) bson.M {
					return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": major(rc.Cluster.Spec.Version)}}
				}
			}
			router := nativeHealingFixture(t, rc, s)
			router.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "msg": "isdbgrid", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
			router.command = func(bson.Raw) bson.M { return bson.M{"ok": 1} }
			for _, m := range s.Members {
				if m.Role == agentapi.RoleRouter {
					dialer[m.Name+":27017"] = router
				}
			}
			rc.Dialer = dialer
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				current, _ := load(rc.Cluster)
				switch r.URL.Path {
				case "/v1/status":
					ready := true
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: current.ConfigVersion, EngineVersion: rc.Cluster.Spec.Version, MongoReady: &ready, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "mongos": {State: "running"}}})
				case "/v1/bootstrap":
					json.NewEncoder(w).Encode(agentapi.BootstrapResponse{Initialized: true, UserCreated: true})
				default:
					json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				}
			}))
			t.Cleanup(server.Close)
			rc.Agents = idleMongoAgents{testDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
