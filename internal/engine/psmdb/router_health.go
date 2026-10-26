// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func routerHealthy(status agentapi.Status) bool {
	return running(status, agentapi.RoleRouter) && status.MongoReady != nil && *status.MongoReady
}

type routerProcessObservation struct {
	instance  string
	processes map[string]agentapi.ProcessStatus
	at        time.Time
}

func routerInstancesHealthy(ctx context.Context, rc *engine.Context, m *member, version int64, observed ...engine.InstanceInventory) (bool, []routerProcessObservation, error) {
	// Generic status already includes the native metadata probe. Visit each
	// instance once so configuration and health come from the same observation.
	var observations []routerProcessObservation
	var mu sync.Mutex
	probe := func(client *agentapi.Client, instance string) (bool, error) {
		readCtx, cancel := context.WithTimeout(ctx, observationReadBudget)
		status, err := client.Status(readCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("PSMDB router status read exceeded %s budget: inspect member %s instance %s responsiveness/connectivity: %w", observationReadBudget, m.Name, instance, err)
		}
		if err != nil {
			return false, err
		}
		mu.Lock()
		observations = append(observations, routerProcessObservation{instance: instance, processes: status.Processes, at: now(rc)})
		mu.Unlock()
		if status.ConfigVersion < version {
			return false, fmt.Errorf("router configuration %d is older than required %d", status.ConfigVersion, version)
		}
		if status.MongoError != "" {
			return false, errors.New(status.MongoError)
		}
		if running(status, agentapi.RoleRouter) && status.MongoReady == nil {
			// CP upgrades do not roll agents. Older binaries lack this additive
			// observation, so perform the same authenticated majority read over
			// the pinned instance's namespace transport instead.
			return legacyRouterHealthy(ctx, rc, m, client)
		}
		if !routerHealthy(status) {
			return false, errors.New("router authenticated config-server metadata is not ready")
		}
		return true, nil
	}
	var all bool
	var count int
	var err error
	if len(observed) == 0 {
		all, count, err = engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, probe)
	} else {
		all, count, err = engine.VisitObservedInstancesWithIdentity(ctx, rc, m.Name, observed[0], probe)
	}
	slices.SortFunc(observations, func(a, b routerProcessObservation) int {
		return strings.Compare(a.instance, b.instance)
	})
	if err != nil {
		return false, observations, err
	}
	required := max(1, m.Containers, rc.Cluster.Spec.Components.Router.Replicas)
	if count < required {
		return false, observations, fmt.Errorf("router %s has %d live instances; require %d", m.Name, count, required)
	}
	return all, observations, nil
}

func legacyRouterHealthy(ctx context.Context, rc *engine.Context, m *member, transport *agentapi.Client) (bool, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	pw, err := secret(ctx, rc, "users/clusterMonitor")
	if err != nil {
		return false, err
	}
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	opts := mongoadmin.DialOptions{Hosts: []string{m.Name + ":27017"}, Dialer: transport, Direct: true, Username: "clusterMonitor", Password: string(pw), CAPEM: ca, TLS: s.TLSMode != "disabled" && s.TLSMode != "allowTLS"}
	c, err := mongoadmin.Dial(ctx, opts)
	if err != nil && s.TLSMode == "preferTLS" {
		opts.TLS = false
		c, err = mongoadmin.Dial(ctx, opts)
	}
	if err != nil {
		return false, fmt.Errorf("legacy router authenticated metadata probe: %w", err)
	}
	defer c.Disconnect(context.Background())
	// RunCommand adds maxTimeMS from the deadline; an explicit field would
	// duplicate it and MongoDB rejects the command before reading metadata.
	command := bson.D{{Key: "find", Value: "shards"}, {Key: "filter", Value: bson.D{}}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}, {Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}}}
	commandCtx, commandCancel := context.WithTimeout(ctx, 2*time.Second)
	defer commandCancel()
	if err := c.Database("config").RunCommand(commandCtx, command, options.RunCmd().SetReadPreference(readpref.Primary())).Err(); err != nil {
		return false, fmt.Errorf("legacy router cannot read config-server majority metadata: %w", err)
	}
	return true, nil
}
