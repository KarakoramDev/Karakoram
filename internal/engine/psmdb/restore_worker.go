// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Staleness is not death proof. Only a later successful launch in the SAME
// supervisor session proves a previously observed child and its predecessors
// were reaped. Cross-host wall-clock ordering never establishes replacement.
const restoreWorkerStaleAfter = 2 * time.Minute

func (h hooks) ObserveRestoreWorkerFailure(ctx context.Context, name string, env pbm.Env, workers map[string]pbm.RestoreWorkerIdentity) (string, bool, error) {
	s, err := load(h.rc.Cluster)
	if err != nil {
		return "", false, err
	}
	set := "rs0"
	if env.Sharded {
		set = "cfg"
	}
	c, err := h.restorePBMClient(ctx, s, set)
	if err != nil {
		return "", false, err
	}
	defer c.Disconnect(ctx)
	return observeRestoreWorkerFailure(ctx, c, name, workers, func(ctx context.Context, host string) (agentapi.ProcessStatus, error) {
		for _, node := range env.Nodes {
			if node.Member != host {
				continue
			}
			a, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), node.Member)
			if err != nil {
				return agentapi.ProcessStatus{}, err
			}
			defer a.Close()
			status, err := engine.ObserveAgentStatus(ctx, a, "PSMDB", node.Member)
			if err != nil {
				return agentapi.ProcessStatus{}, err
			}
			return status.Processes["pbm-agent"], nil
		}
		return agentapi.ProcessStatus{}, nil // Unknown ownership is not death proof.
	})
}

func observeRestoreWorkerFailure(ctx context.Context, c *mongo.Client, name string, workers map[string]pbm.RestoreWorkerIdentity, process func(context.Context, string) (agentapi.ProcessStatus, error)) (string, bool, error) {
	db := c.Database("admin")
	var receipt struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := db.Collection("pbmCmd").FindOne(ctx, bson.M{"cmd": "restore", "restore.name": name}).Decode(&receipt); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", false, nil
		}
		return "", false, err
	}
	if receipt.ID.IsZero() {
		return "", false, nil
	}
	var metadata struct {
		Status     string `bson:"status"`
		Transition int64  `bson:"last_transition_ts"`
	}
	if err := db.Collection("pbmRestores").FindOne(ctx, bson.M{"name": name}).Decode(&metadata); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", false, nil
		}
		return "", false, err
	}
	switch metadata.Status {
	case "", "done", "error", "partlyDone", "partly-done", "cancelled", "canceled":
		return "", false, nil // A terminal native outcome wins over stale worker evidence.
	}
	var hello struct {
		LocalTime time.Time `bson:"localTime"`
	}
	readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	if err := db.RunCommand(readCtx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(readCtx.Err(), context.DeadlineExceeded) {
			return "", false, fmt.Errorf("PSMDB restore worker hello read exceeded %s budget: inspect member in connection error for responsiveness/connectivity: %w", engine.ObservationTimeout, err)
		}
		return "", false, err
	}
	transition := time.Unix(metadata.Transition, 0)
	nativeStale := metadata.Transition > 0 && !hello.LocalTime.Before(transition.Add(restoreWorkerStaleAfter))
	cursor, err := db.Collection("pbmLock").Find(ctx, bson.M{"type": "restore", "opid": receipt.ID.Hex()})
	if err != nil {
		return "", false, err
	}
	defer cursor.Close(ctx)
	unconfirmed := false
	for cursor.Next(ctx) {
		var lock struct {
			Node string         `bson:"node"`
			HB   bson.Timestamp `bson:"hb"`
		}
		if err := cursor.Decode(&lock); err != nil {
			return "", unconfirmed, err
		}
		heartbeat := time.Unix(int64(lock.HB.T), 0)
		lockStale := lock.HB.T != 0 && !hello.LocalTime.Before(heartbeat.Add(restoreWorkerStaleAfter))
		unconfirmed = unconfirmed || (nativeStale && lockStale)
		host, _, err := net.SplitHostPort(lock.Node)
		if err != nil {
			continue
		}
		owner, err := process(ctx, host)
		if err != nil {
			return "", unconfirmed, err
		}
		if owner.SupervisorSession == "" || owner.Launch == 0 {
			continue // Legacy agents cannot prove a process incarnation.
		}
		key := receipt.ID.Hex() + "/" + lock.Node
		previous, observed := workers[key]
		if !observed || previous.Session != owner.SupervisorSession {
			workers[key] = pbm.RestoreWorkerIdentity{Session: owner.SupervisorSession, Launch: owner.Launch}
			continue // First observation or supervisor interruption is ambiguous.
		}
		if owner.Launch <= previous.Launch || !nativeStale || !lockStale {
			continue
		}
		return fmt.Sprintf("native restore worker on %s was replaced: supervisor launch %d supersedes observed launch %d in the same session; heartbeat and native progress have been stale for at least %s (possible OOM or process restart); inspect member memory limits and native logs before explicit fresh-target recovery", host, owner.Launch, previous.Launch, restoreWorkerStaleAfter), false, nil
	}
	return "", unconfirmed, cursor.Err()
}
