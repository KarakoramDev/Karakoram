// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ObserveRestoreAbsence is observation only. Absence never proves that a
// historical handler exited; PBM orchestration must actively fence every agent.
func (h hooks) ObserveRestoreAbsence(ctx context.Context, name string, env pbm.Env) (int64, bool, error) {
	if h.rc.Cluster.Spec.Topology.Type == "sharded" {
		return 0, false, nil
	}
	s, err := load(h.rc.Cluster)
	if err != nil {
		return 0, false, err
	}
	c, err := h.p.dial(ctx, h.rc, s, "rs0", "backup")
	if err != nil {
		return 0, false, err
	}
	defer c.Disconnect(ctx)
	stamp, absent, err := observeRestoreAbsence(ctx, c, name)
	if err != nil || !absent {
		return stamp, false, err
	}
	absent, err = pbm.RestoreStorageAbsent(ctx, env, name)
	return stamp, absent, err
}

func observeRestoreAbsence(ctx context.Context, c *mongo.Client, name string) (int64, bool, error) {
	db := c.Database("admin")
	filter := bson.M{"cmd": "restore", "restore.name": name}
	count, err := db.Collection("pbmCmd").CountDocuments(ctx, filter)
	if err != nil || count != 1 {
		return 0, false, err
	}
	var cmd struct {
		ID bson.ObjectID `bson:"_id"`
		TS int64         `bson:"ts"`
	}
	if err := db.Collection("pbmCmd").FindOne(ctx, filter).Decode(&cmd); err != nil {
		return 0, false, err
	}
	var hello struct {
		LocalTime time.Time `bson:"localTime"`
	}
	readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	if err := db.RunCommand(readCtx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(readCtx.Err(), context.DeadlineExceeded) {
			return 0, false, fmt.Errorf("PSMDB restore absence hello read exceeded %s budget: inspect member in connection error for responsiveness/connectivity: %w", engine.ObservationTimeout, err)
		}
		return 0, false, err
	}
	if cmd.TS <= 0 || hello.LocalTime.Before(time.Unix(cmd.TS, 0).Add(time.Minute)) {
		return cmd.TS, false, nil
	}
	count, err = db.Collection("pbmRestores").CountDocuments(ctx, bson.M{"name": name})
	if err != nil || count != 0 {
		return cmd.TS, false, err
	}
	for _, collection := range []string{"pbmLock", "pbmLockOp"} {
		// Any conflicting PBM work blocks process replacement, not only
		// another restore. Only this restore's own stale lock is explainable.
		cursor, err := db.Collection(collection).Find(ctx, bson.M{})
		if err != nil {
			return cmd.TS, false, err
		}
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			var lock struct {
				HB   bson.Timestamp `bson:"hb"`
				OPID string         `bson:"opid"`
				Type string         `bson:"type"`
			}
			if err := cursor.Decode(&lock); err != nil {
				return cmd.TS, false, err
			}
			if lock.Type != "restore" || lock.OPID != cmd.ID.Hex() || lock.HB.T == 0 || hello.LocalTime.Before(time.Unix(int64(lock.HB.T), 0).Add(time.Minute)) {
				return cmd.TS, false, nil
			}
		}
		if err := cursor.Err(); err != nil {
			return cmd.TS, false, err
		}
	}
	return cmd.TS, true, nil
}

// InvalidateOperationObservation also serves reconciler failures before this
// provider is entered (dependency resolution and restore-provider validation).
func (*Provider) InvalidateOperationObservation(op *domain.Operation) error {
	if op.Kind != domain.OperationRestore {
		return nil
	}
	raw, err := pbm.ResetRestoreAbsence(op.Checkpoint)
	if err == nil {
		op.Checkpoint = raw
	}
	return err
}
