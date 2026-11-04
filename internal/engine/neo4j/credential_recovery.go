// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"errors"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

// Only an explicit authentication rejection authorizes an offline repair.
// Network failures and other server errors must never cause an outage.
func adminCredentialsRejected(err error) bool {
	var response *agentapi.HTTPError
	return errors.As(err, &response) && response.StatusCode == http.StatusConflict
}

func recoverRotatedAdmin(ctx context.Context, rc *engine.Context, op *domain.Operation, cp operationCheckpoint) (engine.OpResult, error) {
	if cp.Step == "recover-admin" {
		next, err := secret(ctx, rc, "operations/"+op.ID+"/password")
		if err != nil {
			return checkpoint(cp, false), err
		}
		var job neo4jspec.JobResponse
		err = action(ctx, rc, neo4jspec.ActionRecoverAdmin, neo4jspec.RecoverAdminRequest{
			Receipt: op.ID + "-recover-admin", AdminPassword: string(next),
		}, &job)
		if err != nil {
			return checkpoint(cp, false), errors.New("Neo4j administrator recovery action failed")
		}
		switch job.State {
		case neo4jspec.JobRunning:
			return checkpoint(cp, false), nil
		case neo4jspec.JobDone:
			cp.Step = "record-admin-recovery"
			return checkpoint(cp, false), nil
		case neo4jspec.JobFailed:
			return checkpoint(cp, false), errors.New("Neo4j offline administrator recovery failed")
		default:
			return checkpoint(cp, false), errors.New("Neo4j administrator recovery returned an invalid job state")
		}
	}
	if rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{
			Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
			Operation: op.ID, Type: "Warning", Reason: "AdministratorCredentialsRecovered",
			Message:   "Repairing drifted Neo4j administrator credentials required an offline recovery and caused a database outage",
			CreatedAt: now(rc),
		}); err != nil {
			return checkpoint(cp, false), err
		}
	}
	cp.Step = "commit-password"
	return checkpoint(cp, false), nil
}
