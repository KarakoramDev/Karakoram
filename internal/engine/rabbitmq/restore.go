// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

const maxDefinitionsBytes = 64 << 20

type invalidDefinitionsError struct{ message string }

func (e *invalidDefinitionsError) Error() string { return e.message }

func validDefinitions(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return len(data) <= maxDefinitionsBytes && len(trimmed) != 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

// Definitions restore merges customer objects. Bootstrap-managed users and their
// grants must retain the target's credentials rather than importing source hashes.
func targetDefinitions(data []byte) ([]byte, error) {
	var object map[string]json.RawMessage
	if len(data) > maxDefinitionsBytes || json.Unmarshal(data, &object) != nil || object == nil {
		return nil, &invalidDefinitionsError{"backup contains invalid definitions JSON"}
	}
	for _, key := range []string{"users", "permissions", "topic_permissions"} {
		raw, exists := object[key]
		if !exists {
			continue
		}
		var entries []map[string]json.RawMessage
		if json.Unmarshal(raw, &entries) != nil || entries == nil {
			return nil, &invalidDefinitionsError{"backup contains invalid definitions entries"}
		}
		kept := entries[:0]
		for _, entry := range entries {
			field := "user"
			if key == "users" {
				field = "name"
			}
			var user string
			if json.Unmarshal(entry[field], &user) != nil || user == "" {
				return nil, &invalidDefinitionsError{"backup contains invalid definitions user identity"}
			}
			switch user {
			case "karakoram", "admin", "monitor", "guest":
				continue
			}
			kept = append(kept, entry)
		}
		object[key], _ = json.Marshal(kept)
	}
	return json.Marshal(object)
}

func definitionsSource(ctx context.Context, rc *engine.Context, source domain.ClusterRef, name string) (*domain.Backup, error) {
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Ref().Namespace
	}
	if source.Name == "" {
		source.Name = rc.Cluster.Name
	}
	if name == "" {
		return nil, invalid("RabbitMQ restore requires a definitions backup; PITR is not supported")
	}
	b, err := rc.Store.GetBackup(ctx, source, name)
	if err != nil {
		return nil, err
	}
	if b.Provider != "rabbitmq" || b.Type != "definitions" || b.Phase != domain.PhaseSucceeded || b.Location == "" {
		return nil, invalid("restore source is not a successful RabbitMQ definitions backup")
	}
	return b, nil
}

func (p *Provider) importDefinitionsBackup(ctx context.Context, rc *engine.Context, b *domain.Backup) error {
	s, err := definitionsStorage(ctx, rc, b)
	if err != nil {
		return err
	}
	if err = definitionsLocation(*s, *b); err != nil {
		return err
	}
	client, closeClient, err := definitionsS3Client(*s, s.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	object, err := client.GetObject(ctx, s.Bucket, b.Location, minio.GetObjectOptions{})
	if err != nil {
		return definitionsS3Error("download", err)
	}
	defer object.Close()
	// Surface S3 failures separately from corrupt gzip payloads (GetObject is lazy).
	if _, err = object.Stat(); err != nil {
		return definitionsS3Error("download", err)
	}
	reader, err := gzip.NewReader(io.LimitReader(object, maxDefinitionsBytes+1))
	if err != nil {
		if errors.Is(err, gzip.ErrHeader) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return &invalidDefinitionsError{"backup contains invalid gzip definitions"}
		}
		return definitionsS3Error("download", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, maxDefinitionsBytes+1))
	closeErr := reader.Close()
	var corrupt flate.CorruptInputError
	if len(data) > maxDefinitionsBytes || errors.Is(readErr, gzip.ErrChecksum) || errors.Is(readErr, gzip.ErrHeader) || errors.Is(readErr, io.ErrUnexpectedEOF) || errors.As(readErr, &corrupt) {
		return &invalidDefinitionsError{"backup contains corrupt or oversized gzip definitions"}
	}
	if readErr != nil {
		return definitionsS3Error("download", readErr)
	}
	if closeErr != nil {
		return &invalidDefinitionsError{"backup contains corrupt gzip definitions"}
	}
	data, err = targetDefinitions(data)
	if err != nil {
		return err
	}
	admin, err := p.adminClient(ctx, rc)
	if err != nil {
		return err
	}
	defer admin.Close()
	return admin.ImportDefinitions(ctx, data)
}

func (p *Provider) restoreOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := definitionsLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		if cp.Failure != "" {
			return definitionsResult(cp), errors.New(cp.Failure)
		}
		return definitionsResult(cp), nil
	}
	name, err := definitionsPayload(op, "restore")
	if err != nil {
		return definitionsResult(cp), err
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return definitionsResult(cp), err
	}
	if r.Source.PITRTime != nil {
		cp.Step, cp.Failure = "failed", "RabbitMQ definitions restore does not support PITR"
	}
	var b *domain.Backup
	if cp.Step == "" || cp.Step == "import" {
		b, err = definitionsSource(ctx, rc, domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}, r.Source.Backup)
		if err != nil {
			if !errors.Is(err, engine.ErrInvalid) {
				return definitionsResult(cp), err
			}
			cp.Step, cp.Failure = "failed", err.Error()
		}
	}
	switch cp.Step {
	case "":
		stamp := now(rc)
		if r.StartedAt == nil {
			r.StartedAt = &stamp
		}
		r.Phase, r.Error = domain.PhaseRunning, ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsRestoreEvent(ctx, rc, op, r, "RestoreStarted", "RabbitMQ definitions restore started"); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "import"
	case "import":
		if e := p.importDefinitionsBackup(ctx, rc, b); e != nil {
			var invalid *invalidDefinitionsError
			switch {
			case errors.As(e, &invalid):
				cp.Failure = invalid.Error()
			case errors.Is(e, engine.ErrInvalid):
				cp.Failure = "RabbitMQ rejected definitions import"
			default:
				return definitionsResult(cp), e
			}
			cp.Step = "failed"
		} else {
			cp.Step = "complete"
		}
	case "complete", "failed":
		stamp := now(rc)
		r.CompletedAt = &stamp
		reason, message := "RestoreCompleted", "RabbitMQ definitions restore succeeded (objects merged; messages are not restored)"
		r.Phase, r.Error = domain.PhaseSucceeded, ""
		if cp.Failure != "" {
			r.Phase, r.Error = domain.PhaseFailed, cp.Failure
			reason, message = "RestoreFailed", cp.Failure
		}
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsRestoreEvent(ctx, rc, op, r, reason, message); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "done"
		if cp.Failure != "" {
			return definitionsResult(cp), errors.New(cp.Failure)
		}
	default:
		return definitionsResult(cp), errors.New("unknown definitions restore step")
	}
	return definitionsResult(cp), nil
}

func definitionsRestoreEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, r *domain.Restore, reason, message string) error {
	if err := definitionsEvent(ctx, rc, op, reason, message); err != nil {
		return err
	}
	if rc.Events == nil || r.SourceNamespace == "" || r.SourceNamespace == rc.Cluster.Ref().Namespace {
		return nil
	}
	source := r.Source.SourceCluster
	if source == "" {
		source = rc.Cluster.Name
	}
	kind := "Normal"
	if reason == "RestoreFailed" {
		kind = "Warning"
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: r.SourceNamespace, Cluster: source, Operation: op.ID, Type: kind, Reason: reason, Message: message + " into " + rc.Cluster.Ref().Namespace + "/" + rc.Cluster.Name, CreatedAt: now(rc)})
}

// cloneStep runs after bootstrap and readiness. A persisted successful import is
// never repeated; an interrupted import may be safely replayed because definitions
// merge by object identity. Message contents are not part of this backup type.
func (p *Provider) cloneStep(ctx context.Context, rc *engine.Context) (bool, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	if rc.Cluster.Spec.DataSource == nil || s.CloneImported {
		return true, nil
	}
	source := rc.Cluster.Spec.DataSource
	if source.PITRTime != nil {
		return false, errors.New("RabbitMQ definitions clone does not support PITR")
	}
	b, err := definitionsSource(ctx, rc, domain.ClusterRef{Namespace: source.Namespace, Name: source.Cluster}, source.Backup)
	if err != nil {
		return false, err
	}
	if err = p.importDefinitionsBackup(ctx, rc, b); err != nil {
		return false, err
	}
	s.CloneImported = true
	if err = s.save(rc.Cluster); err != nil {
		return false, err
	}
	return true, nil
}
