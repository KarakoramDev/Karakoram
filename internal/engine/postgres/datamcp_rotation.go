package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func (p *Core) rotateDataUser(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	var payload struct {
		User string `json:"user"`
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return engine.OpResult{}, engine.Invalid("invalid rotation payload")
	}
	ro, rw := engine.DataAccessLevels(rc.Cluster)
	if !(payload.User == engine.DataAccessReadUser && ro || payload.User == engine.DataAccessReadWriteUser && rw) {
		return engine.OpResult{}, engine.Invalid("rotation requires an enabled data-access system user")
	}
	var cp struct {
		Stage string `json:"stage"`
	}
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	key := "rotations/" + op.ID + "/" + payload.User
	result := func(stage string, done bool) (engine.OpResult, error) {
		cp.Stage = stage
		raw, err := json.Marshal(cp)
		return engine.OpResult{Done: done, Step: stage, Checkpoint: raw, RequeueAfter: time.Second}, err
	}
	if cp.Stage == "" {
		if _, err := secret(ctx, rc, key); missing(err) {
			password, e := secrets.Password()
			if e != nil {
				return engine.OpResult{}, e
			}
			if e = putSecret(ctx, rc, key, []byte(password)); e != nil {
				return engine.OpResult{}, e
			}
		} else if err != nil {
			return engine.OpResult{}, err
		}
		return result("password-staged", false)
	}
	if cp.Stage == "password-staged" {
		password, err := secret(ctx, rc, key)
		if err != nil {
			return engine.OpResult{}, err
		}
		if err = putSecret(ctx, rc, "users/"+payload.User, password); err != nil {
			return engine.OpResult{}, err
		}
		if err = putSecret(ctx, rc, "access/revision", []byte(op.ID)); err != nil {
			return engine.OpResult{}, err
		}
		return result("reload", false)
	}
	if cp.Stage == "reload" {
		if _, err := p.Sync(ctx, rc); err != nil {
			return engine.OpResult{}, err
		}
		s, err := load(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		for _, m := range s.Members {
			if m.ConfigVersion != s.ConfigVersion {
				return result("reload", false)
			}
			ok, err := ready(ctx, rc, m)
			if err != nil {
				return engine.OpResult{}, err
			}
			if !ok {
				return result("reload", false)
			}
		}
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return engine.OpResult{}, err
		}
		return result("rotated", true)
	}
	if cp.Stage == "rotated" {
		return result("rotated", true)
	}
	return engine.OpResult{}, engine.Invalid("invalid system user rotation checkpoint")
}
