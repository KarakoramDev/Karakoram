// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func (p *Provider) runRotateTLS(ctx context.Context, rc *engine.Context, s *state, cp operationState) (engine.OpResult, error) {
	switch cp.Stage {
	case "":
		for _, m := range s.esNodes() {
			cp.Names = append(cp.Names, m.Name)
		}
		cp.Stage = "issue"
	case "issue":
		if cp.Index < len(cp.Names) {
			m := s.find(cp.Names[cp.Index])
			if m != nil {
				if err := p.issueLeafs(ctx, rc, s, m, true); err != nil {
					return operationResult(cp, false), err
				}
			}
			cp.Index++
		} else {
			cp.Stage = "publish"
		}
	case "publish":
		p.bumpConfig(s)
		cp.ConfigVersion = s.ConfigVersion
		p.reloadMembers(ctx, rc, cp.Names...)
		cp.Stage = "ack"
	case "ack":
		done, err := p.rotationAcknowledged(ctx, rc, s, cp, false)
		if err != nil || !done {
			return operationResult(cp, false), err
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), operationInvalid("unknown TLS rotation checkpoint")
	}
	return operationResult(cp, false), nil
}

func rotationTarget(user string) (key, kind string) {
	switch user {
	case engine.DataAccessReadUser:
		return secretDataAccessRead, ""
	case engine.DataAccessReadWriteUser:
		return secretDataAccessReadWrite, ""
	case "elastic":
		return "users/elastic", ""
	case "karakoram-monitor":
		return "users/karakoram-monitor", ""
	case "kibana-token":
		return "kibana/token", kindKibana
	case "logstash":
		return "users/karakoram_logstash", kindLogstash
	case "apm":
		return "users/karakoram_apm", kindAPM
	case "apm-secret-token":
		return "apm/secret-token", kindAPM
	default:
		return "", ""
	}
}

func (p *Provider) runRotateUser(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, user string) (engine.OpResult, error) {
	key, kind := rotationTarget(user)
	if key == "" || op.ID == "" {
		return operationResult(cp, true), operationInvalid("unsupported system user or missing operation identity")
	}
	stagedKey := "operations/" + op.ID + "/rotation"
	tokenName := "kk-rotate-" + secrets.Hash(op.ID)[:20]
	switch cp.Stage {
	case "":
		if user == "kibana-token" {
			old, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "kibana/token-name")
			if err != nil && !operationMissing(err) {
				return operationResult(cp, false), err
			}
			cp.OldTokenName = string(old)
		}
		for _, m := range s.Members {
			if kind != "" && m.Kind == kind || user == "karakoram-monitor" && (m.Kind == kindNode || m.Kind == kindMaster) {
				cp.Names = append(cp.Names, m.Name)
			}
		}
		cp.Stage = "generate"
	case "generate":
		// A staged encrypted secret survives crashes between generation, publication
		// and acknowledgement. Never put credential material into the checkpoint.
		value, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), stagedKey)
		if err != nil && !operationMissing(err) {
			return operationResult(cp, false), err
		}
		if len(value) == 0 {
			var generated string
			if user == "kibana-token" {
				client, e := p.es(ctx, rc, s)
				if e != nil {
					return operationResult(cp, false), e
				}
				defer client.Close()
				// Only the uninstalled operation-specific token may be replayed this way.
				if e = client.DeleteServiceToken(ctx, "elastic", "kibana", tokenName); e != nil && !esapi.IsNotFound(e) {
					return operationResult(cp, false), e
				}
				generated, err = client.CreateServiceToken(ctx, "elastic", "kibana", tokenName)
			} else {
				generated, err = secrets.RandomString(64)
			}
			if err != nil {
				return operationResult(cp, false), err
			}
			if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), stagedKey, []byte(generated)); err != nil {
				return operationResult(cp, false), err
			}
		}
		cp.Stage = "install"
	case "install":
		value, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), stagedKey)
		if err != nil {
			return operationResult(cp, false), err
		}
		if len(value) == 0 {
			return operationResult(cp, false), errors.New("rotation credential is unavailable")
		}
		if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, value); err != nil {
			return operationResult(cp, false), err
		}
		if user == "kibana-token" {
			if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "kibana/token-name", []byte(tokenName)); err != nil {
				return operationResult(cp, false), err
			}
		}
		cp.Stage = "ensure"
	case "ensure":
		if err := p.ensureSecurity(ctx, rc, s, true); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "publish"
	case "publish":
		p.bumpConfig(s)
		cp.ConfigVersion = s.ConfigVersion
		p.reloadMembers(ctx, rc, cp.Names...)
		cp.Stage = "ack"
	case "ack":
		done, err := p.rotationAcknowledged(ctx, rc, s, cp, false)
		if err != nil || !done {
			return operationResult(cp, false), err
		}
		cp.Index = 0
		if kind != "" {
			cp.Stage = "restart"
		} else {
			cp.Stage = "invalidate"
		}
	case "restart":
		if cp.Index < len(cp.Names) {
			name := cp.Names[cp.Index]
			m := s.find(name)
			if m != nil {
				agent, err := p.agentClient(ctx, rc, name)
				if err != nil {
					return operationResult(cp, false), err
				}
				defer agent.Close()
				if err = agent.Process(ctx, agentapi.ProcessAction{Process: componentProcess(m), Action: "restart", RequestID: op.ID + "/restart/" + name}); err != nil {
					return operationResult(cp, false), err
				}
			}
			cp.Index++
		} else {
			cp.Stage = "running"
		}
	case "running":
		done, err := p.rotationAcknowledged(ctx, rc, s, cp, true)
		if err != nil || !done {
			return operationResult(cp, false), err
		}
		cp.Stage = "invalidate"
	case "invalidate":
		if user == "kibana-token" && cp.OldTokenName != "" && cp.OldTokenName != tokenName {
			client, err := p.es(ctx, rc, s)
			if err != nil {
				return operationResult(cp, false), err
			}
			defer client.Close()
			if err = client.DeleteServiceToken(ctx, "elastic", "kibana", cp.OldTokenName); err != nil && !esapi.IsNotFound(err) {
				return operationResult(cp, false), err
			}
		}
		cp.Stage = "cleanup"
	case "cleanup":
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), stagedKey); err != nil && !operationMissing(err) {
			return operationResult(cp, false), err
		}
		cp.Stage = "done"
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), operationInvalid("unknown credential rotation checkpoint")
	}
	return operationResult(cp, false), nil
}

func (p *Provider) rotationAcknowledged(ctx context.Context, rc *engine.Context, s *state, cp operationState, running bool) (bool, error) {
	for _, name := range cp.Names {
		agent, err := p.agentClient(ctx, rc, name)
		if err != nil {
			return false, err
		}
		defer agent.Close()
		status, err := engine.ObserveAgentStatus(ctx, agent, "Elasticsearch", name)
		if err != nil {
			return false, err
		}
		if status.ConfigVersion < cp.ConfigVersion {
			p.reloadMembers(ctx, rc, name)
			return false, nil
		}
		if running {
			m := s.find(name)
			if m != nil && status.Processes[componentProcess(m)].State != "running" {
				return false, nil
			}
		}
	}
	return true, nil
}
