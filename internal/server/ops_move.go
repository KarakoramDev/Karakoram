// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"

	"github.com/KarakoramDev/Karakoram/internal/api"
)

func (s *Server) MoveClusterMember(ctx context.Context, request api.MoveClusterMemberRequestObject) (api.MoveClusterMemberResponseObject, error) {
	return s.handle(ctx, "MoveClusterMember", input{Cluster: request.Cluster, Member: request.Member}), nil
}
func (r response) VisitMoveClusterMemberResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) replaceOrMoveMember(ctx context.Context, rc *engine.Context, provider engine.Provider, name string, relocate bool) response {
	c := rc.Cluster
	var member *domain.MemberStatus
	for i := range c.Status.Members {
		if c.Status.Members[i].Name == name {
			member = &c.Status.Members[i]
			break
		}
	}
	if member == nil {
		return fail(404, "not_found", "Member not found")
	}
	if validator, ok := provider.(engine.MemberReplacementValidator); ok {
		if err := validator.ValidateMemberReplacement(c, name); err != nil {
			if errors.Is(err, engine.ErrInvalid) {
				return fail(409, "conflict", err.Error())
			}
			return s.failure(err)
		}
	}
	if !relocate {
		return s.enqueue(ctx, c.Ref(), domain.OperationReplaceMember, map[string]string{"member": name})
	}
	if failure := s.providerContext(rc); failure != nil {
		return *failure
	}
	authoritative := rc.Backend.Capabilities().Placement
	if authoritative && !domain.KnownPlacementGroup(member.PlacementGroup) {
		return fail(409, "conflict", "Cannot move member: current backend placement group is unknown; wait for authoritative placement observation")
	}
	payload, err := json.Marshal(struct {
		Member     string `json:"member"`
		Relocate   bool   `json:"relocate"`
		AvoidGroup string `json:"avoidGroup"`
	}{name, true, member.PlacementGroup})
	if err != nil {
		return s.failure(err)
	}
	op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationReplaceMember, Payload: payload, Actor: principal(ctx).Actor(), Target: domain.OperationTarget{Kind: "member", Name: name}}
	if !authoritative {
		op.Message = "Best-effort move: a new runtime and volume will be created; this backend provides no host placement guarantee"
	}
	ApplyMaintenanceOverride(ctx, &op)
	if err := s.Store.EnqueueOperation(ctx, &op); err != nil {
		return s.failure(err)
	}
	s.trigger(ctx, c.Ref())
	return response{status: 202, body: operationAPI(op), operationID: op.ID}
}
