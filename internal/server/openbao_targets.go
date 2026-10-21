// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
)

// openbaoConnectionTarget is the trusted controller resolver. Request handlers
// independently enforce caller permissions before invoking the provider.
func (s *Server) openbaoConnectionTarget(ctx context.Context, ref domain.ClusterRef) (openbao.Target, error) {
	rc, p, e := s.context(ctx, ref)
	if e != nil {
		return openbao.Target{}, e
	}
	if s.providerContext(rc) != nil {
		return openbao.Target{}, engine.ErrUnavailable
	}
	return openbao.Target{Provider: p, Context: rc}, nil
}
