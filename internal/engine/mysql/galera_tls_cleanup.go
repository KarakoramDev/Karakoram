// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// finishGaleraTLSCleanup runs only after Cleanup has removed every runtime.
// Suspension can therefore finish publishing the common traffic bundle without
// waiting for agents that cleanup intentionally removed.
func finishGaleraTLSCleanup(ctx context.Context, rc *engine.Context, s *state, keepVolumes bool) error {
	if keepVolumes && s.GaleraTLS != nil {
		switch s.GaleraTLS.Stage {
		case "fence", "stopped", "applied", "reform":
			for _, file := range galeraTrafficFiles {
				value, err := secret(ctx, rc, galeraTrafficPending+file.suffix)
				if err != nil {
					return err
				}
				if err := putSecret(ctx, rc, file.active, value); err != nil {
					return err
				}
			}
			publishRevision(s)
			// Persist activation before removing any pending file. A retry
			// must not attempt to copy an already removed pending file.
			s.GaleraTLS.Stage = "cleanup"
			if err := save(rc.Cluster, s); err != nil {
				return err
			}
		case "cleanup":
			// The common active bundle was already fully published.
		default:
			return engine.Invalid("unknown Galera traffic TLS checkpoint %s", s.GaleraTLS.Stage)
		}
	}
	for _, file := range galeraTrafficFiles {
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), galeraTrafficPending+file.suffix); err != nil && !missing(err) {
			return err
		}
		if !keepVolumes {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), file.active); err != nil && !missing(err) {
				return err
			}
		}
	}
	s.GaleraTLS = nil
	return nil
}
