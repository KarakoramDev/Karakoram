// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// PostgreSQL defaults reserve three superuser slots. Keep another five for
// management/backup/direct administration and three monitor connections per
// proxy instance (connect, ping, read-only). Application pools must not borrow
// these slots even when the frontend user is postgres.
func (p *Core) proxyConnectionLimit(rc *engine.Context, s *state, proxy *member) (int, error) {
	old, desired := max(1, proxy.Replicas), max(1, rc.Cluster.Spec.Components.Extra["proxy"].Replicas)
	// Reserve deployment overlap before it starts, including ordinary updates
	// with unchanged logical replica count. Unknown backends may overlap both
	// full replica sets; a positive backend bound permits a tighter budget.
	instances := old + desired
	if surge := rc.Backend.Capabilities().StatelessRolloutSurge; surge > 0 {
		instances = max(old, desired) + surge
	}
	options := []json.RawMessage{rc.Cluster.Spec.EngineOptions}
	for _, m := range s.group(0).data() {
		options = append(options, m.Options)
	}
	if s.Rolling != nil && s.Rolling.Phase < len(s.Rolling.Phases) {
		options = append(options, s.Rolling.Phases[s.Rolling.Phase].Options)
	}
	var capacity int64
	for i, raw := range options {
		o, err := p.sharedMemoryOptions(raw)
		if err != nil {
			return 0, err
		}
		limit, err := sharedMemoryValue(o.Parameters, "max_connections")
		if err != nil {
			return 0, err
		}
		if i == 0 || limit < capacity {
			capacity = limit
		}
	}
	// During directional changes, only the smaller old/new capacity is safe
	// for every possible writer. Divide the budget, rather than giving each
	// proxy a full PostgreSQL-sized pool.
	limit := (capacity-8)/int64(instances) - 3
	if limit < 1 {
		return 0, engine.Invalid("PostgreSQL max_connections cannot accommodate proxy pools and reserved management connections")
	}
	return int(limit), nil
}
