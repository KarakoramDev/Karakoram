// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func primaryDecreaseAllowed(s pgspec.Spec, st pgspec.StatusResponse) bool {
	if s.Topology != pgspec.TopologyHA || !st.Running || st.Role != "primary" || st.Leader != s.Member || !st.EtcdHealthy || s.Postgres == nil || len(st.SharedMemoryParameters) != 5 {
		return false
	}
	decrease := false
	for key, current := range st.SharedMemoryParameters {
		next, ok := pgspec.SharedMemoryDefault(key)
		if !ok {
			return false
		}
		if value, ok := s.Postgres.Parameters[key]; ok {
			var err error
			next, err = strconv.ParseInt(value, 10, 64)
			if err != nil || next < 0 {
				return false
			}
		}
		if next > current {
			return false
		}
		decrease = decrease || next < current
	}
	return decrease
}
