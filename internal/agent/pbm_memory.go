// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func pbmProcessSpec(c agentapi.MemberConfig, uri string) processSpec {
	env := make([]string, 1, 2)
	env[0] = "PBM_MONGODB_URI=" + uri
	if c.PBMGoMemoryLimitBytes != 0 {
		env = append(env, "GOMEMLIMIT="+strconv.FormatUint(c.PBMGoMemoryLimitBytes, 10))
	}
	return processSpec{binary: "pbm-agent", env: env, goMemoryLimitBytes: c.PBMGoMemoryLimitBytes}
}
