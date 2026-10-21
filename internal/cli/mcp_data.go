// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import "github.com/spf13/cobra"

func dataMCPConfig(cmd *cobra.Command, a *app, cluster string, write bool, client string) error {
	return usageError("data MCP gateway support is not part of this build")
}
