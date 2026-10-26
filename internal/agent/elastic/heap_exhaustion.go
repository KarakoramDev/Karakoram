// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

const heapTailLines = 200
const heapTailBytes = 32 << 10

// Only fixed fatal V8 signatures are retained; log text never leaves the agent.
func kibanaHeapFailure(lines []agentapi.LogLine, process agentapi.ProcessStatus, since time.Time, capMiB int) *agentspec.HeapExhaustion {
	if capMiB <= 0 || process.SupervisorSession == "" || process.Launch == 0 {
		return nil
	}
	bytes := 0
	for i := len(lines) - 1; i >= max(0, len(lines)-heapTailLines); i-- {
		line := lines[i]
		bytes += len(line.Message)
		if bytes > heapTailBytes {
			break
		}
		if line.Timestamp.Before(since) || line.Process != agentspec.ComponentKibana || line.Stream != "stderr" || line.SupervisorSession != process.SupervisorSession || line.Launch == 0 || line.Launch > process.Launch {
			continue
		}
		if line.ExitCode == nil || *line.ExitCode == 0 || !strings.HasPrefix(line.Message, "FATAL ERROR: ") {
			continue
		}
		// Output from the current running child is not evidence of an exit.
		if line.Launch == process.Launch && process.State != "failed" {
			continue
		}
		for _, marker := range []string{"JavaScript heap out of memory", "Ineffective mark-compacts near heap limit", "Reached heap limit"} {
			if strings.Contains(line.Message, marker) {
				return &agentspec.HeapExhaustion{SupervisorSession: line.SupervisorSession, Launch: line.Launch, OldSpaceMiB: capMiB}
			}
		}
	}
	return nil
}

func renderedOldSpaceMiB(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil || len(data) > 1024 {
		return 0
	}
	value := strings.TrimSpace(string(data))
	const prefix = "--max-old-space-size="
	if !strings.HasPrefix(value, prefix) {
		return 0
	}
	capMiB, err := strconv.Atoi(strings.TrimPrefix(value, prefix))
	if err != nil {
		return 0
	}
	return max(0, capMiB)
}

func heapExhaustionMessage(capMiB int, version string) string {
	minimum := ""
	if strings.HasPrefix(version, "9.") {
		minimum = " to at least 1.5 GiB for Kibana 9.x"
	}
	return fmt.Sprintf("Kibana heap exhausted at the old-space cap (%d MiB); increase spec.components.extra.kibana.resources minRam and maxRam together%s. This is not evidence of a cgroup OOM kill", capMiB, minimum)
}
