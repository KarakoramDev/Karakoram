// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var nativeISTWritesets = regexp.MustCompile(`Receiving IST: ([1-9][0-9]*) writesets, seqnos ([0-9]+)-([0-9]+)`)

// A receiver setup/progress line is not completion. Galera also emits an IST
// completion for certification-index preload after SST, so require an actual
// nonempty writeset receive range followed by its completed position and reject
// physical SST activity in this same post-crash log window.
func containsCompletedIST(raw []byte) bool {
	text := string(raw)
	for _, sst := range []string{"Proceeding with SST", "Preparing the backup at", "Moving the backup to"} {
		if strings.Contains(text, sst) {
			return false
		}
	}
	var end int64
	receiving := false
	for _, line := range strings.Split(text, "\n") {
		if match := nativeISTWritesets.FindStringSubmatch(line); match != nil {
			count, countErr := strconv.ParseInt(match[1], 10, 64)
			first, firstErr := strconv.ParseInt(match[2], 10, 64)
			last, lastErr := strconv.ParseInt(match[3], 10, 64)
			if countErr == nil && firstErr == nil && lastErr == nil && first > 0 && last >= first && count == last-first+1 {
				receiving = true
				end = last
			}
		}
		_, position, ok := strings.Cut(line, "IST received: ")
		if !receiving || !ok {
			continue
		}
		fields := strings.Fields(position)
		if len(fields) != 1 {
			continue
		}
		_, seqno, err := myspec.ParseGaleraPosition(fields[0])
		if err == nil && seqno >= end {
			return true
		}
	}
	return false
}

func TestNativeCompletedISTRequiresActualWritesetsAndCompletion(t *testing.T) {
	const position = "826914cc-cd20-11f1-b4c4-6ac2787ad4af:18"
	receive := "[Galera] Receiving IST: 3 writesets, seqnos 16-18\n"
	complete := "[Galera] IST received: " + position + "\n"
	for _, test := range []struct {
		name, log string
		want      bool
	}{
		{"completed writesets", receive + complete, true},
		{"receiver only", "Prepared IST receiver for 16-18", false},
		{"progress only", "Receiving IST... 100.0% (3/3 events) complete.", false},
		{"receive attempt only", receive, false},
		{"preload completion only", complete, false},
		{"completion before attempt", complete + receive, false},
		{"older completion", receive + "IST received: 826914cc-cd20-11f1-b4c4-6ac2787ad4af:15\n", false},
		{"invalid completion", receive + "IST received: invalid:18\n", false},
		{"physical SST then index preload", "Proceeding with SST.........\n" + receive + complete, false},
		{"physical SST prepare", receive + "Preparing the backup at /private/sst\n" + complete, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := containsCompletedIST([]byte(test.log)); got != test.want {
				t.Fatalf("completedIST=%v want=%v", got, test.want)
			}
		})
	}
}
