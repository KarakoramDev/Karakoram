// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
)

func TestContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Harness {
		server := newFakeAPI(t)
		b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return contracttest.Harness{Backend: b, Hostname: func(s string) string { return "kkcontract" + s }, Artifact: func() infra.Artifact {
			return infra.Artifact{Reader: strings.NewReader("archive"), Name: "karakoram.tar.gz", Binary: "karakoram"}
		}, PollInterval: time.Millisecond, Timeout: 5 * time.Second}
	})
}
