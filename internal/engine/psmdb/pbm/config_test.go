// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"strings"
	"testing"
)

const configGolden = `storage:
    type: s3
    s3:
        endpointUrl: https://s3.example.test
        region: us-east-1
        bucket: bucket
        prefix: karakoram/target-target-id
        forcePathStyle: true
        insecureSkipTLSVerify: false
        credentials:
            access-key-id: access
            secret-access-key: secret
pitr:
    enabled: false
    oplogSpanMin: 1
backup:
    compression: s2
restore:
    numDownloadWorkers: 1
    maxDownloadBufferMb: 64
    downloadChunkMb: 8
`

func TestRenderConfigGolden(t *testing.T) {
	env, _, _, _ := setup(t)
	rendered, err := RenderConfig(env)
	if err != nil {
		t.Fatal(err)
	}
	if string(rendered) != configGolden {
		t.Fatalf("config golden mismatch:\n%s", rendered)
	}
	env.Storage.VerifyTLS = false
	env.Storage.ForcePathStyle = false
	env.Storage.Region = "eu-central-1"
	env.PITR.Enabled = true
	env.PITR.OplogSpanMinutes = 5
	rendered, err = RenderConfig(env)
	if err != nil {
		t.Fatal(err)
	}
	golden := strings.NewReplacer("insecureSkipTLSVerify: false", "insecureSkipTLSVerify: true", "forcePathStyle: true", "forcePathStyle: false", "region: us-east-1", "region: eu-central-1", "enabled: false", "enabled: true", "oplogSpanMin: 1", "oplogSpanMin: 5").Replace(configGolden)
	if string(rendered) != golden {
		t.Fatalf("custom config mismatch:\n%s", rendered)
	}
	env.Storage.Type = "filesystem"
	if _, err = RenderConfig(env); err == nil {
		t.Fatal("unsupported storage accepted")
	}
}
