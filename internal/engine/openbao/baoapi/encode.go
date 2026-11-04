// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package baoapi

import (
	"bytes"
	"encoding/json"
	"io"
)

func jsonReader(v any) io.Reader {
	data, err := json.Marshal(v)
	if err != nil {
		// Only fixed request structs are encoded here; they always marshal.
		panic(err)
	}
	return bytes.NewReader(data)
}
