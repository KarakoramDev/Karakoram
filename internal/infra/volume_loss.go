// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package infra

import "fmt"

// VolumeLostError identifies confirmed physical loss behind an owned logical
// volume record. It is not used for API failures, unavailable nodes, mounts or
// unverified foreign services. Engines may replace the member through their
// native safety workflow; backends must never manufacture empty storage for it.
type VolumeLostError struct {
	Volume    string
	ServiceID string
	Message   string
}

func (e *VolumeLostError) Error() string {
	return fmt.Sprintf("%s: volume %s: %s", ErrConflict, e.Volume, e.Message)
}
func (e *VolumeLostError) Is(target error) bool { return target == ErrConflict }
func (e *VolumeLostError) SafeDetail() string   { return e.Error() }
