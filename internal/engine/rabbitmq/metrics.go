// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func (*Provider) Metrics() []domain.MetricDescriptor {
	names := []string{"up", "uptimeSeconds", "connections", "channels", "consumers", "queues", "messagesReady", "messagesUnacked", "published", "delivered", "memoryAlarm", "diskAlarm", "residentMemoryBytes", "memoryLimitBytes", "diskFreeBytes", "diskFreeLimitBytes", "clusterMembersRunning"}
	out := make([]domain.MetricDescriptor, len(names))
	for i, name := range names {
		unit := "count"
		switch i {
		case 1:
			unit = "seconds"
		case 8, 9:
			unit = "messages/second"
		case 12, 13, 14, 15:
			unit = "bytes"
		}
		out[i] = domain.MetricDescriptor{Exposition: rmqspec.MetricNames[i], Name: "rabbitmq." + name, Unit: unit, Scale: 1}
	}
	return out
}

var _ engine.MetricsCatalog = (*Provider)(nil)
