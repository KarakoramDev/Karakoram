// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/notify"
)

func (s *Server) ListNotificationChannels(ctx context.Context, request api.ListNotificationChannelsRequestObject) (api.ListNotificationChannelsResponseObject, error) {
	return s.handle(ctx, "ListNotificationChannels", input{}), nil
}
func (r response) VisitListNotificationChannelsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) CreateNotificationChannel(ctx context.Context, request api.CreateNotificationChannelRequestObject) (api.CreateNotificationChannelResponseObject, error) {
	return s.handle(ctx, "CreateNotificationChannel", input{Body: request.Body}), nil
}
func (r response) VisitCreateNotificationChannelResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) GetNotificationChannel(ctx context.Context, request api.GetNotificationChannelRequestObject) (api.GetNotificationChannelResponseObject, error) {
	return s.handle(ctx, "GetNotificationChannel", input{Name: request.Channel}), nil
}
func (r response) VisitGetNotificationChannelResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) ReplaceNotificationChannel(ctx context.Context, request api.ReplaceNotificationChannelRequestObject) (api.ReplaceNotificationChannelResponseObject, error) {
	return s.handle(ctx, "ReplaceNotificationChannel", input{Name: request.Channel, Body: request.Body}), nil
}
func (r response) VisitReplaceNotificationChannelResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) DeleteNotificationChannel(ctx context.Context, request api.DeleteNotificationChannelRequestObject) (api.DeleteNotificationChannelResponseObject, error) {
	return s.handle(ctx, "DeleteNotificationChannel", input{Name: request.Channel}), nil
}
func (r response) VisitDeleteNotificationChannelResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) TestNotificationChannel(ctx context.Context, request api.TestNotificationChannelRequestObject) (api.TestNotificationChannelResponseObject, error) {
	return s.handle(ctx, "TestNotificationChannel", input{Name: request.Channel}), nil
}
func (r response) VisitTestNotificationChannelResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) notificationChannels(ctx context.Context, operation string, in input) response {
	switch operation {
	case "ListNotificationChannels":
		items, err := s.Store.ListNotificationChannels(ctx)
		return listResponse(s, ctx, items, err)
	case "GetNotificationChannel":
		channel, err := s.Store.GetNotificationChannel(ctx, in.Name)
		if err != nil {
			return s.failure(err)
		}
		return ok(channel)
	case "DeleteNotificationChannel":
		if err := s.Store.DeleteNotificationChannel(ctx, in.Name); err != nil {
			return s.failure(err)
		}
		return response{status: http.StatusNoContent}
	case "TestNotificationChannel":
		channel, err := s.Store.NotificationChannelForDelivery(ctx, in.Name)
		if err != nil {
			return s.failure(err)
		}
		var identity [8]byte
		if _, err := rand.Read(identity[:]); err != nil {
			return s.failure(err)
		}
		// Negative ephemeral IDs cannot collide with positive durable event IDs.
		id := int64(binary.BigEndian.Uint64(identity[:]) | (1 << 63))
		event := domain.Event{ID: id, Type: "Normal", Reason: "NotificationTest", Message: "Karakoram notification test", CreatedAt: time.Now().UTC()}
		err = notify.NewSender().Send(ctx, *channel, event, domain.NotificationOperationFailed)
		result := map[string]any{"delivered": err == nil}
		if err != nil {
			result["error"] = "notification delivery failed"
		}
		return ok(result)
	case "CreateNotificationChannel", "ReplaceNotificationChannel":
		body := in.Body.(*api.NotificationChannel)
		raw, err := json.Marshal(body)
		if err != nil {
			return s.failure(err)
		}
		var channel domain.NotificationChannel
		if err = json.Unmarshal(raw, &channel); err != nil {
			return s.failure(err)
		}
		// Read-only values from input are never persisted.
		channel.Status = domain.NotificationChannelStatus{}
		if body.Webhook != nil {
			if body.Webhook.Url != nil {
				channel.Secrets.WebhookURL = *body.Webhook.Url
			}
			if body.Webhook.SigningSecret != nil {
				channel.Secrets.WebhookSigningSecret = *body.Webhook.SigningSecret
			}
		}
		if body.Slack != nil && body.Slack.WebhookUrl != nil {
			channel.Secrets.SlackWebhookURL = *body.Slack.WebhookUrl
		}
		if body.Email != nil && body.Email.Password != nil {
			channel.Secrets.EmailPassword = *body.Email.Password
		}
		create := operation == "CreateNotificationChannel"
		var previous *domain.NotificationChannel
		if !create {
			if channel.Name != in.Name {
				return fail(400, "validation_failed", "channel name must match the path")
			}
			previous, err = s.Store.NotificationChannelForDelivery(ctx, in.Name)
			if err != nil {
				return s.failure(err)
			}
		}
		if err = notify.Prepare(&channel, previous); err != nil {
			return fail(400, "validation_failed", err.Error())
		}
		channel.CreatedAt = time.Now().UTC()
		channel.UpdatedAt = channel.CreatedAt
		if previous != nil {
			channel.CreatedAt = previous.CreatedAt
		}
		if err = s.Store.SaveNotificationChannel(ctx, &channel, create); err != nil {
			return s.failure(err)
		}
		channel.Secrets = domain.NotificationChannelSecrets{}
		if create {
			return response{status: http.StatusCreated, body: channel}
		}
		return ok(channel)
	}
	return fail(500, "internal_error", "Unknown notification operation")
}
