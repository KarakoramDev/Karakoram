// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/KarakoramDev/Karakoram/pkg/client"
)

func ExampleNewClientWithResponses() {
	// Replace this origin with https://<host>/api/v1. Keep the /api/v1 suffix.
	c, err := client.NewClientWithResponses("https://karakoram.example/api/v1",
		client.WithBearerToken(os.Getenv("KARAKORAM_TOKEN")),
		client.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Supply the operation ID returned by an accepted mutation, not a cluster name.
	if err := listAndWait(ctx, c, os.Getenv("KARAKORAM_OPERATION_ID")); err != nil {
		log.Fatal(err)
	}
}

func listAndWait(ctx context.Context, c *client.ClientWithResponses, operationID string) error {
	response, err := c.ListClustersWithResponse(ctx, nil)
	if err != nil {
		return err // Transport, context or response-decoding failure.
	}
	if err := responseError(response.StatusCode(), response.Body); err != nil {
		return err // An HTTP error is separate from a transport error.
	}
	if response.JSON200 == nil {
		return fmt.Errorf("unexpected cluster response: %s", response.Status())
	}
	for _, cluster := range response.JSON200.Items {
		fmt.Println(cluster.Name)
	}
	// Lists are paginated; follow JSON200.NextCursor to fetch subsequent pages.
	if operationID == "" {
		return fmt.Errorf("supply the operation ID returned by an accepted mutation")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		response, err := c.GetOperationWithResponse(ctx, operationID, nil)
		if err != nil {
			return err
		}
		if err := responseError(response.StatusCode(), response.Body); err != nil {
			return err
		}
		if response.JSON200 == nil {
			return fmt.Errorf("unexpected operation response: %s", response.Status())
		}
		op := response.JSON200
		switch string(op.Phase) {
		case "Succeeded":
			return nil
		case "Failed", "Cancelled":
			message := ""
			if op.Message != nil {
				message = *op.Message
			}
			if op.Error != nil {
				return fmt.Errorf("operation %s %s: %s: %s", op.Id, op.Phase, op.Error.Code, op.Error.Message)
			}
			return fmt.Errorf("operation %s %s: %s", op.Id, op.Phase, message)
		}
		select {
		case <-ctx.Done():
			return ctx.Err() // This does not cancel server work; resume polling the same ID.
		case <-ticker.C:
		}
	}
}

func ExampleClientWithResponses_GetClusterMetrics() {
	c, err := client.NewClientWithResponses("https://karakoram.example/api/v1",
		client.WithBearerToken(os.Getenv("KARAKORAM_TOKEN")),
		client.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
	if err != nil {
		log.Fatal(err)
	}
	metrics, err := clusterMetrics(context.Background(), c)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(metrics.Series))
}

func clusterMetrics(ctx context.Context, c *client.ClientWithResponses) (*client.ClusterMetrics, error) {
	namespace, lookback, step := "default", "1h", "30s"
	response, err := c.GetClusterMetricsWithResponse(ctx, "demo", &client.GetClusterMetricsParams{
		Namespace: &namespace, Range: &lookback, Step: &step,
	})
	if err != nil {
		return nil, err
	}
	if err := responseError(response.StatusCode(), response.Body); err != nil {
		return nil, err
	}
	if response.JSON200 == nil {
		return nil, fmt.Errorf("unexpected metrics response: %s", response.Status())
	}
	return response.JSON200, nil
}

func responseError(status int, body []byte) error {
	if status >= 200 && status < 300 {
		return nil
	}
	var apiError client.Error
	if err := json.Unmarshal(body, &apiError); err != nil || apiError.Code == "" {
		return fmt.Errorf("HTTP %d: %s", status, http.StatusText(status))
	}
	var details map[string]interface{}
	if apiError.Details != nil {
		details = *apiError.Details
	}
	// Branch on apiError.Code (and details["field"]/details["reason"]), not message text.
	return fmt.Errorf("HTTP %d: %s: %s (details: %v)", status, apiError.Code, apiError.Message, details)
}
