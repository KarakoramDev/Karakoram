// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mongoadmin

import (
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// ReadError classifies temporary database connectivity/topology failures without
// changing authentication, authorization or non-transient command errors.
// mongo.IsNetworkError and mongo.IsTimeout inspect wrapped errors and labels;
// driver.Error.RetryableRead supplies the driver's transient read code set.
func ReadError(err error) error {
	if err == nil || authenticationError(err) {
		return err
	}
	var selection topology.ServerSelectionError
	selecting := errors.As(err, &selection)
	if selecting {
		// Selection can hide handshake authentication failures in its snapshot.
		for _, server := range selection.Desc.Servers {
			if authenticationError(server.LastError) {
				return err
			}
		}
	}
	if selecting || transientReadError(err) {
		return fmt.Errorf("MongoDB read unavailable; check mongos router or replica-set connectivity: %w: %w", engine.ErrUnavailable, err)
	}
	return err
}

func transientReadError(err error) bool {
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}
	var native driver.Error
	if errors.As(err, &native) && native.RetryableRead() {
		return true
	}
	var command mongo.CommandError
	return errors.As(err, &command) &&
		(driver.Error{Code: command.Code, Labels: command.Labels}).RetryableRead()
}

func authenticationError(err error) bool {
	var authentication *auth.Error
	var native driver.Error
	var server mongo.ServerError
	if (errors.As(err, &native) && (native.Code == 13 || native.Code == 18)) ||
		(errors.As(err, &server) && (server.HasErrorCode(13) || server.HasErrorCode(18))) {
		return true
	}
	// The driver also wraps failed hello/SASL network exchanges in auth.Error.
	// Inspect its cause before treating the wrapper as a credential failure.
	return errors.As(err, &authentication) && !transientReadError(authentication.Inner())
}
