// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"testing"
	"time"
)

func absenceCursor(collection string, docs ...bson.D) bson.D {
	batch := bson.A{}
	for _, doc := range docs {
		batch = append(batch, doc)
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "admin." + collection}, {Key: "firstBatch", Value: batch}}}}
}

// absenceWireClient uses the native net.Pipe fixture rather than the driver's
// deprecated deployment override. replacementMongo handles handshake/auth and
// explicit hello itself; the third response below remains that exact hello.
func absenceWireClient(t *testing.T, opts *options.ClientOptions, responses ...bson.D) *mongo.Client {
	t.Helper()
	auth, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) {
		return scram.StoredCredentials{}, errors.New("unexpected authentication in absence fixture")
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &replacementMongo{t: t, auth: auth, hello: bson.M{}}
	for _, element := range responses[2] {
		server.hello[element.Key] = element.Value
	}
	next := 0
	server.command = func(bson.Raw) bson.M {
		server.mu.Lock()
		defer server.mu.Unlock()
		if next == 2 {
			next++ // hello is answered by replacementMongo's native dispatcher.
		}
		if next >= len(responses) {
			t.Error("unexpected command after absence fixture response sequence")
			return bson.M{"ok": 0, "code": 59, "errmsg": "absence fixture response sequence exhausted"}
		}
		response := bson.M{}
		for _, element := range responses[next] {
			response[element.Key] = element.Value
		}
		next++
		return response
	}
	client, err := mongo.Connect(opts.SetHosts([]string{"demom1:27017"}).SetDirect(true).SetDialer(server))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

func TestRestoreAbsenceConcreteObserverDoesNotInferHandlerExit(t *testing.T) {
	const ts = int64(1792568288)
	for _, scenario := range []string{"absent", "fresh-lock", "unrelated-lock", "native-metadata-present", "command-too-young"} {
		t.Run(scenario, func(t *testing.T) {
			id := bson.NewObjectID()
			age := int64(120)
			count := int64(0)
			locks := []bson.D{}
			switch scenario {
			case "fresh-lock":
				locks = append(locks, bson.D{{Key: "hb", Value: bson.Timestamp{T: uint32(ts + 119)}}, {Key: "opid", Value: id.Hex()}})
			case "unrelated-lock":
				locks = append(locks, bson.D{{Key: "hb", Value: bson.Timestamp{T: uint32(ts)}}, {Key: "opid", Value: "other"}})
			case "native-metadata-present":
				count = 1
			case "command-too-young":
				age = 59
			}
			client := absenceWireClient(t, options.Client(),
				absenceCursor("pbmCmd", bson.D{{Key: "n", Value: int64(1)}}),
				absenceCursor("pbmCmd", bson.D{{Key: "_id", Value: id}, {Key: "ts", Value: ts}}),
				bson.D{{Key: "ok", Value: 1}, {Key: "localTime", Value: time.Unix(ts+age, 0)}},
				absenceCursor("pbmRestores", bson.D{{Key: "n", Value: count}}),
				absenceCursor("pbmLock", locks...), absenceCursor("pbmLockOp"),
			)
			stamp, absent, err := observeRestoreAbsence(t.Context(), client, "native-name")
			if err != nil || stamp != ts || absent != (scenario == "absent") {
				t.Fatal(stamp, absent, err)
			}
			// No log/history/process-birth query exists. Even a positive result is
			// merely absence and must enter every-member active fencing, not recovery.
		})
	}
}

func TestRestoreAdmissionChecksAllPBMOperationsInBothLockCollections(t *testing.T) {
	const ts = int64(1792568288)
	for _, collection := range []string{"pbmLock", "pbmLockOp"} {
		for _, operation := range []string{"backup", "pitr", "replay", "resync", "unknown", "restore", "own-stale-restore", "stale-backup-same-opid"} {
			t.Run(collection+"/"+operation, func(t *testing.T) {
				id := bson.NewObjectID()
				lockType, opid, heartbeat := operation, "other-operation", uint32(ts+119)
				allowed := operation == "own-stale-restore"
				if allowed {
					lockType, opid, heartbeat = "restore", id.Hex(), uint32(ts)
				}
				if operation == "stale-backup-same-opid" {
					lockType, opid, heartbeat = "backup", id.Hex(), uint32(ts)
				}
				lock := bson.D{{Key: "type", Value: lockType}, {Key: "opid", Value: opid}, {Key: "hb", Value: bson.Timestamp{T: heartbeat}}}
				ordinary, operationLocks := []bson.D{}, []bson.D{}
				if collection == "pbmLock" {
					ordinary = append(ordinary, lock)
				} else {
					operationLocks = append(operationLocks, lock)
				}
				responses := []bson.D{
					absenceCursor("pbmCmd", bson.D{{Key: "n", Value: int64(1)}}),
					absenceCursor("pbmCmd", bson.D{{Key: "_id", Value: id}, {Key: "ts", Value: ts}}),
					bson.D{{Key: "ok", Value: 1}, {Key: "localTime", Value: time.Unix(ts+120, 0)}},
					absenceCursor("pbmRestores", bson.D{{Key: "n", Value: int64(0)}}),
					absenceCursor("pbmLock", ordinary...), absenceCursor("pbmLockOp", operationLocks...),
				}
				queried := map[string]bool{}
				opts := options.Client().SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, started *event.CommandStartedEvent) {
					if started.CommandName != "find" {
						return
					}
					name, _ := started.Command.Lookup("find").StringValueOK()
					if name != "pbmLock" && name != "pbmLockOp" {
						return
					}
					queried[name] = true
					filter, ok := started.Command.Lookup("filter").DocumentOK()
					if !ok || len(filter) != 5 {
						t.Error("lock admission filtered out conflicting PBM operation types", name, filter)
					}
				}})
				client := absenceWireClient(t, opts, responses...)
				_, absent, err := observeRestoreAbsence(t.Context(), client, "native-name")
				if err != nil || absent != allowed || !queried[collection] {
					t.Fatal("conflicting lock admitted or own stale restore rejected", absent, allowed, queried, err)
				}
			})
		}
	}
}

func TestRestoreHalfSecondKillAdmitsOnlyAgedOwnHeartbeat(t *testing.T) {
	// r19: initiation at 12:34:54.959Z, primary agent/init killed at
	// 12:34:55.488Z. No restore metadata or operation lock was published.
	const ts = int64(1792586094)
	for _, age := range []int64{59, 60, 61, 900} {
		t.Run((time.Duration(age) * time.Second).String(), func(t *testing.T) {
			id, err := bson.ObjectIDFromHex("6ad8b16ead5c920119c4a99c")
			if err != nil {
				t.Fatal(err)
			}
			client := absenceWireClient(t, options.Client(),
				absenceCursor("pbmCmd", bson.D{{Key: "n", Value: int64(1)}}),
				absenceCursor("pbmCmd", bson.D{{Key: "_id", Value: id}, {Key: "ts", Value: ts}}),
				bson.D{{Key: "ok", Value: 1}, {Key: "localTime", Value: time.Unix(ts+age, 0)}},
				absenceCursor("pbmRestores", bson.D{{Key: "n", Value: int64(0)}}),
				absenceCursor("pbmLock", bson.D{{Key: "type", Value: "restore"}, {Key: "opid", Value: id.Hex()}, {Key: "hb", Value: bson.Timestamp{T: uint32(ts + 1)}}}),
				absenceCursor("pbmLockOp"),
			)
			stamp, absent, err := observeRestoreAbsence(t.Context(), client, "2026-10-21T12:34:54.959019416Z")
			if err != nil || stamp != ts || absent != (age >= 61) {
				t.Fatal("stale own lock blocked fencing or fresh heartbeat admitted", age, stamp, absent, err)
			}
		})
	}
}
