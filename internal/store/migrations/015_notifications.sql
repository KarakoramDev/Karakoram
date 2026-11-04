CREATE TABLE notification_deliveries (
    channel TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','delivered','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt INTEGER NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created INTEGER NOT NULL,
    updated INTEGER NOT NULL,
    delivered_at INTEGER,
    error_at INTEGER,
    PRIMARY KEY(channel,event_id)
);
CREATE INDEX notification_deliveries_due ON notification_deliveries(channel,state,next_attempt);
CREATE INDEX notification_deliveries_failed ON notification_deliveries(state,channel);
