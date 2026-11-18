-- One row per alert to deliver: the delivery service's durable queue.
--   pending  waiting for next_at (coalescing, rate limit, or retry backoff)
--   sending  claimed by a sender until lease_until
--   sent     delivered
--   failed   permanent error or out of retries (also sent to alerts.dlq)
CREATE TABLE deliveries (
    alert_id    text PRIMARY KEY,
    -- One email per (search, listing, what happened, at what price): a second
    -- alert for the same thing (e.g. re-detected after a replay) is dropped.
    dedup_key   text        NOT NULL UNIQUE,
    site        text        NOT NULL,
    user_id     text        NOT NULL,
    channel     text        NOT NULL DEFAULT 'email',
    alert       jsonb       NOT NULL,
    status      text        NOT NULL DEFAULT 'pending',
    attempts    int         NOT NULL DEFAULT 0,
    next_at     timestamptz NOT NULL,
    lease_until timestamptz,
    last_error  text,
    message_id  text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    sent_at     timestamptz
);
CREATE INDEX deliveries_due ON deliveries (next_at) WHERE status = 'pending';
CREATE INDEX deliveries_leased ON deliveries (lease_until) WHERE status = 'sending';
CREATE INDEX deliveries_user ON deliveries (site, user_id, created_at);
