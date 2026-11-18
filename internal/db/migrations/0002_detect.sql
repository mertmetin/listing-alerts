-- What the change detector knows about each listing, across sources.
-- version is bumped on every write; writers update only the version they
-- read (optimistic concurrency), so two workers that briefly own the same
-- partition during a rebalance cannot both win.
CREATE TABLE listing_state (
    listing_key text PRIMARY KEY,
    version     bigint      NOT NULL,
    state       jsonb       NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
