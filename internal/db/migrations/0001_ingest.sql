-- Sync position per source (vow, idx, offmls).
CREATE TABLE ingest_cursors (
    source     text PRIMARY KEY,
    cursor     jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Last version of each listing as each source showed it. Used to drop
-- unchanged and stale records before they become events.
CREATE TABLE source_listings (
    source       text        NOT NULL,
    listing_key  text        NOT NULL,
    content_hash text        NOT NULL,
    modified_at  timestamptz NOT NULL,
    present      boolean     NOT NULL,
    listing      jsonb,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, listing_key)
);

CREATE INDEX source_listings_present ON source_listings (source) WHERE present;
