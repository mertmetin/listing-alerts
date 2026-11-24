-- The listing hub: full IDX records (what the public sites display), with
-- photos and rooms, served to mertmetin.ca and hotrentals.ca through a
-- changes feed ordered by seq.
--
-- One writer (the IDX poller) assigns seq inside its commit, so seq order is
-- commit order and a reader that pages by seq never skips a row.
CREATE SEQUENCE hub_seq;

CREATE TABLE hub_listings (
    listing_key  text PRIMARY KEY,
    seq          bigint      NOT NULL,
    present      boolean     NOT NULL,
    modified_at  timestamptz NOT NULL,
    content_hash text        NOT NULL, -- over the whole record, so remarks or photo changes count
    photos_at    timestamptz,          -- PhotosChangeTimestamp the photos were fetched for
    county       text        NOT NULL DEFAULT '',
    transaction  text        NOT NULL DEFAULT '',
    record       jsonb       NOT NULL, -- the RESO fields as PropTx sent them
    photos       jsonb       NOT NULL DEFAULT '[]', -- [{large, medium, thumb}], preferred first
    rooms        jsonb       NOT NULL DEFAULT '[]',
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX hub_listings_seq ON hub_listings (seq);
CREATE INDEX hub_listings_county_seq ON hub_listings (county, seq);

-- Backfill progress (the hub can be added to a poller that is already past
-- its own baseline).
CREATE TABLE hub_state (
    k text PRIMARY KEY,
    v text NOT NULL
);
