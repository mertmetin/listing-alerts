-- Off-market archive: for-sale listings that ended without selling
-- (Expired, Cancelled = Terminated, Withdrawn = Suspended), kept for the
-- days-on-market research (ottawa-home-value). Together with sold_listings it
-- is every listing's outcome. Same rules as the sold archive: rows are never
-- deleted, a later version replaces the record, a relist is a new ListingKey.
--
-- PropTx VOW data: it stays on this server. Nothing here is published.
CREATE TABLE offmarket_listings (
    listing_key         text PRIMARY KEY,
    county              text        NOT NULL DEFAULT '',
    standard_status     text        NOT NULL DEFAULT '', -- Expired, Cancelled, Withdrawn
    mls_status          text        NOT NULL DEFAULT '', -- Expired, Terminated, Suspended
    property_sub_type   text        NOT NULL DEFAULT '',
    list_price          numeric,
    original_list_price numeric,
    list_date           date,       -- ListingContractDate
    off_market_date     date,       -- ExpirationDate if expired, else list date + DaysOnMarket
    days_on_market      integer,    -- PropTx DaysOnMarket (this listing only, not across relists)
    modified_at         timestamptz NOT NULL,
    record              jsonb       NOT NULL,
    lat                 double precision,
    lng                 double precision,
    geo_precision       text,
    first_seen          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX offmarket_listings_county_list ON offmarket_listings (county, list_date);
CREATE INDEX offmarket_listings_street_number ON offmarket_listings (county, (record->>'StreetNumber'));

CREATE TABLE offmarket_state (
    k text PRIMARY KEY,
    v text NOT NULL
);

-- Status and price changes as the VOW poller sees them: one row each time a
-- listing's status or list price differs from the version stored before.
-- PropTx keeps only the last price change per listing, so this is the only
-- full price-drop history, from 2026-11 on.
CREATE TABLE listing_history (
    source          text        NOT NULL,
    listing_key     text        NOT NULL,
    at              timestamptz NOT NULL, -- the record's ModificationTimestamp
    standard_status text        NOT NULL DEFAULT '',
    mls_status      text        NOT NULL DEFAULT '',
    list_price      numeric,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, listing_key, at)
);
CREATE INDEX listing_history_key ON listing_history (listing_key, at);

-- Seed: what every listing looks like now, so the first change has a "before".
INSERT INTO listing_history (source, listing_key, at, standard_status, mls_status, list_price)
SELECT source, listing_key, modified_at, coalesce(listing->>'standard_status', ''), coalesce(listing->>'mls_status', ''),
       (listing->>'list_price')::numeric
FROM source_listings WHERE source = 'vow' AND listing IS NOT NULL
ON CONFLICT DO NOTHING;
