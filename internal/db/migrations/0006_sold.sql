-- Sold archive: every closed for-sale listing in scope (Ottawa by default),
-- with the full record, kept for valuation research. Rows are never deleted.
-- A later version of the same listing (a corrected price, a status change)
-- replaces the record; a relist is a new ListingKey and so a new row.
--
-- PropTx VOW data: it stays on this server. Nothing here is published.
CREATE TABLE sold_listings (
    listing_key       text PRIMARY KEY,
    county            text        NOT NULL DEFAULT '',
    mls_status        text        NOT NULL DEFAULT '', -- Sold, Deal Fell Through, ...
    property_sub_type text        NOT NULL DEFAULT '',
    close_price       numeric,
    close_date        date,       -- closing day (can be months after the sale)
    contract_date     date,       -- PurchaseContractDate: when the sale went firm
    modified_at       timestamptz NOT NULL,
    record            jsonb       NOT NULL, -- the RESO fields as PropTx sent them
    lat               double precision,
    lng               double precision,
    geo_precision     text,       -- feed | address | fsa | NULL (not located)
    first_seen        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sold_listings_county_contract ON sold_listings (county, contract_date);
CREATE INDEX sold_listings_county_close ON sold_listings (county, close_date);

-- Sync position of the sold archive (a reso.Cursor as JSON).
CREATE TABLE sold_state (
    k text PRIMARY KEY,
    v text NOT NULL
);
