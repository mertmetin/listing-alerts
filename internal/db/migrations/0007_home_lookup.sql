-- Home lookups (GET /v1/homes/lookup) match on the street number first.
CREATE INDEX sold_listings_street_number ON sold_listings (county, (record->>'StreetNumber'));
CREATE INDEX hub_listings_street_number ON hub_listings (county, (record->>'StreetNumber')) WHERE present;
