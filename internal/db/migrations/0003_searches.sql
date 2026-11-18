-- Saved searches, written by the API, read by matcher shards.
CREATE TABLE saved_searches (
    id         text PRIMARY KEY,
    site       text        NOT NULL,
    user_id    text        NOT NULL,
    spec       jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted    boolean     NOT NULL DEFAULT false
);
CREATE INDEX saved_searches_user ON saved_searches (site, user_id) WHERE NOT deleted;
-- Matcher shards follow changes by updated_at.
CREATE INDEX saved_searches_updated ON saved_searches (updated_at, id);

-- Site API keys. Only a SHA-256 of each key is stored.
CREATE TABLE api_keys (
    key_hash   bytea PRIMARY KEY,
    site       text        NOT NULL,
    label      text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
