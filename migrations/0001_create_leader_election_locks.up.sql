CREATE TABLE leader_election_locks (
                                       namespace   text        NOT NULL,
                                       name        text        NOT NULL,
                                       record_json jsonb       NOT NULL,
                                       version     bigint      NOT NULL DEFAULT 1,
                                       updated_at  timestamptz NOT NULL DEFAULT now(),
                                       PRIMARY KEY (namespace, name)
);

CREATE INDEX idx_leader_election_locks_updated_at
    ON leader_election_locks (updated_at);