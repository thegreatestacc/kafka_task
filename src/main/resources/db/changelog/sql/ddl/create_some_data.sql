CREATE
EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS some_data
(
    id
    UUID
    NOT
    NULL,
    value
    TEXT,
    CONSTRAINT
    some_data_pkey
    PRIMARY
    KEY
(
    id
)
    );
