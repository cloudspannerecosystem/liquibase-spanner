ALTER TABLE posts ADD default_uuid varchar(36)
ALTER TABLE posts ALTER COLUMN default_uuid SET DEFAULT (spanner.generate_uuid())