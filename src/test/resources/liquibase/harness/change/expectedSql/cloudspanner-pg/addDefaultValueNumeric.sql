ALTER TABLE authors ADD numericColumn NUMERIC
ALTER TABLE authors ALTER COLUMN numericColumn SET DEFAULT (numeric '100.0')