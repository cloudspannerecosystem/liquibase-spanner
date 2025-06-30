CREATE SCHEMA newSchema
CREATE TABLE newSchema.createTableSchema (id bigint NOT NULL, textCol varchar, PRIMARY KEY (id))
CREATE SEQUENCE newSchema.test_sequence bit_reversed_positive START COUNTER WITH 100