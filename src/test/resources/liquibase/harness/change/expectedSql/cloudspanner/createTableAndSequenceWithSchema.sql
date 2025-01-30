CREATE SCHEMA newSchema
CREATE TABLE newSchema.createTableSchema (id INT64 NOT NULL, textCol STRING(MAX)) PRIMARY KEY (id)
CREATE SEQUENCE newSchema.test_sequence OPTIONS (sequence_kind='bit_reversed_positive', start_with_counter = 100)