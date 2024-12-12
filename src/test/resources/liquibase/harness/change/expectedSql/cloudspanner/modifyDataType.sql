CREATE TABLE modify_data_type_test (id INT64 NOT NULL, stringColumn STRING(50), dateColumn date) PRIMARY KEY (id)
ALTER TABLE modify_data_type_test ALTER COLUMN stringColumn BYTES(50)