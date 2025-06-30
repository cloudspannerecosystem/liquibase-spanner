CREATE TABLE modify_data_type_test (id bigint NOT NULL, stringColumn varchar(50), dateColumn date, PRIMARY KEY (id))
ALTER TABLE modify_data_type_test ALTER COLUMN stringColumn TYPE bytea

