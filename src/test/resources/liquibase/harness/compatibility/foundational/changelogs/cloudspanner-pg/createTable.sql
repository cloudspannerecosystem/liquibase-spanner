--liquibase formatted sql

--changeset as:4 -context:test_context -labels:test_label
--comment: test_comment
CREATE TABLE test_table_sql (test_column bigint, primary key (test_column));

--rollback DROP TABLE test_table_sql;