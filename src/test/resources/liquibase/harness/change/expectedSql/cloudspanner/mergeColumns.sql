CREATE TABLE full_name_table (id INT64 NOT NULL, first_name STRING(50), last_name STRING(50)) PRIMARY KEY (id)
INSERT INTO full_name_table (id, first_name) VALUES (1, 'John')
UPDATE full_name_table SET last_name = 'Doe' WHERE first_name='John'
INSERT INTO full_name_table (id, first_name) VALUES (2, 'Jane')
UPDATE full_name_table SET last_name = 'Doe' WHERE first_name='Jane'
ALTER TABLE full_name_table ADD full_name STRING(255)
SET AUTOCOMMIT=TRUE
SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'
UPDATE full_name_table SET full_name = first_name || ' ' || last_name WHERE TRUE
SET AUTOCOMMIT_DML_MODE='TRANSACTIONAL'
ALTER TABLE full_name_table DROP COLUMN first_name
ALTER TABLE full_name_table DROP COLUMN last_name