CREATE TABLE full_name_table (id bigint NOT NULL, first_name varchar(50), last_name varchar(50), PRIMARY KEY (id))
INSERT INTO full_name_table (id, first_name) VALUES (1, 'John')
UPDATE full_name_table SET last_name = 'Doe' WHERE first_name='John'
INSERT INTO full_name_table (id, first_name) VALUES (2, 'Jane')
UPDATE full_name_table SET last_name = 'Doe' WHERE first_name='Jane'
ALTER TABLE full_name_table ADD full_name varchar(255)
set autocommit=true
set spanner.autocommit_dml_mode='partitioned_non_atomic'
UPDATE full_name_table SET full_name = first_name || ' ' || last_name WHERE TRUE
set spanner.autocommit_dml_mode='transactional'
ALTER TABLE full_name_table DROP COLUMN first_name
ALTER TABLE full_name_table DROP COLUMN last_name