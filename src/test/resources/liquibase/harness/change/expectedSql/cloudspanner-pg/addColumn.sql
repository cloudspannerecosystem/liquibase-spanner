ALTER TABLE authors ADD varcharColumn varchar(25)
ALTER TABLE authors ADD intColumn bigint
ALTER TABLE authors ADD dateColumn date
UPDATE authors SET varcharColumn = 'INITIAL_VALUE' WHERE TRUE
UPDATE authors SET intColumn = 5 WHERE TRUE
UPDATE authors SET dateColumn = '2020-09-21'::date WHERE TRUE