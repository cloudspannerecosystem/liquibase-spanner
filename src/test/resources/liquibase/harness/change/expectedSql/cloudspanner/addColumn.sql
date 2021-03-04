ALTER TABLE authors ADD COLUMN varcharColumn STRING(25)
ALTER TABLE authors ADD COLUMN intColumn INT64
ALTER TABLE authors ADD COLUMN dateColumn date
UPDATE authors SET varcharColumn = 'INITIAL_VALUE' WHERE TRUE
UPDATE authors SET intColumn = 5 WHERE TRUE
UPDATE authors SET dateColumn = DATE '2020-09-21' WHERE TRUE