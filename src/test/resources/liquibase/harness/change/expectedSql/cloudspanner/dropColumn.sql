ALTER TABLE posts ADD varcharColumn STRING(25)
UPDATE posts SET varcharColumn = 'INITIAL_VALUE' WHERE TRUE
ALTER TABLE posts DROP COLUMN varcharColumn