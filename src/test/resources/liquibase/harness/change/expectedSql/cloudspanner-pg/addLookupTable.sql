CREATE TABLE authors_data (authors_email varchar(100) NOT NULL, PRIMARY KEY (authors_email))
INSERT INTO authors_data (authors_email) SELECT DISTINCT email FROM authors WHERE email IS NOT NULL
ALTER TABLE authors ADD CONSTRAINT FK_AUTHORS_AUTHORS_DATA FOREIGN KEY (email) REFERENCES authors_data (authors_email)