
-- To run Liquibase Harness test suite:
-- 1. Start Cloud Spanner emulator with `docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator`
-- 2. Execute the script below to initialize a database on projects/my-project/instances/my-instance/databases/my-database
-- 3. Execute test suite

START BATCH DDL;
CREATE TABLE authors (
                         id   INT64 NOT NULL,
                         first_name  STRING(1024),
                         last_name   STRING(1024),
                         email   STRING(1024),
                         birthdate  DATE,
                         added timestamp NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(id);
CREATE TABLE posts (
                       id   INT64 NOT NULL,
                       author_id  INT64 NOT NULL,
                       title   STRING(1024),
                       description   STRING(1024),
                       content   STRING(1024),
                       inserted_date  DATE
) PRIMARY KEY(id);
RUN BATCH;

START BATCH DML;
INSERT INTO authors (id, first_name, last_name, email, birthdate, added) VALUES
(1,'Eileen','Lubowitz','ppaucek@example.org','1991-03-04','2004-05-30 02:08:25'),
(2,'Tamia','Mayert','shansen@example.org','2016-03-27','2014-03-21 02:52:00'),
(3,'Cyril','Funk','reynolds.godfrey@example.com','1988-04-21','2011-06-24 18:17:48'),
(4,'Nicolas','Buckridge','xhoeger@example.net','2017-02-03','2019-04-22 02:04:41'),
(5,'Jayden','Walter','lillian66@example.com','2010-02-27','1990-02-04 02:32:00');
INSERT INTO posts (id, author_id, title, description, content, inserted_date) VALUES
(1,1,'temporibus','voluptatum','Fugit non et doloribus repudiandae.','2015-11-18'),
(2,2,'ea','aut','Tempora molestias maiores provident molestiae sint possimus quasi.','1975-06-08'),
(3,3,'illum','rerum','Delectus recusandae sit officiis dolor.','1975-02-25'),
(4,4,'itaque','deleniti','Magni nam optio id recusandae.','2010-07-28'),
(5,5,'ad','similique','Rerum tempore quis ut nesciunt qui excepturi est.','2006-10-09');
RUN BATCH;
