CREATE TABLE sqltest (id bigint NOT NULL, PRIMARY KEY (id))
start batch dml
insert into sqltest (id) values (1)
insert into sqltest (id) values (2)
insert into sqltest (id) values (3)
run batch