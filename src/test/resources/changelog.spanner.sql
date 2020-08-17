--liquibase formatted sql

--ignoreLines:start
--
-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
--ignoreLines:end


--changeset test.user:1 labels:base,feature1
--preconditions onFail:HALT onError:HALT
START BATCH DDL;
create table table1
(
    id   int64,
    name string(MAX),
) PRIMARY KEY (id);
RUN BATCH;
INSERT INTO table1(id, name) VALUES (1, 'test');
--rollback DROP TABLE table1;

--changeset test.user:2 labels:base,feature2
--preconditions onFail:HALT onError:HALT
START BATCH DDL;
ALTER TABLE table1
    ADD COLUMN extra string(20);
RUN BATCH;
--rollback DROP COLUMN table1;

--changeset test.user:3 labels:base,feature3
--preconditions onFail:HALT onError:HALT
UPDATE table1
SET extra = 'abc' WHERE true;
