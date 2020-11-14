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

--changeset test.user:100 labels:rollback_stuff runInTransaction:FALSE
--preconditions onFail:HALT onError:HALT
START BATCH DDL;
create table rollback_table
(
    id   int64 NOT NULL,
    name string(255),
) PRIMARY KEY (id);
RUN BATCH;
--rollback DROP TABLE rollback_table;

