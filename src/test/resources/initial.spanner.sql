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

CREATE TABLE DATABASECHANGELOG
(
    id            string(MAX) not null,
    author        string(MAX) not null,
    filename      string(MAX) not null,
    dateExecuted  timestamp   not null,
    orderExecuted int64       not null,
    execType      string(MAX),
    md5sum        string(MAX),
    description   string(MAX),
    comments      string(MAX),
    tag           string(MAX),
    liquibase     string(MAX),
    contexts      string(MAX),
    labels        string(MAX),
    deployment_id string(MAX),
) primary key (id, author, filename);

CREATE TABLE DATABASECHANGELOGLOCK
(
    id          int64,
    locked      bool,
    lockgranted timestamp,
    lockedby    string(max),
) primary key (id)
