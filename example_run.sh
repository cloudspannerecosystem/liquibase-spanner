#!/bin/bash
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# NOTE: This assumes that you are using the Spanner emulator.
# See the instructions here:
# https://cloud.google.com/spanner/docs/emulator#using_the_gcloud_cli_with_the_emulator
#

# JDBC connection to Spanner
# (This uses the emulator running on localhost)
# NOTE: The Spanner database must be initialized by src/test/resources/initial.spanner.sql
JDBC_URL="jdbc:cloudspanner://127.0.0.1:9010/projects/projectid/instances/test-instance/databases/test?autocommit=false;usePlainText=true"

# Directory of the BASH script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Build shadowJar
gradle build shadowJar

# Run with Liquibase with the shadow jar
liquibase \
  --changeLogFile="${DIR}/src/test/resources/changelog.spanner.sql" \
  --classpath="${DIR}/build/libs/shadow-0.1-SNAPSHOT-all.jar" \
  --url="${JDBC_URL}" \
  --driver=com.google.cloud.spanner.jdbc.JdbcDriver \
  --databaseClass=liquibase.ext.spanner.CloudSpanner \
  --logLevel=INFO \
  updateSQL

# Build jibDocker
gradle build jibDocker

# Run with docker (Liquibase & Spanner self-contained)
# - Host network to call into localhost (if needed)
# - Mount local directory for pulling in sample changelog
docker run \
  --network="host" \
  -v "${DIR}/src/test/resources:/sql" \
  spanner-liquibase \
  --changeLogFile=/sql/changelog.spanner.sql \
  --username=  \
  --password= \
  --url="${JDBC_URL}" \
  --logLevel=INFO \
  updateSQL

