#!/bin/bash
#
# Copyright 2015 - 2026 Delft University of Technology / Arcade Data Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

rootdir=$(dirname $(readlink -f ${BASH_SOURCE[0]}))/../..

# Parse commandline instructions (provided by Graphalytics).
while [[ $# -gt 1 ]]
  do
  key="$1"
  value="$2"

  case ${key} in

    --arcadedb-home)
      ARCADEDB_HOME="$value"
      shift;;

    --bolt-uri)
      BOLT_URI="$value"
      shift;;

    --http-uri)
      HTTP_URI="$value"
      shift;;

    --graph-name)
      GRAPH_NAME="$value"
      shift;;

    --input-vertex-path)
      INPUT_VERTEX_PATH="$value"
      shift;;

    --input-edge-path)
      INPUT_EDGE_PATH="$value"
      shift;;

    --output-path)
      OUTPUT_PATH="$value"
      shift;;

    --directed)
      DIRECTED="$value"
      shift;;

    --weighted)
      WEIGHTED="$value"
      shift;;

    *)
      echo "Error: invalid option: " "$key"
      exit 1
      ;;
  esac
  shift
done

BOLT_URI=${BOLT_URI:-bolt://localhost:7687}
HTTP_URI=${HTTP_URI:-http://localhost:2480}
DB_NAME=${GRAPH_NAME//[^a-zA-Z0-9_]/_}

rm -rf ${OUTPUT_PATH}
mkdir -p ${OUTPUT_PATH}

echo "Creating ArcadeDB database: ${DB_NAME}"

# Drop database if it exists (ignore errors)
curl -s -X POST "${HTTP_URI}/api/v1/server" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d "{\"command\": \"drop database ${DB_NAME}\"}" 2>/dev/null || true

# Create the database
curl -s -X POST "${HTTP_URI}/api/v1/server" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d "{\"command\": \"create database ${DB_NAME}\"}"

echo "Creating schema..."

# Create vertex type with VID property and index
curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d '{"language": "sql", "command": "CREATE VERTEX TYPE Vertex"}'

curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d '{"language": "sql", "command": "CREATE PROPERTY Vertex.VID LONG"}'

curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d '{"language": "sql", "command": "CREATE INDEX ON Vertex (VID) UNIQUE"}'

# Create edge type
curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d '{"language": "sql", "command": "CREATE EDGE TYPE EDGE"}'

if [ "${WEIGHTED}" = "true" ]; then
  curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
    -H "Content-Type: application/json" \
    -u "root:playwithdata" \
    -d '{"language": "sql", "command": "CREATE PROPERTY EDGE.WEIGHT DOUBLE"}'
fi

echo "Loading vertices from ${INPUT_VERTEX_PATH}..."

# Load vertices in batches via SQL
BATCH_SIZE=5000
BATCH=""
COUNT=0
while IFS= read -r line; do
  VID=$(echo "$line" | awk '{print $1}')
  if [ -z "$VID" ]; then continue; fi
  BATCH="${BATCH}CREATE VERTEX Vertex SET VID = ${VID};"
  COUNT=$((COUNT + 1))
  if [ $((COUNT % BATCH_SIZE)) -eq 0 ]; then
    curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
      -H "Content-Type: application/json" \
      -u "root:playwithdata" \
      -d "{\"language\": \"sqlscript\", \"command\": \"BEGIN;${BATCH}COMMIT;\"}" > /dev/null
    BATCH=""
    echo "  Loaded ${COUNT} vertices..."
  fi
done < "${INPUT_VERTEX_PATH}"

# Flush remaining vertices
if [ -n "$BATCH" ]; then
  curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
    -H "Content-Type: application/json" \
    -u "root:playwithdata" \
    -d "{\"language\": \"sqlscript\", \"command\": \"BEGIN;${BATCH}COMMIT;\"}" > /dev/null
fi
echo "  Loaded ${COUNT} vertices total."

echo "Loading edges from ${INPUT_EDGE_PATH}..."

# Load edges in batches via SQL
BATCH=""
COUNT=0
while IFS=' ' read -r SRC DST WEIGHT; do
  if [ -z "$SRC" ] || [ -z "$DST" ]; then continue; fi
  if [ "${WEIGHTED}" = "true" ] && [ -n "$WEIGHT" ]; then
    BATCH="${BATCH}CREATE EDGE EDGE FROM (SELECT FROM Vertex WHERE VID = ${SRC}) TO (SELECT FROM Vertex WHERE VID = ${DST}) SET WEIGHT = ${WEIGHT};"
  else
    BATCH="${BATCH}CREATE EDGE EDGE FROM (SELECT FROM Vertex WHERE VID = ${SRC}) TO (SELECT FROM Vertex WHERE VID = ${DST});"
  fi
  COUNT=$((COUNT + 1))
  if [ $((COUNT % BATCH_SIZE)) -eq 0 ]; then
    curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
      -H "Content-Type: application/json" \
      -u "root:playwithdata" \
      -d "{\"language\": \"sqlscript\", \"command\": \"BEGIN;${BATCH}COMMIT;\"}" > /dev/null
    BATCH=""
    echo "  Loaded ${COUNT} edges..."
  fi
done < "${INPUT_EDGE_PATH}"

# Flush remaining edges
if [ -n "$BATCH" ]; then
  curl -s -X POST "${HTTP_URI}/api/v1/command/${DB_NAME}" \
    -H "Content-Type: application/json" \
    -u "root:playwithdata" \
    -d "{\"language\": \"sqlscript\", \"command\": \"BEGIN;${BATCH}COMMIT;\"}" > /dev/null
fi
echo "  Loaded ${COUNT} edges total."

# Store the database name for later use
echo "${DB_NAME}" > "${OUTPUT_PATH}/database_name"
echo "Graph loading complete: ${DB_NAME}"
