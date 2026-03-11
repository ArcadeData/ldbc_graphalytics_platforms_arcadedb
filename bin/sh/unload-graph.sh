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

  case $key in

    --graph-name)
      GRAPH_NAME="$value"
      shift;;

    --output-path)
      OUTPUT_PATH="$value"
      shift;;

    --http-uri)
      HTTP_URI="$value"
      shift;;

    *)
      echo "Error: invalid option: " "$key"
      exit 1
      ;;
  esac
  shift
done

HTTP_URI=${HTTP_URI:-http://localhost:2480}

# Read the database name from the output path
if [ -f "${OUTPUT_PATH}/database_name" ]; then
  DB_NAME=$(cat "${OUTPUT_PATH}/database_name")
else
  DB_NAME=${GRAPH_NAME//[^a-zA-Z0-9_]/_}
fi

echo "Dropping ArcadeDB database: ${DB_NAME}"

# Drop the database
curl -s -X POST "${HTTP_URI}/api/v1/server" \
  -H "Content-Type: application/json" \
  -u "root:playwithdata" \
  -d "{\"command\": \"drop database ${DB_NAME}\"}" 2>/dev/null || true

# Clean up output path
if [[ ! -z "${GRAPH_NAME}" && "${OUTPUT_PATH}" == *"${GRAPH_NAME}"* ]]; then
  rm -r "$OUTPUT_PATH"
else
  echo "Failed to delete graph ${GRAPH_NAME}, path ${OUTPUT_PATH} does not contain graph name (unsafe)."
  exit 1
fi
