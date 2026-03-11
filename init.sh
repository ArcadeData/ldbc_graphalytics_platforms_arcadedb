#!/bin/sh

set -e

GRAPHS_DIR=${1:-~/graphs}
ARCADEDB_DIR=${2:-~/arcadedb}

PROJECT=graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT

rm -rf $PROJECT
mvn package -DskipTests
tar xf $PROJECT-bin.tar.gz
cd $PROJECT/
cp -r config-template config
sed -i "s|^graphs.root-directory =$|graphs.root-directory = $GRAPHS_DIR|g" config/benchmark.properties
sed -i "s|^graphs.validation-directory =$|graphs.validation-directory = $GRAPHS_DIR|g" config/benchmark.properties
sed -i "s|^platform.arcadedb.home =$|platform.arcadedb.home = $ARCADEDB_DIR|g" config/platform.properties
sed -i "s|^platform.arcadedb.bolt-uri =$|platform.arcadedb.bolt-uri = bolt://localhost:7687|g" config/platform.properties
