/*
 * Copyright 2015 - 2026 Delft University of Technology / Arcade Data Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Graph loader for ArcadeDB. Creates an embedded database and imports vertex/edge
 * data from EVLP-formatted files directly using the ArcadeDB Java API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBLoader {

    private static final Logger LOG = LogManager.getLogger();
    private static final int BATCH_SIZE = 10000;

    protected FormattedGraph formattedGraph;
    protected ArcadeDBConfiguration platformConfig;

    public ArcadeDBLoader(FormattedGraph formattedGraph, ArcadeDBConfiguration platformConfig) {
        this.formattedGraph = formattedGraph;
        this.platformConfig = platformConfig;
    }

    public int load(String loadedInputPath) throws Exception {
        LOG.info("Creating embedded ArcadeDB database at: " + loadedInputPath);

        // Ensure parent directory exists
        new File(loadedInputPath).getParentFile().mkdirs();

        // Drop existing database if present
        DatabaseFactory factory = new DatabaseFactory(loadedInputPath);
        if (factory.exists()) {
            factory.open().drop();
        }

        // Create and configure the database
        try (Database database = factory.create()) {
            createSchema(database);
            loadVertices(database);
            loadEdges(database);
        }

        LOG.info("Graph loading complete: " + formattedGraph.getName());
        return 0;
    }

    private void createSchema(Database database) {
        database.getSchema().createVertexType(ArcadeDBConstants.VERTEX_TYPE);
        database.getSchema().createEdgeType(ArcadeDBConstants.EDGE_TYPE);

        database.getSchema().getType(ArcadeDBConstants.VERTEX_TYPE)
                .createProperty(ArcadeDBConstants.ID_PROPERTY, Type.LONG);
        database.getSchema().getType(ArcadeDBConstants.VERTEX_TYPE)
                .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, ArcadeDBConstants.ID_PROPERTY);

        if (formattedGraph.hasEdgeProperties()) {
            database.getSchema().getType(ArcadeDBConstants.EDGE_TYPE)
                    .createProperty(ArcadeDBConstants.WEIGHT_PROPERTY, Type.DOUBLE);
        }

        LOG.info("Schema created: vertex type '{}', edge type '{}'",
                ArcadeDBConstants.VERTEX_TYPE, ArcadeDBConstants.EDGE_TYPE);
    }

    private void loadVertices(Database database) throws IOException {
        LOG.info("Loading vertices from: " + formattedGraph.getVertexFilePath());

        int count = 0;
        database.begin();

        try (BufferedReader reader = new BufferedReader(new FileReader(formattedGraph.getVertexFilePath()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;

                long vid = Long.parseLong(line.split("\\s+")[0]);

                MutableVertex vertex = database.newVertex(ArcadeDBConstants.VERTEX_TYPE);
                vertex.set(ArcadeDBConstants.ID_PROPERTY, vid);
                vertex.save();

                count++;
                if (count % BATCH_SIZE == 0) {
                    database.commit();
                    database.begin();
                    LOG.info("  Loaded {} vertices...", count);
                }
            }
        }

        database.commit();
        LOG.info("Loaded {} vertices total.", count);
    }

    private void loadEdges(Database database) throws IOException {
        LOG.info("Loading edges from: " + formattedGraph.getEdgeFilePath());
        boolean weighted = formattedGraph.hasEdgeProperties();

        int count = 0;
        database.begin();

        try (BufferedReader reader = new BufferedReader(new FileReader(formattedGraph.getEdgeFilePath()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;

                String[] parts = line.split("\\s+");
                long srcId = Long.parseLong(parts[0]);
                long dstId = Long.parseLong(parts[1]);

                Vertex src = lookupVertex(database, srcId);
                Vertex dst = lookupVertex(database, dstId);

                if (weighted && parts.length > 2) {
                    double weight = Double.parseDouble(parts[2]);
                    src.newEdge(ArcadeDBConstants.EDGE_TYPE, dst, true,
                            ArcadeDBConstants.WEIGHT_PROPERTY, weight);
                } else {
                    src.newEdge(ArcadeDBConstants.EDGE_TYPE, dst, true);
                }

                count++;
                if (count % BATCH_SIZE == 0) {
                    database.commit();
                    database.begin();
                    LOG.info("  Loaded {} edges...", count);
                }
            }
        }

        database.commit();
        LOG.info("Loaded {} edges total.", count);
    }

    private Vertex lookupVertex(Database database, long vid) {
        IndexCursor cursor = database.lookupByKey(ArcadeDBConstants.VERTEX_TYPE,
                ArcadeDBConstants.ID_PROPERTY, vid);
        if (cursor.hasNext()) {
            return cursor.next().asVertex();
        }
        throw new RuntimeException("Vertex not found with VID=" + vid);
    }

    public int unload(String loadedInputPath) throws Exception {
        LOG.info("Deleting database at: " + loadedInputPath);

        DatabaseFactory factory = new DatabaseFactory(loadedInputPath);
        if (factory.exists()) {
            try (Database database = factory.open()) {
                database.drop();
            }
        }

        return 0;
    }

}
