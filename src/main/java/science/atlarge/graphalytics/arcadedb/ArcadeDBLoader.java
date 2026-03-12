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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Graph loader for ArcadeDB. Creates an embedded database and imports vertex/edge
 * data from EVLP-formatted files directly using the ArcadeDB Java API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBLoader {

    private static final Logger LOG = LogManager.getLogger();
    private static final int VERTEX_BATCH_SIZE = 10000;
    private static final int EDGE_BATCH_SIZE = 20000;
    private static final int BUCKETS = 8;

    protected FormattedGraph formattedGraph;
    protected ArcadeDBConfiguration platformConfig;

    public ArcadeDBLoader(FormattedGraph formattedGraph, ArcadeDBConfiguration platformConfig) {
        this.formattedGraph = formattedGraph;
        this.platformConfig = platformConfig;
    }

    public int load(String loadedInputPath) throws Exception {
        LOG.info("Creating embedded ArcadeDB database at: " + loadedInputPath);

        long totalVertices = formattedGraph.getNumberOfVertices();
        long totalEdges = formattedGraph.getNumberOfEdges();
        LOG.info("Graph '{}': {} vertices, {} edges", formattedGraph.getName(),
                String.format("%,d", totalVertices), String.format("%,d", totalEdges));

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
            Map<Long, RID> vidToRid = loadVertices(database, totalVertices);
            loadEdges(database, totalEdges, vidToRid);
        }

        LOG.info("Graph loading complete: " + formattedGraph.getName());
        return 0;
    }

    private void createSchema(Database database) {
        database.getSchema().createVertexType(ArcadeDBConstants.VERTEX_TYPE, BUCKETS);
        database.getSchema().createEdgeType(ArcadeDBConstants.EDGE_TYPE, BUCKETS);

        database.getSchema().getType(ArcadeDBConstants.VERTEX_TYPE)
                .createProperty(ArcadeDBConstants.ID_PROPERTY, Type.LONG);
        database.getSchema().getType(ArcadeDBConstants.VERTEX_TYPE)
                .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, ArcadeDBConstants.ID_PROPERTY);

        if (formattedGraph.hasEdgeProperties()) {
            database.getSchema().getType(ArcadeDBConstants.EDGE_TYPE)
                    .createProperty(ArcadeDBConstants.WEIGHT_PROPERTY, Type.DOUBLE);
        }

        LOG.info("Schema created: vertex type '{}' ({} buckets), edge type '{}' ({} buckets)",
                ArcadeDBConstants.VERTEX_TYPE, BUCKETS, ArcadeDBConstants.EDGE_TYPE, BUCKETS);
    }

    private Map<Long, RID> loadVertices(Database database, long totalVertices) throws IOException {
        LOG.info("[Step 1/2] Loading vertices from: {}", formattedGraph.getVertexFilePath());

        Map<Long, RID> vidToRid = new HashMap<>();
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
                vidToRid.put(vid, vertex.getIdentity());

                count++;
                if (count % VERTEX_BATCH_SIZE == 0) {
                    database.commit();
                    database.begin();
                    if (totalVertices > 0) {
                        LOG.info("[Step 1/2] Loading vertices: {}% ({}/{})",
                                String.format("%.1f", 100.0 * count / totalVertices),
                                String.format("%,d", count), String.format("%,d", totalVertices));
                    }
                }
            }
        }

        database.commit();
        LOG.info("[Step 1/2] Loading vertices: 100% - {} vertices loaded.", String.format("%,d", count));
        return vidToRid;
    }

    private void loadEdges(Database database, long totalEdges, Map<Long, RID> vidToRid) throws IOException {
        LOG.info("[Step 2/2] Loading edges from: {} (VID->RID cache: {} entries)",
                formattedGraph.getEdgeFilePath(), String.format("%,d", vidToRid.size()));
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

                RID srcRid = vidToRid.get(srcId);
                RID dstRid = vidToRid.get(dstId);
                if (srcRid == null)
                    throw new RuntimeException("Vertex not found with VID=" + srcId);
                if (dstRid == null)
                    throw new RuntimeException("Vertex not found with VID=" + dstId);

                Vertex src = srcRid.asVertex();
                Vertex dst = dstRid.asVertex();

                if (weighted && parts.length > 2) {
                    double weight = Double.parseDouble(parts[2]);
                    src.newEdge(ArcadeDBConstants.EDGE_TYPE, dst,
                            ArcadeDBConstants.WEIGHT_PROPERTY, weight);
                } else {
                    src.newEdge(ArcadeDBConstants.EDGE_TYPE, dst);
                }

                count++;
                if (count % EDGE_BATCH_SIZE == 0) {
                    database.commit();
                    database.begin();
                    if (totalEdges > 0) {
                        LOG.info("[Step 2/2] Loading edges: {}% ({}/{})",
                                String.format("%.1f", 100.0 * count / totalEdges),
                                String.format("%,d", count), String.format("%,d", totalEdges));
                    }
                }
            }
        }

        database.commit();
        LOG.info("[Step 2/2] Loading edges: 100% - {} edges loaded.", String.format("%,d", count));
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
