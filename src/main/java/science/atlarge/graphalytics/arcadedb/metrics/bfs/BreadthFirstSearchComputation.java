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
package science.atlarge.graphalytics.arcadedb.metrics.bfs;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the breadth-first search algorithm using ArcadeDB's native
 * algo.bfs procedure. Stores distance results as vertex properties.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BreadthFirstSearchComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final long startVertexId;
    private final boolean directed;

    public BreadthFirstSearchComputation(Database graphDatabase, long startVertexId, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.startVertexId = startVertexId;
        this.directed = directed;
    }

    public void run() {
        LOG.debug("- Starting BFS algorithm from vertex {}", startVertexId);

        // Initialize all vertices with max distance
        graphDatabase.begin();
        Iterator<Vertex> allVertices = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (allVertices.hasNext()) {
            MutableVertex v = allVertices.next().modify();
            v.set(DISTANCE, Long.MAX_VALUE);
            v.save();
        }
        graphDatabase.commit();

        // Run BFS using ArcadeDB's native algorithm
        String direction = directed ? "'OUTGOING'" : "'BOTH'";
        String query = String.format(
                "MATCH (start:Vertex {VID: %d}) " +
                "CALL algo.bfs(start, ['EDGE'], %s) " +
                "YIELD node, depth " +
                "RETURN node.VID AS id, depth AS value",
                startVertexId, direction
        );

        graphDatabase.begin();
        ResultSet result = graphDatabase.command("cypher", query);
        while (result.hasNext()) {
            Result record = result.next();
            long vid = record.getProperty("id");
            long depth = ((Number) record.getProperty("value")).longValue();

            Vertex vertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, vid).next().asVertex();
            MutableVertex mv = vertex.modify();
            mv.set(DISTANCE, depth);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed BFS algorithm");
    }
}
