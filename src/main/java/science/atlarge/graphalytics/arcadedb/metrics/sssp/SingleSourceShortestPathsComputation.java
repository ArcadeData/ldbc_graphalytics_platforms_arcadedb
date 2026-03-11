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
package science.atlarge.graphalytics.arcadedb.metrics.sssp;

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
 * Implementation of the single source shortest paths algorithm using ArcadeDB's
 * native algo.dijkstra.singleSource procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SingleSourceShortestPathsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final long startVertexId;
    private final boolean directed;

    public SingleSourceShortestPathsComputation(Database graphDatabase, long startVertexId, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.startVertexId = startVertexId;
        this.directed = directed;
    }

    public void run() {
        LOG.debug("- Starting Single Source Shortest Paths algorithm from vertex {}", startVertexId);

        // Initialize all vertices with infinity
        graphDatabase.begin();
        Iterator<Vertex> allVertices = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (allVertices.hasNext()) {
            MutableVertex v = allVertices.next().modify();
            v.set(SSSP, Double.POSITIVE_INFINITY);
            v.save();
        }
        graphDatabase.commit();

        // Run SSSP using ArcadeDB's native Dijkstra algorithm
        String direction = directed ? "'OUTGOING'" : "'BOTH'";
        String query = String.format(
                "MATCH (start:Vertex {VID: %d}) " +
                "CALL algo.dijkstra.singleSource(start, ['EDGE'], 'WEIGHT', %s) " +
                "YIELD node, cost " +
                "RETURN node.VID AS id, cost AS value",
                startVertexId, direction
        );

        graphDatabase.begin();
        ResultSet result = graphDatabase.command("cypher", query);
        while (result.hasNext()) {
            Result record = result.next();
            long vid = record.getProperty("id");
            double cost = ((Number) record.getProperty("value")).doubleValue();

            Vertex vertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, vid).next().asVertex();
            MutableVertex mv = vertex.modify();
            mv.set(SSSP, cost);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed SSSP algorithm");
    }
}
