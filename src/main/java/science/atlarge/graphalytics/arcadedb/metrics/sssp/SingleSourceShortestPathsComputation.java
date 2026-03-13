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
import com.arcadedb.query.opencypher.procedures.algo.AlgoDijkstraSingleSource;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * SSSP computation using ArcadeDB's built-in algo.dijkstra.singleSource procedure.
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
        LOG.info("- Starting SSSP algorithm (source={}) using built-in algo.dijkstra.singleSource", startVertexId);

        // Find start vertex and initialize all with infinity
        Vertex startVertex = null;
        LOG.info("  [Step 1/3] Initializing vertices with default distance...");
        int totalVertices = 0;

        graphDatabase.begin();
        Iterator<Vertex> allIt = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (allIt.hasNext()) {
            Vertex v = allIt.next();
            long vid = v.getLong(ID_PROPERTY);
            if (vid == startVertexId)
                startVertex = v;
            MutableVertex mv = v.modify();
            mv.set(SSSP, Double.POSITIVE_INFINITY);
            mv.save();
            totalVertices++;
        }
        if (startVertex != null) {
            MutableVertex mv = startVertex.modify();
            mv.set(SSSP, 0.0);
            mv.save();
        }
        graphDatabase.commit();
        LOG.info("  [Step 1/3] Initialized {} vertices.", String.format("%,d", totalVertices));

        if (startVertex == null) {
            LOG.warn("  Start vertex with VID={} not found!", startVertexId);
            return;
        }

        // Execute built-in Dijkstra
        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        String direction = directed ? "OUT" : "BOTH";
        AlgoDijkstraSingleSource algo = new AlgoDijkstraSingleSource();
        LOG.info("  [Step 2/3] Running Dijkstra (direction={})...", direction);
        Stream<Result> results = algo.execute(
                new Object[]{ startVertex, EDGE_TYPE, WEIGHT_PROPERTY, direction }, null, context);

        // Write results
        graphDatabase.begin();
        int reachable = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            double cost = ((Number) row.getProperty("cost")).doubleValue();
            MutableVertex mv = v.modify();
            mv.set(SSSP, cost);
            mv.save();
            reachable++;
        }
        graphDatabase.commit();

        LOG.info("  [Step 3/3] SSSP complete: {} reachable, {} unreachable out of {} total.",
                String.format("%,d", reachable),
                String.format("%,d", totalVertices - reachable - 1),
                String.format("%,d", totalVertices));
    }
}
