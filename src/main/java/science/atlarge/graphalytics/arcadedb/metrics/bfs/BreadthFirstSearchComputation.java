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
import com.arcadedb.query.opencypher.procedures.algo.AlgoBFS;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * BFS computation using ArcadeDB's built-in algo.bfs procedure.
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
        LOG.info("- Starting BFS algorithm (source={}) using built-in algo.bfs", startVertexId);

        // Find start vertex and initialize all vertices with default distance
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
            mv.set(DISTANCE, Long.MAX_VALUE);
            mv.save();
            totalVertices++;
        }
        if (startVertex != null) {
            MutableVertex mv = startVertex.modify();
            mv.set(DISTANCE, 0L);
            mv.save();
        }
        graphDatabase.commit();
        LOG.info("  [Step 1/3] Initialized {} vertices.", String.format("%,d", totalVertices));

        if (startVertex == null) {
            LOG.warn("  Start vertex with VID={} not found!", startVertexId);
            return;
        }

        // Execute built-in BFS
        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        String direction = directed ? "OUT" : "BOTH";
        AlgoBFS algo = new AlgoBFS();
        LOG.info("  [Step 2/3] Running BFS traversal (direction={})...", direction);
        Stream<Result> results = algo.execute(new Object[]{ startVertex, EDGE_TYPE, direction }, null, context);

        // Write results
        graphDatabase.begin();
        int reachable = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            int depth = ((Number) row.getProperty("depth")).intValue();
            MutableVertex mv = v.modify();
            mv.set(DISTANCE, (long) depth);
            mv.save();
            reachable++;
        }
        graphDatabase.commit();

        LOG.info("  [Step 3/3] BFS complete: {} reachable, {} unreachable out of {} total.",
                String.format("%,d", reachable),
                String.format("%,d", totalVertices - reachable - 1),
                String.format("%,d", totalVertices));
    }
}
