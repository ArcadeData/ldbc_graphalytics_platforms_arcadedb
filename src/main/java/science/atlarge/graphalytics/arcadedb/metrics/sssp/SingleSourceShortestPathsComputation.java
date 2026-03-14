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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
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
 * When a Graph Analytical View with edge properties is available, uses CSR-native Dijkstra
 * for maximum performance (zero OLTP access during the algorithm).
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

        // Check for CSR-accelerated path with edge properties
        final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(graphDatabase, new String[]{EDGE_TYPE});
        if (provider instanceof GraphAnalyticalView gav && gav.isReady() && gav.hasEdgeProperties()) {
            runWithCSR(gav);
            return;
        }

        // Fallback to OLTP path
        runWithOLTP();
    }

    private void runWithCSR(final GraphAnalyticalView gav) {
        LOG.info("  [CSR-accelerated] Using Graph Analytical View for SSSP (with edge properties)");

        // Step 1: Initialize all vertices with infinity via SQL (bulk operation)
        LOG.info("  [Step 1/3] Initializing vertices with default distance (SQL bulk update)...");
        graphDatabase.begin();
        graphDatabase.command("sql", "UPDATE " + VERTEX_TYPE + " SET " + SSSP + " = " + Double.POSITIVE_INFINITY);
        graphDatabase.commit();

        // Find start vertex dense ID
        final int startIdx = findStartVertexCSR(gav);
        if (startIdx < 0) {
            LOG.warn("  Start vertex with VID={} not found in CSR!", startVertexId);
            return;
        }

        // Set start vertex distance to 0
        graphDatabase.begin();
        graphDatabase.command("sql", "UPDATE " + VERTEX_TYPE + " SET " + SSSP + " = 0.0 WHERE " + ID_PROPERTY + " = " + startVertexId);
        graphDatabase.commit();

        final int n = gav.getNodeCount();
        LOG.info("  [Step 1/3] Initialized {} vertices.", String.format("%,d", n));

        // Step 2: Run Dijkstra directly on CSR arrays with edge weights from columnar storage
        LOG.info("  [Step 2/3] Running Dijkstra on CSR (direction={})...", directed ? "OUT" : "BOTH");
        final Vertex.DIRECTION direction = directed ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
        final double[] dist = GraphAlgorithms.dijkstraSingleSource(gav, startIdx, WEIGHT_PROPERTY, direction, EDGE_TYPE);

        // Step 3: Write results back
        LOG.info("  [Step 3/3] Writing SSSP results...");
        graphDatabase.begin();
        int reachable = 0;
        for (int i = 0; i < n; i++) {
            if (i == startIdx || dist[i] == Double.POSITIVE_INFINITY)
                continue;
            final Vertex v = gav.getRID(i).asVertex();
            final MutableVertex mv = v.modify();
            mv.set(SSSP, dist[i]);
            mv.save();
            reachable++;
        }
        graphDatabase.commit();

        LOG.info("  [Step 3/3] SSSP complete: {} reachable, {} unreachable out of {} total.",
                String.format("%,d", reachable),
                String.format("%,d", n - reachable - 1),
                String.format("%,d", n));
    }

    private void runWithOLTP() {
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

    private int findStartVertexCSR(final GraphAnalyticalView gav) {
        // Find the vertex with VID = startVertexId, then get its CSR dense ID
        try (final var rs = graphDatabase.query("sql",
                "SELECT FROM " + VERTEX_TYPE + " WHERE " + ID_PROPERTY + " = ?", startVertexId)) {
            if (rs.hasNext()) {
                final Result row = rs.next();
                final Vertex v = row.getVertex().get();
                return gav.getNodeId(v.getIdentity());
            }
        }
        return -1;
    }
}
