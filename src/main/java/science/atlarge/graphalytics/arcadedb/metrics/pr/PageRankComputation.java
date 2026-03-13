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
package science.atlarge.graphalytics.arcadedb.metrics.pr;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.algo.AlgoPageRank;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * PageRank computation using ArcadeDB's built-in algo.pagerank procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageRankComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final int maxIterations;
    private final float dampingFactor;
    private final boolean directed;

    public PageRankComputation(Database graphDatabase, int maxIterations, float dampingFactor, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.maxIterations = maxIterations;
        this.dampingFactor = dampingFactor;
        this.directed = directed;
    }

    public void run() {
        LOG.info("- Starting PageRank algorithm (iterations={}, damping={}) using built-in algo.pagerank",
                maxIterations, dampingFactor);

        // Configure the algorithm
        Map<String, Object> config = new HashMap<>();
        config.put("dampingFactor", (double) dampingFactor);
        config.put("maxIterations", maxIterations);
        config.put("tolerance", 0.0); // Run exact number of iterations (no early convergence)

        // Execute built-in algorithm
        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        AlgoPageRank algo = new AlgoPageRank();
        LOG.info("  [Step 1/2] Computing PageRank...");
        Stream<Result> results = algo.execute(new Object[]{ config }, null, context);

        // Write results to vertex properties
        LOG.info("  [Step 2/2] Writing results...");
        graphDatabase.begin();
        int count = 0;
        int total = 0;
        java.util.Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            double score = ((Number) row.getProperty("score")).doubleValue();
            MutableVertex mv = v.modify();
            mv.set(PAGERANK, score);
            mv.save();
            count++;
            total++;
            if (count % 100000 == 0) {
                LOG.info("  [Step 2/2] Writing results: {} vertices written", String.format("%,d", total));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed PageRank algorithm ({} iterations on {} vertices)", maxIterations, String.format("%,d", total));
    }
}
