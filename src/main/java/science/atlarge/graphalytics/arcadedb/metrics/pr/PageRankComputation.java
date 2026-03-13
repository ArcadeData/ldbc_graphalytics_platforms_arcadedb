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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * PageRank computation using ArcadeDB's built-in algo.pagerank procedure.
 * This automatically benefits from CSR acceleration when a GraphAnalyticalView is available.
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
        LOG.info("- Starting PageRank algorithm (iterations={}, damping={}, directed={}) using built-in algo.pagerank",
                maxIterations, dampingFactor, directed);

        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        String direction = directed ? "OUT" : "BOTH";
        AlgoPageRank algo = new AlgoPageRank();
        LOG.info("  [Step 1/2] Running PageRank (direction={})...", direction);
        Stream<Result> results = algo.execute(new Object[]{
                Map.of(
                        "dampingFactor", (double) dampingFactor,
                        "maxIterations", maxIterations,
                        "tolerance", 0.0, // disable early stop to match LDBC exactly
                        "direction", direction
                )
        }, null, context);

        boolean csrAccelerated = Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR));
        LOG.info("  [Step 1/2] PageRank complete (CSR accelerated: {})", csrAccelerated);

        // Write results to vertex properties
        LOG.info("  [Step 2/2] Writing results...");
        graphDatabase.begin();
        int count = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            double score = ((Number) row.getProperty("score")).doubleValue();
            MutableVertex mv = v.modify();
            mv.set(PAGERANK, score);
            mv.save();
            count++;
            if (count % 100000 == 0)
                LOG.info("  [Step 2/2] Writing results: {} vertices", String.format("%,d", count));
        }
        graphDatabase.commit();

        LOG.info("- Completed PageRank algorithm ({} iterations on {} vertices)", maxIterations, String.format("%,d", count));
    }
}
