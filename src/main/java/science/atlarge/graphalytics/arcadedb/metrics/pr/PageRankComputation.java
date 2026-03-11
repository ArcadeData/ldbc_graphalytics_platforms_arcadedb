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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the PageRank algorithm using ArcadeDB's native
 * algo.pagerank procedure. Stores scores as vertex properties.
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
        LOG.debug("- Starting PageRank algorithm (iterations={}, damping={})", maxIterations, dampingFactor);

        String query = String.format(
                "CALL algo.pagerank({dampingFactor: %f, maxIterations: %d}) " +
                "YIELD node, score " +
                "RETURN node.VID AS id, score AS value",
                dampingFactor, maxIterations
        );

        graphDatabase.begin();
        ResultSet result = graphDatabase.command("cypher", query);
        while (result.hasNext()) {
            Result record = result.next();
            long vid = record.getProperty("id");
            double score = ((Number) record.getProperty("value")).doubleValue();

            Vertex vertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, vid).next().asVertex();
            MutableVertex mv = vertex.modify();
            mv.set(PAGERANK, score);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed PageRank algorithm");
    }
}
