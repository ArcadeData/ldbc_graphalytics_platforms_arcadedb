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
package science.atlarge.graphalytics.arcadedb.metrics.cdlp;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLabelPropagation;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * CDLP computation using ArcadeDB's built-in algo.labelpropagation procedure.
 * Maps sequential community IDs to VIDs for LDBC compatibility.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CommunityDetectionLPComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final int maxIterations;
    private final boolean directed;

    public CommunityDetectionLPComputation(Database graphDatabase, int maxIterations, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.maxIterations = maxIterations;
        this.directed = directed;
    }

    public void run() {
        LOG.info("- Starting CDLP algorithm (iterations={}) using built-in algo.labelpropagation", maxIterations);

        Map<String, Object> config = new HashMap<>();
        config.put("maxIterations", maxIterations);
        config.put("direction", directed ? "OUT" : "BOTH");

        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        AlgoLabelPropagation algo = new AlgoLabelPropagation();
        LOG.info("  [Step 1/2] Computing label propagation...");
        Stream<Result> results = algo.execute(new Object[]{ config }, null, context);

        // Map sequential communityIds to VIDs (first vertex encountered per community)
        Map<Integer, Long> communityIdToVid = new HashMap<>();
        LOG.info("  [Step 2/2] Writing results...");

        graphDatabase.begin();
        int count = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            int communityId = ((Number) row.getProperty("communityId")).intValue();

            long vid = v.getLong(ID_PROPERTY);
            long mappedLabel = communityIdToVid.computeIfAbsent(communityId, k -> vid);

            MutableVertex mv = v.modify();
            mv.set(LABEL, mappedLabel);
            mv.save();
            count++;
            if (count % 100000 == 0) {
                LOG.info("  [Step 2/2] Writing results: {} vertices, {} communities",
                        String.format("%,d", count), String.format("%,d", communityIdToVid.size()));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed CDLP: {} vertices, {} communities",
                String.format("%,d", count), String.format("%,d", communityIdToVid.size()));
    }
}
