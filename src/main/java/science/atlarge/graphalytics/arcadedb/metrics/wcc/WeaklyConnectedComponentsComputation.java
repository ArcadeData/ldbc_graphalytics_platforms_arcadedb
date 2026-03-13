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
package science.atlarge.graphalytics.arcadedb.metrics.wcc;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.algo.AlgoWCC;
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
 * WCC computation using ArcadeDB's built-in algo.wcc procedure.
 * Maps sequential component IDs to VIDs for LDBC compatibility.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class WeaklyConnectedComponentsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;

    public WeaklyConnectedComponentsComputation(Database graphDatabase) {
        this.graphDatabase = graphDatabase;
    }

    public void run() {
        LOG.info("- Starting WCC algorithm using built-in algo.wcc");

        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        AlgoWCC algo = new AlgoWCC();
        LOG.info("  [Step 1/2] Computing connected components...");
        Stream<Result> results = algo.execute(new Object[]{}, null, context);

        // Map sequential componentIds to VIDs (first vertex encountered per component)
        Map<Integer, Long> componentIdToVid = new HashMap<>();
        LOG.info("  [Step 2/2] Writing results...");

        graphDatabase.begin();
        int count = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            int componentId = ((Number) row.getProperty("componentId")).intValue();

            long vid = v.getLong(ID_PROPERTY);
            long mappedComponentId = componentIdToVid.computeIfAbsent(componentId, k -> vid);

            MutableVertex mv = v.modify();
            mv.set(COMPONENT, mappedComponentId);
            mv.save();
            count++;
            if (count % 100000 == 0) {
                LOG.info("  [Step 2/2] Writing results: {} vertices, {} components",
                        String.format("%,d", count), String.format("%,d", componentIdToVid.size()));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed WCC: {} vertices, {} components",
                String.format("%,d", count), String.format("%,d", componentIdToVid.size()));
    }
}
