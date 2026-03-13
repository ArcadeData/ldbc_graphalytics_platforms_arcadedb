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
package science.atlarge.graphalytics.arcadedb.metrics.lcc;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.algo.AlgoLocalClusteringCoefficient;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.stream.Stream;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * LCC computation using ArcadeDB's built-in algo.localClusteringCoefficient procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalClusteringCoefficientComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final boolean directed;

    public LocalClusteringCoefficientComputation(Database graphDatabase, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.directed = directed;
    }

    public void run() {
        LOG.info("- Starting LCC algorithm using built-in algo.localClusteringCoefficient");

        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(graphDatabase);

        AlgoLocalClusteringCoefficient algo = new AlgoLocalClusteringCoefficient();
        LOG.info("  [Step 1/2] Computing local clustering coefficients...");
        Stream<Result> results = algo.execute(new Object[]{ EDGE_TYPE }, null, context);

        LOG.info("  [Step 2/2] Writing results...");
        graphDatabase.begin();
        int count = 0;
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            Vertex v = ((Vertex) row.getProperty("node"));
            double lcc = ((Number) row.getProperty("localClusteringCoefficient")).doubleValue();
            MutableVertex mv = v.modify();
            mv.set(LCC, lcc);
            mv.save();
            count++;
            if (count % 100000 == 0) {
                LOG.info("  [Step 2/2] Writing results: {} vertices", String.format("%,d", count));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed LCC algorithm on {} vertices", String.format("%,d", count));
    }
}
