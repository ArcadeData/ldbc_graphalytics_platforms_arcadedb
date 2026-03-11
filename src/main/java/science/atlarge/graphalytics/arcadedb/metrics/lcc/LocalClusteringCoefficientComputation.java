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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the local clustering coefficient algorithm using ArcadeDB's
 * native algo.localClusteringCoefficient procedure.
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
        LOG.debug("- Starting Local Clustering Coefficient algorithm");

        String query =
                "CALL algo.localClusteringCoefficient(['EDGE']) " +
                "YIELD node, localClusteringCoefficient " +
                "RETURN node.VID AS id, localClusteringCoefficient AS value";

        graphDatabase.begin();
        ResultSet result = graphDatabase.command("cypher", query);
        while (result.hasNext()) {
            Result record = result.next();
            long vid = record.getProperty("id");
            double lcc = ((Number) record.getProperty("value")).doubleValue();

            Vertex vertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, vid).next().asVertex();
            MutableVertex mv = vertex.modify();
            mv.set(LCC, lcc);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed LCC algorithm");
    }
}
