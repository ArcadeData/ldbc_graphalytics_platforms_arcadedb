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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the community detection (label propagation) algorithm using
 * ArcadeDB's native algo.labelpropagation procedure.
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
        LOG.debug("- Starting Community Detection Label Propagation algorithm (maxIterations={})", maxIterations);

        String direction = directed ? "'OUTGOING'" : "'BOTH'";
        String query = String.format(
                "CALL algo.labelpropagation({maxIterations: %d, direction: %s}) " +
                "YIELD node, communityId " +
                "RETURN node.VID AS id, communityId AS value",
                maxIterations, direction
        );

        graphDatabase.begin();
        ResultSet result = graphDatabase.command("cypher", query);
        while (result.hasNext()) {
            Result record = result.next();
            long vid = record.getProperty("id");
            long communityId = ((Number) record.getProperty("value")).longValue();

            Vertex vertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, vid).next().asVertex();
            MutableVertex mv = vertex.modify();
            mv.set(LABEL, communityId);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed CDLP algorithm");
    }
}
