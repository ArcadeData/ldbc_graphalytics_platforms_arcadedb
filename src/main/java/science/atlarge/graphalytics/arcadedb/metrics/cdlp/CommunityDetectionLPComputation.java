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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of the community detection (label propagation) algorithm using
 * ArcadeDB's native algo.labelpropagation procedure via Cypher/Bolt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CommunityDetectionLPComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Session session;
    private final int maxIterations;
    private final boolean directed;

    public CommunityDetectionLPComputation(Session session, int maxIterations, boolean directed) {
        this.session = session;
        this.maxIterations = maxIterations;
        this.directed = directed;
    }

    /**
     * Executes the label propagation algorithm and returns a map of vertex ID to community ID.
     */
    public Map<Long, Number> run() {
        LOG.debug("- Starting Community Detection Label Propagation algorithm (maxIterations={})", maxIterations);

        Map<Long, Number> results = new TreeMap<>();

        String direction = directed ? "'OUTGOING'" : "'BOTH'";
        String query = String.format(
                "CALL algo.labelpropagation({maxIterations: %d, direction: %s})\n" +
                "YIELD node, communityId\n" +
                "RETURN node.VID AS id, communityId AS value",
                maxIterations,
                direction
        );

        Result result = session.run(query);
        while (result.hasNext()) {
            Record record = result.next();
            long id = record.get("id").asLong();
            long communityId = record.get("value").asLong();
            results.put(id, communityId);
        }

        LOG.debug("- Completed CDLP algorithm, {} vertices classified", results.size());
        return results;
    }
}
