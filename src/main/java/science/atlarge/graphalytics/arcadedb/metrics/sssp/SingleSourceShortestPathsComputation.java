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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of the single source shortest paths algorithm using ArcadeDB's
 * native algo.dijkstra.singleSource procedure via Cypher/Bolt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SingleSourceShortestPathsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Session session;
    private final long startVertexId;
    private final boolean directed;

    public SingleSourceShortestPathsComputation(Session session, long startVertexId, boolean directed) {
        this.session = session;
        this.startVertexId = startVertexId;
        this.directed = directed;
    }

    /**
     * Executes the SSSP algorithm and returns a map of vertex ID to shortest distance.
     */
    public Map<Long, Number> run() {
        LOG.debug("- Starting Single Source Shortest Paths algorithm from vertex {}", startVertexId);

        Map<Long, Number> results = new TreeMap<>();

        String direction = directed ? "'OUTGOING'" : "'BOTH'";
        String query = String.format(
                "MATCH (start:Vertex {VID: %d})\n" +
                "CALL algo.dijkstra.singleSource(start, ['EDGE'], 'WEIGHT', %s)\n" +
                "YIELD node, cost\n" +
                "RETURN node.VID AS id, cost AS value",
                startVertexId,
                direction
        );

        Result result = session.run(query);
        while (result.hasNext()) {
            Record record = result.next();
            long id = record.get("id").asLong();
            double cost = record.get("value").asDouble();
            results.put(id, cost);
        }

        LOG.debug("- Completed SSSP algorithm, {} vertices reached", results.size());
        return results;
    }
}
