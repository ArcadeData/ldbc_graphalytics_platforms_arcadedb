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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of the weakly connected components algorithm using ArcadeDB's
 * native algo.wcc procedure via Cypher/Bolt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class WeaklyConnectedComponentsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Session session;

    public WeaklyConnectedComponentsComputation(Session session) {
        this.session = session;
    }

    /**
     * Executes the WCC algorithm and returns a map of vertex ID to component ID.
     */
    public Map<Long, Number> run() {
        LOG.debug("- Starting Weakly Connected Components algorithm");

        Map<Long, Number> results = new TreeMap<>();

        String query =
                "CALL algo.wcc()\n" +
                "YIELD node, componentId\n" +
                "RETURN node.VID AS id, componentId AS value";

        Result result = session.run(query);
        while (result.hasNext()) {
            Record record = result.next();
            long id = record.get("id").asLong();
            long componentId = record.get("value").asLong();
            results.put(id, componentId);
        }

        LOG.debug("- Completed WCC algorithm, {} vertices classified", results.size());
        return results;
    }
}
