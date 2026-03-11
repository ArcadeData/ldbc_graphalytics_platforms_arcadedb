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
package science.atlarge.graphalytics.arcadedb.metrics;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Serializer for algorithm results. Collects results from Cypher procedure
 * YIELD output and writes them to the Graphalytics output format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OutputSerializer {

    /**
     * Collects all vertex results from a Cypher query into a sorted map.
     * The query must return columns named 'id' (Long) and 'value' (Number).
     *
     * @param session the Bolt session
     * @param query   the Cypher query
     * @return a sorted map of vertex ID to result value
     */
    public static Map<Long, Number> collectResults(Session session, String query) {
        Map<Long, Number> results = new TreeMap<>();
        Result result = session.run(query);
        while (result.hasNext()) {
            Record record = result.next();
            long id = record.get("id").asLong();
            Number value = record.get("value").asNumber();
            results.put(id, value);
        }
        return results;
    }

    /**
     * Writes collected results to the output file in Graphalytics format.
     * Format: "vertexId value" per line, where floating-point values use scientific notation.
     *
     * @param results    the algorithm results (vertex ID -> value)
     * @param outputPath the output file path
     * @param isFloating whether values should be written in scientific notation
     */
    public static void serialize(Map<Long, Number> results, String outputPath, boolean isFloating) throws IOException {
        try (FileWriter writer = new FileWriter(outputPath)) {
            for (Map.Entry<Long, Number> entry : results.entrySet()) {
                if (isFloating) {
                    writer.write(String.format("%d %e\n", entry.getKey(), entry.getValue().doubleValue()));
                } else {
                    writer.write(String.format("%d %d\n", entry.getKey(), entry.getValue().longValue()));
                }
            }
        }
    }

    /**
     * Fills in default values for vertices not present in the results.
     * Queries all vertices and adds default value for any missing ones.
     *
     * @param session      the Bolt session
     * @param results      the existing results map (modified in place)
     * @param defaultValue the default value for vertices without results
     */
    public static void fillDefaults(Session session, Map<Long, Number> results, Number defaultValue) {
        Result allVertices = session.run(
                "MATCH (n:Vertex) RETURN n.VID AS id ORDER BY id"
        );
        while (allVertices.hasNext()) {
            Record record = allVertices.next();
            long id = record.get("id").asLong();
            if (!results.containsKey(id)) {
                results.put(id, defaultValue);
            }
        }
    }
}
