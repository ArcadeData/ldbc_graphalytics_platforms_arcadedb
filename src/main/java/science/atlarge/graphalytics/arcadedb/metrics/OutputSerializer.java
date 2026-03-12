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

import com.arcadedb.database.Database;
import com.arcadedb.graph.Vertex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.ID_PROPERTY;

/**
 * Generic class for serializing the output of a metric.
 * Reads results stored as properties on vertices and writes them
 * to the Graphalytics output format.
 *
 * @param <N> the type of the result value
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OutputSerializer<N extends Number> {

    private static final Logger LOG = LogManager.getLogger();

    private final String property;
    private final N defaultValue;

    /**
     * @param property     the name of the property storing the result
     * @param defaultValue a value to use if the property is not set on a vertex
     */
    public OutputSerializer(String property, N defaultValue) {
        this.property = property;
        this.defaultValue = defaultValue;
    }

    /**
     * Serializes the graph database results into the output file.
     * Floating-point values use scientific notation, integers use decimal.
     *
     * @param graphDatabase the database to serialize
     * @param outputPath    the path where the output file should be written
     */
    public void serialize(Database graphDatabase, String outputPath) throws IOException {
        LOG.info("  Serializing results to: {}", outputPath);
        int count = 0;
        try (FileWriter writer = new FileWriter(outputPath)) {
            Iterator<Vertex> vertices = graphDatabase.iterateType("Vertex", false);
            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                writer.write(serializeValue(vertex) + "\n");
                count++;
                if (count % 100000 == 0) {
                    LOG.info("  Serialized {} vertices...", String.format("%,d", count));
                }
            }
        }
        LOG.info("  Serialization complete: {} vertices written.", String.format("%,d", count));
    }

    @SuppressWarnings("unchecked")
    private String serializeValue(Vertex vertex) {
        long id = ((Number) vertex.get(ID_PROPERTY)).longValue();
        N value;
        Object prop = vertex.get(property);
        if (prop != null) {
            value = (N) prop;
        } else {
            value = defaultValue;
        }

        if (value instanceof Double || value instanceof Float) {
            return String.format("%d %e", id, value.doubleValue());
        } else {
            return String.format("%d %d", id, value.longValue());
        }
    }
}
