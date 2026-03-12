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
package science.atlarge.graphalytics.arcadedb.metrics.bfs;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the breadth-first search algorithm using direct graph
 * traversal on the ArcadeDB Java API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BreadthFirstSearchComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final long startVertexId;
    private final boolean directed;

    public BreadthFirstSearchComputation(Database graphDatabase, long startVertexId, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.startVertexId = startVertexId;
        this.directed = directed;
    }

    public void run() {
        LOG.debug("- Starting BFS algorithm from vertex {}", startVertexId);

        // Initialize all vertices with max distance
        graphDatabase.begin();
        Iterator<Vertex> allVertices = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (allVertices.hasNext()) {
            MutableVertex v = allVertices.next().modify();
            v.set(DISTANCE, Long.MAX_VALUE);
            v.save();
        }
        graphDatabase.commit();

        // Find start vertex
        Vertex startVertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, startVertexId).next().asVertex();

        // BFS
        Map<RID, Long> distances = new HashMap<>();
        Queue<Vertex> queue = new LinkedList<>();
        queue.add(startVertex);
        distances.put(startVertex.getIdentity(), 0L);

        while (!queue.isEmpty()) {
            Vertex current = queue.poll();
            long currentDist = distances.get(current.getIdentity());

            // Outgoing edges
            for (Edge edge : current.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                Vertex neighbor = edge.getInVertex();
                if (!distances.containsKey(neighbor.getIdentity())) {
                    distances.put(neighbor.getIdentity(), currentDist + 1);
                    queue.add(neighbor);
                }
            }

            // Incoming edges (for undirected)
            if (!directed) {
                for (Edge edge : current.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                    Vertex neighbor = edge.getOutVertex();
                    if (!distances.containsKey(neighbor.getIdentity())) {
                        distances.put(neighbor.getIdentity(), currentDist + 1);
                        queue.add(neighbor);
                    }
                }
            }
        }

        // Write results
        graphDatabase.begin();
        for (Map.Entry<RID, Long> entry : distances.entrySet()) {
            Vertex v = graphDatabase.lookupByRID(entry.getKey(), true).asVertex();
            MutableVertex mv = v.modify();
            mv.set(DISTANCE, entry.getValue());
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed BFS algorithm");
    }
}
