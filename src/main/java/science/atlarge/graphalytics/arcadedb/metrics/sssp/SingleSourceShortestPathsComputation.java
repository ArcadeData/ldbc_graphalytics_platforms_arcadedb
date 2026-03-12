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
 * Implementation of the single source shortest paths algorithm (Dijkstra)
 * using the ArcadeDB Java graph API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SingleSourceShortestPathsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final long startVertexId;
    private final boolean directed;

    public SingleSourceShortestPathsComputation(Database graphDatabase, long startVertexId, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.startVertexId = startVertexId;
        this.directed = directed;
    }

    public void run() {
        LOG.debug("- Starting Single Source Shortest Paths algorithm from vertex {}", startVertexId);

        // Initialize all vertices with infinity
        graphDatabase.begin();
        Iterator<Vertex> allVertices = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (allVertices.hasNext()) {
            MutableVertex v = allVertices.next().modify();
            v.set(SSSP, Double.POSITIVE_INFINITY);
            v.save();
        }
        graphDatabase.commit();

        // Dijkstra
        Vertex startVertex = graphDatabase.lookupByKey(VERTEX_TYPE, ID_PROPERTY, startVertexId).next().asVertex();

        Map<RID, Double> distances = new HashMap<>();
        PriorityQueue<double[]> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a[1]));
        Map<RID, long[]> ridBucketKey = new HashMap<>();

        distances.put(startVertex.getIdentity(), 0.0);
        // Store [bucketId, position, distance] but we just need RID->distance
        pq.add(new double[]{startVertex.getIdentity().getBucketId(), startVertex.getIdentity().getPosition(), 0.0});

        while (!pq.isEmpty()) {
            double[] entry = pq.poll();
            RID currentRid = new RID(graphDatabase, (int) entry[0], (long) entry[1]);
            double currentDist = entry[2];

            if (currentDist > distances.getOrDefault(currentRid, Double.POSITIVE_INFINITY))
                continue;

            Vertex current = graphDatabase.lookupByRID(currentRid, true).asVertex();

            // Outgoing edges
            for (Edge edge : current.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                double weight = edge.has(WEIGHT_PROPERTY)
                        ? ((Number) edge.get(WEIGHT_PROPERTY)).doubleValue()
                        : 1.0;
                Vertex neighbor = edge.getInVertex();
                double newDist = currentDist + weight;
                if (newDist < distances.getOrDefault(neighbor.getIdentity(), Double.POSITIVE_INFINITY)) {
                    distances.put(neighbor.getIdentity(), newDist);
                    pq.add(new double[]{neighbor.getIdentity().getBucketId(), neighbor.getIdentity().getPosition(), newDist});
                }
            }

            // Incoming edges (for undirected)
            if (!directed) {
                for (Edge edge : current.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                    double weight = edge.has(WEIGHT_PROPERTY)
                            ? ((Number) edge.get(WEIGHT_PROPERTY)).doubleValue()
                            : 1.0;
                    Vertex neighbor = edge.getOutVertex();
                    double newDist = currentDist + weight;
                    if (newDist < distances.getOrDefault(neighbor.getIdentity(), Double.POSITIVE_INFINITY)) {
                        distances.put(neighbor.getIdentity(), newDist);
                        pq.add(new double[]{neighbor.getIdentity().getBucketId(), neighbor.getIdentity().getPosition(), newDist});
                    }
                }
            }
        }

        // Write results
        graphDatabase.begin();
        for (Map.Entry<RID, Double> e : distances.entrySet()) {
            Vertex v = graphDatabase.lookupByRID(e.getKey(), true).asVertex();
            MutableVertex mv = v.modify();
            mv.set(SSSP, e.getValue());
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed SSSP algorithm");
    }
}
