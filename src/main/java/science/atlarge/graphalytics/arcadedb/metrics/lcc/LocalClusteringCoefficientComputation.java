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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the local clustering coefficient algorithm using
 * triangle counting on the ArcadeDB Java graph API.
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
        LOG.info("- Starting Local Clustering Coefficient algorithm");

        LOG.info("  [Step 1/3] Collecting vertices...");
        List<Vertex> vertices = new ArrayList<>();
        Iterator<Vertex> it = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (it.hasNext()) {
            vertices.add(it.next());
        }
        int n = vertices.size();
        LOG.info("  [Step 1/3] Collected {} vertices. Building neighborhoods...", String.format("%,d", n));

        // Build undirected neighborhood for each vertex (both directions)
        Map<RID, Set<RID>> neighbors = new HashMap<>();
        // Build directed out-neighbors for directed triangle counting
        Map<RID, Set<RID>> outNeighbors = new HashMap<>();

        int built = 0;
        for (Vertex v : vertices) {
            Set<RID> neighborSet = new HashSet<>();
            Set<RID> outSet = new HashSet<>();

            for (Edge edge : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                RID target = edge.getInVertex().getIdentity();
                neighborSet.add(target);
                outSet.add(target);
            }
            for (Edge edge : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                neighborSet.add(edge.getOutVertex().getIdentity());
            }
            neighbors.put(v.getIdentity(), neighborSet);
            outNeighbors.put(v.getIdentity(), outSet);
            built++;
            if (built % 100000 == 0) {
                LOG.info("  [Step 1/3] Building neighborhoods: {}% ({}/{})",
                        String.format("%.1f", 100.0 * built / n),
                        String.format("%,d", built), String.format("%,d", n));
            }
        }
        LOG.info("  [Step 1/3] Neighborhoods built.");

        // Compute LCC for each vertex
        LOG.info("  [Step 2/3] Computing LCC...");
        graphDatabase.begin();
        int computed = 0;
        for (Vertex v : vertices) {
            Set<RID> neighborSet = neighbors.get(v.getIdentity());
            int degree = neighborSet.size();

            double lcc = 0.0;
            if (degree >= 2) {
                if (directed) {
                    // For directed: count directed edges between neighbors
                    // For each ordered pair (u, w) in N(v), check if u->w exists
                    int directedEdges = 0;
                    for (RID u : neighborSet) {
                        Set<RID> uOut = outNeighbors.get(u);
                        if (uOut == null)
                            continue;
                        for (RID w : neighborSet) {
                            if (!u.equals(w) && uOut.contains(w)) {
                                directedEdges++;
                            }
                        }
                    }
                    lcc = (double) directedEdges / (degree * (degree - 1));
                } else {
                    // For undirected: count edges between unordered pairs
                    int triangles = 0;
                    RID[] neighborArray = neighborSet.toArray(new RID[0]);
                    for (int i = 0; i < neighborArray.length; i++) {
                        Set<RID> ni = neighbors.get(neighborArray[i]);
                        if (ni == null)
                            continue;
                        for (int j = i + 1; j < neighborArray.length; j++) {
                            if (ni.contains(neighborArray[j])) {
                                triangles++;
                            }
                        }
                    }
                    lcc = (2.0 * triangles) / (degree * (degree - 1));
                }
            }

            MutableVertex mv = v.modify();
            mv.set(LCC, lcc);
            mv.save();
            computed++;
            if (computed % 100000 == 0) {
                LOG.info("  [Step 2/3] Computing LCC: {}% ({}/{})",
                        String.format("%.1f", 100.0 * computed / n),
                        String.format("%,d", computed), String.format("%,d", n));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed LCC algorithm ({} vertices)", String.format("%,d", n));
    }
}
