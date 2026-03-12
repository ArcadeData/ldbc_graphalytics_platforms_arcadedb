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
 * Implementation of the weakly connected components algorithm using BFS-based
 * component detection on the ArcadeDB graph API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class WeaklyConnectedComponentsComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;

    public WeaklyConnectedComponentsComputation(Database graphDatabase) {
        this.graphDatabase = graphDatabase;
    }

    public void run() {
        LOG.info("- Starting Weakly Connected Components algorithm");

        LOG.info("  [Step 1/3] Collecting vertices...");
        Map<RID, Long> ridToVid = new HashMap<>();
        Map<RID, Long> componentMap = new HashMap<>();
        List<Vertex> allVertices = new ArrayList<>();

        Iterator<Vertex> it = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (it.hasNext()) {
            Vertex v = it.next();
            allVertices.add(v);
            long vid = v.getLong(ID_PROPERTY);
            ridToVid.put(v.getIdentity(), vid);
        }
        int totalVertices = allVertices.size();
        LOG.info("  [Step 1/3] Collected {} vertices.", String.format("%,d", totalVertices));

        LOG.info("  [Step 2/3] Discovering components...");
        Set<RID> visited = new HashSet<>();
        long componentId = 0;

        for (Vertex v : allVertices) {
            if (visited.contains(v.getIdentity()))
                continue;

            // BFS to find all vertices in this component
            Queue<Vertex> queue = new LinkedList<>();
            queue.add(v);
            visited.add(v.getIdentity());

            while (!queue.isEmpty()) {
                Vertex current = queue.poll();
                componentMap.put(current.getIdentity(), ridToVid.get(v.getIdentity()));

                // Traverse both directions for weakly connected
                for (Edge edge : current.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                    Vertex neighbor = edge.getInVertex();
                    if (!visited.contains(neighbor.getIdentity())) {
                        visited.add(neighbor.getIdentity());
                        queue.add(neighbor);
                    }
                }
                for (Edge edge : current.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                    Vertex neighbor = edge.getOutVertex();
                    if (!visited.contains(neighbor.getIdentity())) {
                        visited.add(neighbor.getIdentity());
                        queue.add(neighbor);
                    }
                }
            }
            componentId++;
            if (visited.size() % 100000 < 1000) {
                LOG.info("  [Step 2/3] Discovering components: {}% ({}/{} vertices assigned, {} components found)",
                        String.format("%.1f", 100.0 * visited.size() / totalVertices),
                        String.format("%,d", visited.size()), String.format("%,d", totalVertices), componentId);
            }
        }
        LOG.info("  [Step 2/3] Component discovery complete: {} components found.", componentId);

        // Write results
        LOG.info("  [Step 3/3] Writing results...");
        graphDatabase.begin();
        int written = 0;
        for (Vertex v : allVertices) {
            MutableVertex mv = v.modify();
            mv.set(COMPONENT, componentMap.get(v.getIdentity()));
            mv.save();
            written++;
            if (written % 100000 == 0) {
                LOG.info("  [Step 3/3] Writing results: {}% ({}/{})",
                        String.format("%.1f", 100.0 * written / totalVertices),
                        String.format("%,d", written), String.format("%,d", totalVertices));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed WCC algorithm ({} components)", componentId);
    }
}
