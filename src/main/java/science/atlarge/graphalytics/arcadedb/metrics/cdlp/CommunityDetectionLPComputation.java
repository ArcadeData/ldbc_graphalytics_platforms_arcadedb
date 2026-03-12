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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * Implementation of the community detection (label propagation) algorithm
 * using the ArcadeDB Java graph API.
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
        LOG.info("- Starting Community Detection Label Propagation algorithm (maxIterations={})", maxIterations);

        // Collect vertices and initialize labels with VID
        LOG.info("  [Step 1/3] Collecting vertices and initializing labels...");
        List<Vertex> vertices = new ArrayList<>();
        Map<RID, Long> labels = new HashMap<>();

        Iterator<Vertex> it = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (it.hasNext()) {
            Vertex v = it.next();
            vertices.add(v);
            labels.put(v.getIdentity(), v.getLong(ID_PROPERTY));
        }
        int n = vertices.size();
        LOG.info("  [Step 1/3] Initialized {} vertices.", String.format("%,d", n));

        // Iterate
        for (int iter = 0; iter < maxIterations; iter++) {
            LOG.info("  [Step 2/3] CDLP iteration {}/{} ...", iter + 1, maxIterations);
            Map<RID, Long> newLabels = new HashMap<>();

            for (Vertex v : vertices) {
                // Count neighbor labels
                Map<Long, Integer> labelCounts = new HashMap<>();

                if (directed) {
                    // For directed: use incoming edges
                    for (Edge edge : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                        Long neighborLabel = labels.get(edge.getOutVertex().getIdentity());
                        if (neighborLabel != null)
                            labelCounts.merge(neighborLabel, 1, Integer::sum);
                    }
                    for (Edge edge : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                        Long neighborLabel = labels.get(edge.getInVertex().getIdentity());
                        if (neighborLabel != null)
                            labelCounts.merge(neighborLabel, 1, Integer::sum);
                    }
                } else {
                    for (Edge edge : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                        Long neighborLabel = labels.get(edge.getInVertex().getIdentity());
                        if (neighborLabel != null)
                            labelCounts.merge(neighborLabel, 1, Integer::sum);
                    }
                    for (Edge edge : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                        Long neighborLabel = labels.get(edge.getOutVertex().getIdentity());
                        if (neighborLabel != null)
                            labelCounts.merge(neighborLabel, 1, Integer::sum);
                    }
                }

                if (labelCounts.isEmpty()) {
                    newLabels.put(v.getIdentity(), labels.get(v.getIdentity()));
                } else {
                    // Pick most frequent label, break ties by choosing smallest label
                    int maxCount = Collections.max(labelCounts.values());
                    long bestLabel = Long.MAX_VALUE;
                    for (Map.Entry<Long, Integer> entry : labelCounts.entrySet()) {
                        if (entry.getValue() == maxCount && entry.getKey() < bestLabel) {
                            bestLabel = entry.getKey();
                        }
                    }
                    newLabels.put(v.getIdentity(), bestLabel);
                }
            }

            labels = newLabels;
        }

        // Write results
        LOG.info("  [Step 3/3] Writing results...");
        graphDatabase.begin();
        int written = 0;
        for (Vertex v : vertices) {
            MutableVertex mv = v.modify();
            mv.set(LABEL, labels.get(v.getIdentity()));
            mv.save();
            written++;
            if (written % 100000 == 0) {
                LOG.info("  [Step 3/3] Writing results: {}% ({}/{})",
                        String.format("%.1f", 100.0 * written / n),
                        String.format("%,d", written), String.format("%,d", n));
            }
        }
        graphDatabase.commit();

        LOG.info("- Completed CDLP algorithm ({} iterations on {} vertices)", maxIterations, String.format("%,d", n));
    }
}
