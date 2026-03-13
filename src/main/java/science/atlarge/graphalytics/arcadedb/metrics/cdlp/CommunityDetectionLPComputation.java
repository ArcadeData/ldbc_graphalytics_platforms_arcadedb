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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static science.atlarge.graphalytics.arcadedb.ArcadeDBConstants.*;

/**
 * CDLP computation using VID-based labels as required by the LDBC Graphalytics spec.
 * Cannot use the built-in algo.labelpropagation because LDBC requires labels to be
 * vertex IDs (not sequential indices), and tie-breaking must be on smallest VID.
 * <p>
 * Uses CSR acceleration via GraphTraversalProvider when a GraphAnalyticalView is available.
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
        LOG.info("- Starting CDLP algorithm (maxIterations={})", maxIterations);

        // Try CSR-accelerated path
        final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(graphDatabase, EDGE_TYPE);
        if (provider != null)
            runWithCSR(provider);
        else
            runWithOLTP();
    }

    private void runWithCSR(GraphTraversalProvider provider) {
        LOG.info("  [Step 1/3] Building adjacency from CSR (accelerated)...");
        final int n = provider.getNodeCount();

        // Initialize labels with VIDs (LDBC requirement)
        final long[] label = new long[n];
        for (int i = 0; i < n; i++) {
            Vertex v = provider.getRID(i).asVertex();
            label[i] = v.getLong(ID_PROPERTY);
        }

        // Get CSR-backed neighbor view — BOTH direction for undirected treatment
        final Vertex.DIRECTION direction = directed ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
        final NeighborView view = provider.getNeighborView(direction, EDGE_TYPE);
        LOG.info("  [Step 1/3] CSR adjacency ready for {} vertices (NeighborView: {}).",
                String.format("%,d", n), view != null ? "zero-copy" : "per-node");

        // Synchronous label propagation with VID-based labels
        long[] currentLabel = label;
        for (int iter = 0; iter < maxIterations; iter++) {
            LOG.info("  [Step 2/3] CDLP iteration {}/{} ...", iter + 1, maxIterations);
            long[] newLabel = new long[n];
            boolean changed = false;

            for (int i = 0; i < n; i++) {
                final int degree;
                if (view != null) {
                    degree = view.degree(i);
                } else {
                    degree = provider.getNeighborIds(i, direction, EDGE_TYPE).length;
                }

                if (degree == 0) {
                    newLabel[i] = currentLabel[i];
                    continue;
                }

                // Count neighbor labels
                Map<Long, Integer> labelCounts = new HashMap<>();
                if (view != null) {
                    final int[] nbrs = view.neighbors();
                    for (int j = view.offset(i), end = view.offsetEnd(i); j < end; j++)
                        labelCounts.merge(currentLabel[nbrs[j]], 1, Integer::sum);
                } else {
                    for (int neighborIdx : provider.getNeighborIds(i, direction, EDGE_TYPE))
                        labelCounts.merge(currentLabel[neighborIdx], 1, Integer::sum);
                }

                // Pick most frequent label, break ties by smallest label (VID)
                int maxCount = 0;
                long bestLabel = currentLabel[i];
                for (Map.Entry<Long, Integer> entry : labelCounts.entrySet()) {
                    if (entry.getValue() > maxCount || (entry.getValue() == maxCount && entry.getKey() < bestLabel)) {
                        maxCount = entry.getValue();
                        bestLabel = entry.getKey();
                    }
                }

                newLabel[i] = bestLabel;
                if (bestLabel != currentLabel[i])
                    changed = true;
            }

            currentLabel = newLabel;
            if (!changed)
                break;
        }

        // Write results
        LOG.info("  [Step 3/3] Writing results...");
        graphDatabase.begin();
        for (int i = 0; i < n; i++) {
            MutableVertex mv = provider.getRID(i).asVertex().modify();
            mv.set(LABEL, currentLabel[i]);
            mv.save();
            if ((i + 1) % 100000 == 0)
                LOG.info("  [Step 3/3] Writing results: {}% ({}/{})",
                        String.format("%.1f", 100.0 * (i + 1) / n),
                        String.format("%,d", i + 1), String.format("%,d", n));
        }
        graphDatabase.commit();

        LOG.info("- Completed CDLP algorithm ({} iterations on {} vertices, CSR accelerated)", maxIterations, String.format("%,d", n));
    }

    private void runWithOLTP() {
        LOG.info("  [Step 1/3] Collecting vertices and initializing labels (OLTP)...");
        List<Vertex> vertices = new ArrayList<>();
        Map<RID, Integer> ridToIdx = new HashMap<>();

        Iterator<Vertex> it = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (it.hasNext()) {
            Vertex v = it.next();
            ridToIdx.put(v.getIdentity(), vertices.size());
            vertices.add(v);
        }
        int n = vertices.size();

        // Initialize labels with VIDs (LDBC requirement)
        long[] label = new long[n];
        for (int i = 0; i < n; i++)
            label[i] = vertices.get(i).getLong(ID_PROPERTY);

        // Build adjacency once for performance
        int[][] adj = new int[n][];
        for (int i = 0; i < n; i++) {
            Vertex v = vertices.get(i);
            List<Integer> neighbors = new ArrayList<>();
            for (Edge edge : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                Integer idx = ridToIdx.get(edge.getInVertex().getIdentity());
                if (idx != null)
                    neighbors.add(idx);
            }
            for (Edge edge : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                Integer idx = ridToIdx.get(edge.getOutVertex().getIdentity());
                if (idx != null)
                    neighbors.add(idx);
            }
            adj[i] = neighbors.stream().mapToInt(Integer::intValue).toArray();
        }
        LOG.info("  [Step 1/3] Initialized {} vertices.", String.format("%,d", n));

        // Synchronous label propagation with VID-based labels
        for (int iter = 0; iter < maxIterations; iter++) {
            LOG.info("  [Step 2/3] CDLP iteration {}/{} ...", iter + 1, maxIterations);
            long[] newLabel = new long[n];
            boolean changed = false;

            for (int i = 0; i < n; i++) {
                if (adj[i].length == 0) {
                    newLabel[i] = label[i];
                    continue;
                }

                // Count neighbor labels
                Map<Long, Integer> labelCounts = new HashMap<>();
                for (int neighborIdx : adj[i])
                    labelCounts.merge(label[neighborIdx], 1, Integer::sum);

                // Pick most frequent label, break ties by smallest label (VID)
                int maxCount = 0;
                long bestLabel = label[i];
                for (Map.Entry<Long, Integer> entry : labelCounts.entrySet()) {
                    if (entry.getValue() > maxCount || (entry.getValue() == maxCount && entry.getKey() < bestLabel)) {
                        maxCount = entry.getValue();
                        bestLabel = entry.getKey();
                    }
                }

                newLabel[i] = bestLabel;
                if (bestLabel != label[i])
                    changed = true;
            }

            label = newLabel;
            if (!changed)
                break;
        }

        // Write results
        LOG.info("  [Step 3/3] Writing results...");
        graphDatabase.begin();
        for (int i = 0; i < n; i++) {
            MutableVertex mv = vertices.get(i).modify();
            mv.set(LABEL, label[i]);
            mv.save();
            if ((i + 1) % 100000 == 0)
                LOG.info("  [Step 3/3] Writing results: {}% ({}/{})",
                        String.format("%.1f", 100.0 * (i + 1) / n),
                        String.format("%,d", i + 1), String.format("%,d", n));
        }
        graphDatabase.commit();

        LOG.info("- Completed CDLP algorithm ({} iterations on {} vertices)", maxIterations, String.format("%,d", n));
    }
}
