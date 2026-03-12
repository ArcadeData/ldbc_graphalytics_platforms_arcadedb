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
package science.atlarge.graphalytics.arcadedb.metrics.pr;

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
 * Implementation of the PageRank algorithm using iterative computation
 * on the ArcadeDB Java graph API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageRankComputation {

    private static final Logger LOG = LogManager.getLogger();

    private final Database graphDatabase;
    private final int maxIterations;
    private final float dampingFactor;
    private final boolean directed;

    public PageRankComputation(Database graphDatabase, int maxIterations, float dampingFactor, boolean directed) {
        this.graphDatabase = graphDatabase;
        this.maxIterations = maxIterations;
        this.dampingFactor = dampingFactor;
        this.directed = directed;
    }

    public void run() {
        LOG.debug("- Starting PageRank algorithm (iterations={}, damping={})", maxIterations, dampingFactor);

        // Collect all vertices and build adjacency
        List<Vertex> vertices = new ArrayList<>();
        Map<RID, Integer> ridToIndex = new HashMap<>();
        Iterator<Vertex> it = graphDatabase.iterateType(VERTEX_TYPE, false);
        while (it.hasNext()) {
            Vertex v = it.next();
            ridToIndex.put(v.getIdentity(), vertices.size());
            vertices.add(v);
        }

        int n = vertices.size();
        double[] ranks = new double[n];
        double[] newRanks = new double[n];
        int[] outDegree = new int[n];
        List<List<Integer>> inNeighbors = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            ranks[i] = 1.0 / n;
            inNeighbors.add(new ArrayList<>());
        }

        // Build adjacency structure
        for (int i = 0; i < n; i++) {
            Vertex v = vertices.get(i);
            int outCount = 0;
            for (Edge edge : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)) {
                Integer targetIdx = ridToIndex.get(edge.getInVertex().getIdentity());
                if (targetIdx != null) {
                    inNeighbors.get(targetIdx).add(i);
                    outCount++;
                }
            }
            if (!directed) {
                for (Edge edge : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE)) {
                    Integer targetIdx = ridToIndex.get(edge.getOutVertex().getIdentity());
                    if (targetIdx != null) {
                        inNeighbors.get(targetIdx).add(i);
                        outCount++;
                    }
                }
            }
            outDegree[i] = outCount;
        }

        // If undirected, outDegree needs to count both directions
        if (!directed) {
            for (int i = 0; i < n; i++) {
                outDegree[i] = 0;
                Vertex v = vertices.get(i);
                for (Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
                    outDegree[i]++;
                for (Edge ignored : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
                    outDegree[i]++;
            }
        }

        // Iterate
        for (int iter = 0; iter < maxIterations; iter++) {
            double danglingSum = 0;
            for (int i = 0; i < n; i++) {
                if (outDegree[i] == 0)
                    danglingSum += ranks[i];
            }

            for (int i = 0; i < n; i++) {
                double sum = 0;
                for (int j : inNeighbors.get(i)) {
                    sum += ranks[j] / outDegree[j];
                }
                newRanks[i] = (1 - dampingFactor) / n + dampingFactor * (sum + danglingSum / n);
            }

            System.arraycopy(newRanks, 0, ranks, 0, n);
        }

        // Write results
        graphDatabase.begin();
        for (int i = 0; i < n; i++) {
            MutableVertex mv = vertices.get(i).modify();
            mv.set(PAGERANK, ranks[i]);
            mv.save();
        }
        graphDatabase.commit();

        LOG.debug("- Completed PageRank algorithm");
    }
}
