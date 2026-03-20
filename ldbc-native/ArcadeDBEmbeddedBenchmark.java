import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Standalone LDBC Graphalytics benchmark for ArcadeDB.
 * Loads datagen-7_5-fb once, builds GAV once, runs all 6 algorithms.
 * Comparable to the Kuzu/DuckPGQ Python benchmark scripts.
 */
public class ArcadeDBEmbeddedBenchmark {

  static final String GRAPHS_DIR    = "../datasets/datagen-7_5-fb";
  static final String VERTEX_FILE   = GRAPHS_DIR + "/datagen-7_5-fb.v";
  static final String EDGE_FILE     = GRAPHS_DIR + "/datagen-7_5-fb.e";
  static final String DB_PATH       = "/tmp/arcadedb_benchmark";
  static final String VERTEX_TYPE   = "Vertex";
  static final String EDGE_TYPE     = "EDGE";
  static final String ID_PROP       = "VID";
  static final String WEIGHT_PROP   = "WEIGHT";
  static final int    SOURCE_VERTEX = 6;  // BFS/SSSP source (same as LDBC config)

  public static void main(String[] args) throws Exception {
    System.out.println("======================================================================");
    System.out.println("ArcadeDB BENCHMARK");
    System.out.println("======================================================================");

    // Clean up previous run
    deleteDirectory(new java.io.File(DB_PATH));

    // --- LOAD ---
    System.out.println("\n[ArcadeDB] Loading data...");
    long loadStart = System.currentTimeMillis();

    Database db = new DatabaseFactory(DB_PATH).create();
    db.begin();

    // Create schema
    db.getSchema().createVertexType(VERTEX_TYPE, 8);
    db.getSchema().createEdgeType(EDGE_TYPE, 8);
    db.getSchema().getType(VERTEX_TYPE).createProperty(ID_PROP, Type.LONG);
    db.getSchema().getType(VERTEX_TYPE).createTypeIndex(Schema.INDEX_TYPE.HASH, true, ID_PROP);
    db.getSchema().getType(EDGE_TYPE).createProperty(WEIGHT_PROP, Type.DOUBLE);
    db.commit();

    // Load vertices
    Map<Long, RID> vidToRid = new HashMap<>(700_000);
    db.begin();
    int count = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(VERTEX_FILE), 1 << 20)) {
      String line;
      while ((line = br.readLine()) != null) {
        long vid = Long.parseLong(line.trim());
        MutableVertex v = db.newVertex(VERTEX_TYPE);
        v.set(ID_PROP, vid);
        v.save();
        vidToRid.put(vid, v.getIdentity());
        if (++count % 10_000 == 0) {
          db.commit();
          db.begin();
        }
      }
    }
    db.commit();
    System.out.println("  Vertices: " + count);

    // Load edges
    GraphBatch importer = db.batch()
        .withBatchSize(100_000)
        .withLightEdges(false)
        .withWAL(false)
        .build();

    int edgeCount = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(EDGE_FILE), 1 << 20)) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(" ");
        long src = Long.parseLong(parts[0]);
        long dst = Long.parseLong(parts[1]);
        double weight = Double.parseDouble(parts[2]);
        RID srcRid = vidToRid.get(src);
        RID dstRid = vidToRid.get(dst);
        if (srcRid != null && dstRid != null) {
          importer.newEdge(srcRid, EDGE_TYPE, dstRid, WEIGHT_PROP, weight);
          edgeCount++;
        }
      }
    }
    importer.close();
    System.out.println("  Edges: " + edgeCount);

    // Build GAV
    System.out.println("\n[ArcadeDB] Building Graph Analytical View...");
    long gavStart = System.currentTimeMillis();
    GraphAnalyticalView gav = GraphAnalyticalView.builder(db)
        .withName("benchmark")
        .withVertexTypes(VERTEX_TYPE)
        .withEdgeTypes(EDGE_TYPE)
        .withEdgeProperties(WEIGHT_PROP)
        .build();
    long gavTime = System.currentTimeMillis() - gavStart;
    System.out.println("  GAV build: " + gavTime / 1000.0 + "s");

    long loadTime = System.currentTimeMillis() - loadStart;
    System.out.println("  Total load time: " + loadTime / 1000.0 + "s");

    int n = gav.getNodeMapping().size();
    System.out.println("  GAV nodes: " + n);

    // Find source vertex dense ID
    int sourceIdx = -1;
    db.begin();
    try {
      var it = db.lookupByKey(VERTEX_TYPE, ID_PROP, SOURCE_VERTEX);
      if (it.hasNext()) {
        RID rid = it.next().getIdentity();
        sourceIdx = gav.getNodeMapping().getGlobalId(rid);
      }
    } finally {
      db.rollback();
    }
    System.out.println("  Source vertex " + SOURCE_VERTEX + " -> dense ID " + sourceIdx);

    // --- ALGORITHMS ---
    Map<String, Double> results = new java.util.LinkedHashMap<>();
    results.put("LOAD", loadTime / 1000.0);

    // PageRank
    System.out.println("\n[ArcadeDB] Running PageRank (damping=0.85, iter=10)...");
    long start = System.currentTimeMillis();
    double[] pr = GraphAlgorithms.pageRank(gav, 0.85, 10, EDGE_TYPE);
    double prTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("PR", prTime);
    // Print top 3
    int[] topPR = topK(pr, 3);
    for (int idx : topPR)
      System.out.printf("    Top PR: node=%d, rank=%.6f%n", idx, pr[idx]);
    System.out.println("  PageRank time: " + prTime + "s");

    // WCC
    System.out.println("\n[ArcadeDB] Running WCC...");
    start = System.currentTimeMillis();
    int[] wcc = GraphAlgorithms.connectedComponents(gav, EDGE_TYPE);
    double wccTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("WCC", wccTime);
    int numComponents = GraphAlgorithms.countComponents(wcc);
    System.out.println("  Components: " + numComponents);
    System.out.println("  WCC time: " + wccTime + "s");

    // BFS
    System.out.println("\n[ArcadeDB] Running BFS from vertex " + SOURCE_VERTEX + "...");
    start = System.currentTimeMillis();
    int[] bfs = GraphAlgorithms.shortestPathAll(gav, sourceIdx, Vertex.DIRECTION.BOTH, EDGE_TYPE);
    double bfsTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("BFS", bfsTime);
    int reached = 0;
    for (int d : bfs) if (d >= 0) reached++;
    System.out.println("  Reached: " + reached + " nodes");
    System.out.println("  BFS time: " + bfsTime + "s");

    // LCC
    System.out.println("\n[ArcadeDB] Running LCC...");
    start = System.currentTimeMillis();
    double[] lcc = GraphAlgorithms.localClusteringCoefficient(gav, EDGE_TYPE);
    double lccTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("LCC", lccTime);
    int[] topLCC = topK(lcc, 3);
    for (int idx : topLCC)
      System.out.printf("    Top LCC: node=%d, coeff=%.6f%n", idx, lcc[idx]);
    System.out.println("  LCC time: " + lccTime + "s");

    // SSSP (Dijkstra)
    System.out.println("\n[ArcadeDB] Running SSSP from vertex " + SOURCE_VERTEX + "...");
    start = System.currentTimeMillis();
    double[] sssp = GraphAlgorithms.dijkstraSingleSource(gav, sourceIdx, WEIGHT_PROP,
        Vertex.DIRECTION.BOTH, EDGE_TYPE);
    double ssspTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("SSSP", ssspTime);
    int ssspReached = 0;
    for (double d : sssp) if (d < Double.POSITIVE_INFINITY) ssspReached++;
    System.out.println("  Reached: " + ssspReached + " nodes");
    System.out.println("  SSSP time: " + ssspTime + "s");

    // CDLP
    System.out.println("\n[ArcadeDB] Running CDLP (max_iter=10)...");
    start = System.currentTimeMillis();
    int[] cdlp = GraphAlgorithms.labelPropagation(gav, 10, EDGE_TYPE);
    double cdlpTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("CDLP", cdlpTime);
    System.out.println("  CDLP time: " + cdlpTime + "s");

    // --- SUMMARY ---
    System.out.println("\n======================================================================");
    System.out.println("SUMMARY  -  datagen-7_5-fb (" + n + " vertices, " + edgeCount + " edges)");
    System.out.println("======================================================================");
    System.out.printf("%-10s %10s%n", "Algorithm", "ArcadeDB");
    System.out.println("-".repeat(22));
    for (var e : results.entrySet())
      System.out.printf("%-10s %9.2fs%n", e.getKey(), e.getValue());

    // Cleanup
    db.close();
    deleteDirectory(new java.io.File(DB_PATH));
  }

  static int[] topK(double[] arr, int k) {
    int[] top = new int[k];
    double[] topVal = new double[k];
    java.util.Arrays.fill(topVal, Double.NEGATIVE_INFINITY);
    for (int i = 0; i < arr.length; i++) {
      for (int j = 0; j < k; j++) {
        if (arr[i] > topVal[j]) {
          System.arraycopy(topVal, j, topVal, j + 1, k - j - 1);
          System.arraycopy(top, j, top, j + 1, k - j - 1);
          topVal[j] = arr[i];
          top[j] = i;
          break;
        }
      }
    }
    return top;
  }

  static void deleteDirectory(java.io.File dir) {
    if (!dir.exists()) return;
    java.io.File[] files = dir.listFiles();
    if (files != null)
      for (java.io.File f : files)
        if (f.isDirectory()) deleteDirectory(f);
        else f.delete();
    dir.delete();
  }
}
