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
  static final String DB_PATH       = System.getProperty("db.path", "/tmp/arcadedb_benchmark");
  static final String VERTEX_TYPE   = "Vertex";
  static final String EDGE_TYPE     = "EDGE";
  static final String ID_PROP       = "VID";
  static final String WEIGHT_PROP   = "WEIGHT";
  static final int    SOURCE_VERTEX = 6;  // BFS/SSSP source (same as LDBC config)

  public static void main(String[] args) throws Exception {
    System.out.println("======================================================================");
    System.out.println("ArcadeDB BENCHMARK");
    System.out.println("======================================================================");

    boolean reset = false;
    boolean discard = false;
    for (String arg : args) {
      if (arg.equals("--reset")) reset = true;
      if (arg.equals("--discard")) discard = true;
    }

    // A completed load leaves a sibling marker holding the load time in ms. Reuse the database only
    // when the marker exists: a load that was killed half way never wrote it.
    java.io.File marker = new java.io.File(DB_PATH + ".loaded");
    boolean reuse = !reset && marker.exists() && new java.io.File(DB_PATH).isDirectory();

    Database db;
    GraphAnalyticalView gav;
    long loadTime;
    int edgeCount;

    if (reuse) {
      System.out.println("\n[ArcadeDB] Reusing loaded database at " + DB_PATH + " (use --reset to reload)");
      db = new DatabaseFactory(DB_PATH).open();
      gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "benchmark");
      if (gav == null) {
        gav = GraphAnalyticalView.builder(db)
            .withName("benchmark")
            .withVertexTypes(VERTEX_TYPE)
            .withEdgeTypes(EDGE_TYPE)
            .withEdgeProperties(WEIGHT_PROP)
            .build();
      }
      boolean ready = com.arcadedb.graph.GraphTraversalProviderRegistry.awaitAll(db, 120, java.util.concurrent.TimeUnit.SECONDS);
      if (!ready)
        System.err.println("WARNING: GAV did not become ready within 120s");
      edgeCount = (int) db.countType(EDGE_TYPE, false);
      loadTime = Long.parseLong(new String(java.nio.file.Files.readAllBytes(marker.toPath())).trim());
      System.out.println("  Load time of the original load: " + loadTime / 1000.0 + "s (data reused)");
    } else {
    // Clean up previous run
    deleteDirectory(new java.io.File(DB_PATH));
    marker.delete();

    // --- LOAD ---
    System.out.println("\n[ArcadeDB] Loading data...");
    long loadStart = System.currentTimeMillis();

    db = new DatabaseFactory(DB_PATH).create();
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

    edgeCount = 0;
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
    gav = GraphAnalyticalView.builder(db)
        .withName("benchmark")
        .withVertexTypes(VERTEX_TYPE)
        .withEdgeTypes(EDGE_TYPE)
        .withEdgeProperties(WEIGHT_PROP)
        .build();
    long gavTime = System.currentTimeMillis() - gavStart;
    System.out.println("  GAV build: " + gavTime / 1000.0 + "s");

    loadTime = System.currentTimeMillis() - loadStart;
    System.out.println("  Total load time: " + loadTime / 1000.0 + "s");
    java.nio.file.Files.write(marker.toPath(), Long.toString(loadTime).getBytes());
    } // end of load block

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

    // Warm measurement: the first call of every algorithm is the warm-up (executed, never reported); the reported
    // value is the median of -Dwarm.reps (default 5) timed runs in the same JVM, or one timed run when the warm-up
    // took over 60 s.
    final int warmReps = Math.max(1, Integer.getInteger("warm.reps", 5));
    final GraphAnalyticalView g = gav;
    final int src = sourceIdx;

    // PageRank
    System.out.println("\n[ArcadeDB] Running PageRank (damping=0.85, iter=10)...");
    long start = System.currentTimeMillis();
    // BOTH = undirected, as the Graphalytics reference expects (the 4-argument overload without a
    // direction is directed, OUT).
    double[] pr = GraphAlgorithms.pageRank(gav, 0.85, 10, Vertex.DIRECTION.BOTH, EDGE_TYPE);
    double prTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("PR", prTime);
    warm(results, "PR", warmReps, () -> GraphAlgorithms.pageRank(g, 0.85, 10, Vertex.DIRECTION.BOTH, EDGE_TYPE));
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
    warm(results, "WCC", warmReps, () -> GraphAlgorithms.connectedComponents(g, EDGE_TYPE));
    int numComponents = GraphAlgorithms.countComponents(wcc);
    System.out.println("  Components: " + numComponents);
    System.out.println("  WCC time: " + wccTime + "s");

    // BFS
    System.out.println("\n[ArcadeDB] Running BFS from vertex " + SOURCE_VERTEX + "...");
    start = System.currentTimeMillis();
    int[] bfs = GraphAlgorithms.shortestPathAll(gav, sourceIdx, Vertex.DIRECTION.BOTH, EDGE_TYPE);
    double bfsTime = (System.currentTimeMillis() - start) / 1000.0;
    results.put("BFS", bfsTime);
    warm(results, "BFS", warmReps, () -> GraphAlgorithms.shortestPathAll(g, src, Vertex.DIRECTION.BOTH, EDGE_TYPE));
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
    warm(results, "LCC", warmReps, () -> GraphAlgorithms.localClusteringCoefficient(g, EDGE_TYPE));
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
    warm(results, "SSSP", warmReps, () -> GraphAlgorithms.dijkstraSingleSource(g, src, WEIGHT_PROP, Vertex.DIRECTION.BOTH, EDGE_TYPE));
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
    warm(results, "CDLP", warmReps, () -> GraphAlgorithms.labelPropagation(g, 10, EDGE_TYPE));
    System.out.println("  CDLP time: " + cdlpTime + "s");

    // Live heap after a full GC (the graph analytical view, the loaded data and the result arrays): what the engine
    // needs, as opposed to the process size, which is dominated by the fixed -Xms/-Xmx heap.
    System.gc();
    Runtime rt = Runtime.getRuntime();
    System.out.printf("  [memory] live heap after GC: %.0f MB%n", (rt.totalMemory() - rt.freeMemory()) / 1048576.0);

    // --- Optional full-output dump (-Ddump.dir=<dir>) to validate against the LDBC reference outputs ---
    final String dumpDir = System.getProperty("dump.dir");
    if (dumpDir != null) {
      new java.io.File(dumpDir).mkdirs();
      final long[] vids = new long[n];
      db.begin();
      try {
        var vit = db.iterateType(VERTEX_TYPE, false);
        while (vit.hasNext()) {
          Vertex v = vit.next().asVertex();
          int gid = gav.getNodeMapping().getGlobalId(v.getIdentity());
          if (gid >= 0 && gid < n)
            vids[gid] = ((Number) v.get(ID_PROP)).longValue();
        }
      } finally {
        db.rollback();
      }
      dumpDoubles(dumpDir, "PR", vids, pr);
      dumpInts(dumpDir, "WCC", vids, wcc, false);
      dumpDoubles(dumpDir, "LCC", vids, lcc);
      dumpDoubles(dumpDir, "SSSP", vids, sssp);
      dumpInts(dumpDir, "BFS", vids, bfs, true);
      dumpInts(dumpDir, "CDLP", vids, cdlp, false);
    }

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
    if (discard) deleteDirectory(new java.io.File(DB_PATH));
  }

  /** The value already in results (the warm-up call) is replaced by the median of `reps` timed runs. */
  static void warm(Map<String, Double> results, String name, int reps, java.util.function.Supplier<Object> call) {
    int n = results.get(name) > 60.0 ? 1 : reps;
    double[] t = new double[n];
    for (int i = 0; i < n; i++) {
      long s0 = System.nanoTime();
      call.get();
      t[i] = (System.nanoTime() - s0) / 1e9;
    }
    java.util.Arrays.sort(t);
    double median = t[n / 2];
    System.out.printf("  %s: median of %d timed run(s) after the warm-up call: %.3fs%n", name, n, median);
    results.put(name, median);
  }

  static void dumpDoubles(String dir, String algo, long[] vids, double[] vals) throws Exception {
    try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.BufferedWriter(
        new java.io.FileWriter(dir + "/arcadedb-embedded-" + algo + ".out"), 1 << 20))) {
      for (int i = 0; i < vids.length; i++)
        w.println(vids[i] + " " + (Double.isInfinite(vals[i]) ? "infinity" : Double.toString(vals[i])));
    }
    System.out.println("  [dump] " + algo);
  }

  /** bfsStyle: negative = unreachable (written as the LDBC sentinel). Otherwise the value is a node label. */
  static void dumpInts(String dir, String algo, long[] vids, int[] vals, boolean bfsStyle) throws Exception {
    try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.BufferedWriter(
        new java.io.FileWriter(dir + "/arcadedb-embedded-" + algo + ".out"), 1 << 20))) {
      for (int i = 0; i < vids.length; i++) {
        if (bfsStyle)
          w.println(vids[i] + " " + (vals[i] < 0 ? "9223372036854775807" : Integer.toString(vals[i])));
        else
          w.println(vids[i] + " " + vids[vals[i]]);   // labels are dense ids: write the vertex id they stand for
      }
    }
    System.out.println("  [dump] " + algo);
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
