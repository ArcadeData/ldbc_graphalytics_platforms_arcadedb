import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.executor.ResultSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Standalone LSQB benchmark for ArcadeDB (embedded mode, Cypher queries).
 * Loads LDBC SNB SF1 data, creates graph schema with Message supertype
 * (Post and Comment extend Message), then runs all 9 LSQB queries via Cypher.
 *
 * Compile:
 *   LDBC_JAR=../graphalytics-1.3.0-arcadedb-0.1-SNAPSHOT/lib/graphalytics-platforms-arcadedb-0.1-SNAPSHOT-default.jar
 *   javac -cp "$LDBC_JAR" ArcadeDBLSQB.java
 *
 * Run:
 *   java -Xms4g -Xmx4g -cp ".:$LDBC_JAR" ArcadeDBLSQB
 */
public class ArcadeDBLSQB {

  static final String DATA_DIR = "/Users/luca/graphs/social-network-sf1-merged-fk";
  static final String DB_PATH  = "/tmp/arcadedb_lsqb";

  public static void main(String[] args) throws Exception {
    System.out.println("======================================================================");
    System.out.println("ArcadeDB LSQB BENCHMARK (embedded, Cypher)");
    System.out.println("======================================================================");

    boolean reset = args.length > 0 && args[0].equals("--reset");
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    Database db;
    long loadTime;

    if (!reset && factory.exists()) {
      System.out.println("\n[ArcadeDB] Reusing existing database at " + DB_PATH);
      db = factory.open();
      loadTime = 0;
    } else {
      if (factory.exists()) deleteDirectory(new java.io.File(DB_PATH));

      // --- LOAD ---
      System.out.println("\n[ArcadeDB] Loading LSQB data...");
      long loadStart = System.currentTimeMillis();

      db = factory.create();
      db.begin();

    // Schema: Message supertype, Post and Comment extend it
    db.getSchema().createVertexType("Country");
    db.getSchema().createVertexType("City");
    db.getSchema().createVertexType("TagClass");
    db.getSchema().createVertexType("Tag");
    db.getSchema().createVertexType("Person");
    db.getSchema().createVertexType("Forum");
    db.getSchema().createVertexType("Message");
    db.getSchema().createVertexType("Post").addSuperType("Message");
    db.getSchema().createVertexType("Comment").addSuperType("Message");

    // ID property + unique index on all types
    for (String t : new String[]{"Country","City","TagClass","Tag","Person","Forum","Message"}) {
      db.getSchema().getType(t).createProperty("cid", Type.LONG);
      db.getSchema().getType(t).createTypeIndex(Schema.INDEX_TYPE.HASH, true, "cid");
    }

    // Edge types
    for (String e : new String[]{"IS_PART_OF","IS_LOCATED_IN","HAS_MEMBER","CONTAINER_OF",
        "REPLY_OF","HAS_TAG","HAS_TYPE","HAS_CREATOR","KNOWS","LIKES","HAS_INTEREST"}) {
      db.getSchema().createEdgeType(e);
    }
    db.commit();

    // Load vertices from merged-fk CSVs (pipe-delimited, with headers)
    System.out.println("  Loading vertices...");
    Map<String, Map<Long, RID>> ridMaps = new HashMap<>();
    for (String[] spec : new String[][]{
        {"Country", "Country.csv"},
        {"City", "City.csv"},
        {"TagClass", "TagClass.csv"},
        {"Tag", "Tag.csv"},
        {"Person", "Person.csv"},
        {"Forum", "Forum.csv"},
        {"Post", "Post.csv"},
        {"Comment", "Comment.csv"}
    }) {
      String type = spec[0], file = spec[1];
      Map<Long, RID> map = new HashMap<>();
      db.begin();
      int count = 0;
      try (BufferedReader br = new BufferedReader(new FileReader(DATA_DIR + "/" + file), 1 << 20)) {
        br.readLine(); // skip header
        String line;
        while ((line = br.readLine()) != null) {
          long cid = Long.parseLong(line.split("\\|")[0]);
          MutableVertex v = db.newVertex(type);
          v.set("cid", cid);
          v.save();
          map.put(cid, v.getIdentity());
          if (++count % 10_000 == 0) { db.commit(); db.begin(); }
        }
      }
      db.commit();
      ridMaps.put(type, map);
      System.out.println("    " + type + ": " + count);
    }

    // Load edges
    System.out.println("  Loading edges...");

    // Helper: load edges from a CSV with two columns (from_id, to_id)
    loadEdgesFromCSV(db, ridMaps, "IS_PART_OF", "City.csv", "City", 0, "Country", "ispartof_country", 1);
    loadEdgesFromCSV(db, ridMaps, "IS_LOCATED_IN", "Person.csv", "Person", 0, "City", "islocatedin_city", 1);
    loadEdgesFromCSV(db, ridMaps, "HAS_TYPE", "Tag.csv", "Tag", 0, "TagClass", "hastype_tagclass", 1);

    // Post edges: HAS_CREATOR (Post->Person) + CONTAINER_OF (Forum->Post)
    loadEdgesFromCSV(db, ridMaps, "HAS_CREATOR", "Post.csv", "Post", 0, "Person", null, 1);
    loadEdgesFromCSV(db, ridMaps, "CONTAINER_OF", "Post.csv", "Forum", 2, "Post", null, 0);

    // Comment edges: HAS_CREATOR (Comment->Person) + REPLY_OF (Comment->Post)
    loadEdgesFromCSV(db, ridMaps, "HAS_CREATOR", "Comment.csv", "Comment", 0, "Person", null, 1);
    loadEdgesFromCSV(db, ridMaps, "REPLY_OF", "Comment.csv", "Comment", 0, "Post", null, 3);

    // Edge tables
    loadEdgeTable(db, ridMaps, "HAS_MEMBER", "Forum_hasMember_Person.csv", "Forum", "Person");
    loadEdgeTable(db, ridMaps, "HAS_TAG", "Comment_hasTag_Tag.csv", "Comment", "Tag");
    loadEdgeTable(db, ridMaps, "HAS_TAG", "Post_hasTag_Tag.csv", "Post", "Tag");
    loadEdgeTable(db, ridMaps, "KNOWS", "Person_knows_Person.csv", "Person", "Person");
    loadEdgeTable(db, ridMaps, "LIKES", "Person_likes_Comment.csv", "Person", "Comment");
    loadEdgeTable(db, ridMaps, "LIKES", "Person_likes_Post.csv", "Person", "Post");
    loadEdgeTable(db, ridMaps, "HAS_INTEREST", "Person_hasInterest_Tag.csv", "Person", "Tag");

      loadTime = System.currentTimeMillis() - loadStart;
      System.out.println("  Load time: " + loadTime / 1000.0 + "s");

      // Free memory from RID maps
      ridMaps = null;
      System.gc();
    } // end of load block

    // Ensure GAV is ready (reuse persisted one, or build fresh)
    System.out.println("\n[ArcadeDB] Preparing Graph Analytical View...");
    long gavStart = System.currentTimeMillis();
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav == null) {
      var allVertexTypes = new String[]{"Country","City","TagClass","Tag","Person","Forum","Post","Comment"};
      var allEdgeTypes = new String[]{"IS_PART_OF","IS_LOCATED_IN","HAS_MEMBER","CONTAINER_OF",
          "REPLY_OF","HAS_TAG","HAS_TYPE","HAS_CREATOR","KNOWS","LIKES","HAS_INTEREST"};
      gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(db)
          .withName("lsqb")
          .withVertexTypes(allVertexTypes)
          .withEdgeTypes(allEdgeTypes)
          .build();
    }
    while (!gav.isReady()) Thread.sleep(100);
    long gavTime = System.currentTimeMillis() - gavStart;
    System.out.println("  GAV ready: " + gavTime / 1000.0 + "s  (nodes=" + gav.getNodeMapping().size() + ")");

    // --- QUERIES ---
    Map<String, String> queries = new LinkedHashMap<>();
    // All queries use named variables to enable the cost-based optimizer + GAV
    queries.put("Q1", "MATCH (co:Country)<-[:IS_PART_OF]-(ci:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum)-[:CONTAINER_OF]->(po:Post)<-[:REPLY_OF]-(cm:Comment)-[:HAS_TAG]->(t:Tag)-[:HAS_TYPE]->(tc:TagClass) RETURN count(*) AS count");
    queries.put("Q2", "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count");
    queries.put("Q3", "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(c1:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(c2:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(c3:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count");
    queries.put("Q4", "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person), (m)<-[:LIKES]-(lk:Person), (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count");
    queries.put("Q5", "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count");
    queries.put("Q6", "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count");
    queries.put("Q7", "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person) OPTIONAL MATCH (m)<-[:LIKES]-(lk:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count");
    queries.put("Q8", "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2 RETURN count(*) AS count");
    queries.put("Q9", "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

    Map<String, Double> results = new LinkedHashMap<>();
    results.put("LOAD", loadTime / 1000.0);

    for (Map.Entry<String, String> e : queries.entrySet()) {
      String qid = e.getKey();
      String query = e.getValue();
      System.out.println("\n[ArcadeDB] Running " + qid + "...");
      long start = System.currentTimeMillis();
      try {
        db.begin();
        ResultSet rs = db.query("opencypher", query);
        long count = -1;
        if (rs.hasNext())
          count = ((Number) rs.next().getProperty("count")).longValue();
        rs.close();
        db.rollback();
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        results.put(qid, elapsed);
        System.out.println("  " + qid + " time: " + elapsed + "s  (count=" + count + ")");
      } catch (Exception ex) {
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("  " + qid + " failed (" + elapsed + "s): " + ex.getMessage());
      }
    }

    // --- SUMMARY ---
    System.out.println("\n======================================================================");
    System.out.println("SUMMARY  -  LSQB SF1");
    System.out.println("======================================================================");
    System.out.printf("%-10s %10s%n", "Query", "ArcadeDB");
    System.out.println("-".repeat(22));
    for (var entry : results.entrySet())
      System.out.printf("%-10s %9.2fs%n", entry.getKey(), entry.getValue());

    db.close();
    deleteDirectory(new java.io.File(DB_PATH));
  }

  // Load edges from entity CSV where FK columns indicate the target
  static void loadEdgesFromCSV(Database db, Map<String, Map<Long, RID>> ridMaps,
      String edgeType, String file, String fromType, int fromColIdx,
      String toType, String toColName, int toColIdx) throws Exception {
    Map<Long, RID> fromMap = ridMaps.get(fromType);
    Map<Long, RID> toMap = ridMaps.get(toType);
    db.begin();
    int count = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(DATA_DIR + "/" + file), 1 << 20)) {
      String header = br.readLine();
      String[] cols = header.split("\\|");
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\\|");
        long fromId = Long.parseLong(parts[fromColIdx]);
        if (toColIdx >= parts.length || parts[toColIdx].isEmpty()) continue;
        long toId = Long.parseLong(parts[toColIdx]);
        RID fromRid = fromMap.get(fromId);
        RID toRid = toMap.get(toId);
        if (fromRid != null && toRid != null) {
          fromRid.asVertex().newEdge(edgeType, toRid);
          count++;
          if (count % 50_000 == 0) { db.commit(); db.begin(); }
        }
      }
    }
    db.commit();
    System.out.println("    " + edgeType + " (" + file + "): " + count);
  }

  // Load edges from a 2-column edge table CSV
  static void loadEdgeTable(Database db, Map<String, Map<Long, RID>> ridMaps,
      String edgeType, String file, String fromType, String toType) throws Exception {
    Map<Long, RID> fromMap = ridMaps.get(fromType);
    Map<Long, RID> toMap = ridMaps.get(toType);
    db.begin();
    int count = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(DATA_DIR + "/" + file), 1 << 20)) {
      br.readLine(); // skip header
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\\|");
        long fromId = Long.parseLong(parts[0]);
        long toId = Long.parseLong(parts[1]);
        RID fromRid = fromMap.get(fromId);
        RID toRid = toMap.get(toId);
        if (fromRid != null && toRid != null) {
          fromRid.asVertex().newEdge(edgeType, toRid);
          count++;
          if (count % 50_000 == 0) { db.commit(); db.begin(); }
        }
      }
    }
    db.commit();
    System.out.println("    " + edgeType + " (" + file + "): " + count);
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
