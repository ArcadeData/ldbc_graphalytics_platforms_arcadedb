import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.Result;

/**
 * Quick test to profile OpenCypher query execution and check if GAV is used.
 * Reuses the existing /tmp/arcadedb_lsqb database.
 */
public class TestCypher {
  public static void main(String[] args) throws Exception {
    String dbPath = "/tmp/arcadedb_lsqb";
    if (!new java.io.File(dbPath).exists()) {
      System.out.println("Database not found at " + dbPath + " — run ArcadeDBLSQB first to create it.");
      return;
    }

    Database db = new DatabaseFactory(dbPath).open();

    // Ensure all GAVs are ready (including async-restored ones)
    System.out.println("Waiting for GAVs...");
    long gavStart = System.currentTimeMillis();
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav == null) {
      gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(db)
          .withName("lsqb")
          .withVertexTypes("Country","City","TagClass","Tag","Person","Forum","Post","Comment")
          .withEdgeTypes("IS_PART_OF","IS_LOCATED_IN","HAS_MEMBER","CONTAINER_OF",
              "REPLY_OF","HAS_TAG","HAS_TYPE","HAS_CREATOR","KNOWS","LIKES","HAS_INTEREST")
          .build();
    }
    if (!com.arcadedb.graph.GraphTraversalProviderRegistry.awaitAll(db, 60, java.util.concurrent.TimeUnit.SECONDS))
      System.err.println("WARNING: Some GAVs did not become ready within 60s");
    System.out.println("GAV ready in " + (System.currentTimeMillis() - gavStart) + "ms, nodes=" + gav.getNodeMapping().size());

    // Test simple counts first
    System.out.println("\n--- SQL counts ---");
    db.begin();
    for (String type : new String[]{"Person","Post","Comment","KNOWS","HAS_TAG","REPLY_OF","HAS_CREATOR","CONTAINER_OF"}) {
      ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + type);
      if (rs.hasNext()) System.out.println("  " + type + ": " + rs.next().getProperty("c"));
      rs.close();
    }
    db.rollback();

    // Test a simple OpenCypher query
    System.out.println("\n--- Simple OpenCypher ---");
    db.begin();
    long start = System.currentTimeMillis();
    ResultSet rs = db.query("opencypher", "MATCH (p:Person) RETURN count(p) AS count");
    if (rs.hasNext()) {
      Result r = rs.next();
      System.out.println("  Person count: " + r.getProperty("count") + " in " + (System.currentTimeMillis() - start) + "ms");
    }
    rs.close();
    db.rollback();

    // Test a 2-hop pattern
    System.out.println("\n--- 2-hop OpenCypher ---");
    db.begin();
    start = System.currentTimeMillis();
    rs = db.query("opencypher", "MATCH (:City)-[:IS_PART_OF]->(:Country) RETURN count(*) AS count");
    if (rs.hasNext()) {
      Result r = rs.next();
      System.out.println("  City->Country: " + r.getProperty("count") + " in " + (System.currentTimeMillis() - start) + "ms");
    }
    rs.close();
    db.rollback();

    // Test Q2 (simpler query)
    System.out.println("\n--- Q2 OpenCypher ---");
    db.begin();
    start = System.currentTimeMillis();
    rs = db.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person), " +
        "(p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) " +
        "RETURN count(*) AS count");
    if (rs.hasNext()) {
      Result r = rs.next();
      System.out.println("  Q2 count: " + r.getProperty("count") + " in " + (System.currentTimeMillis() - start) + "ms");
    }
    rs.close();
    db.rollback();

    // Test EXPLAIN on Q2
    System.out.println("\n--- EXPLAIN Q2 (SQL MATCH equivalent) ---");
    db.begin();
    rs = db.query("sql",
        "EXPLAIN MATCH {type: Person, as: p1} -KNOWS- {type: Person, as: p2}, " +
        "{as: p1} <-HAS_CREATOR- {type: Comment, as: cm} -REPLY_OF-> {type: Post, as: po} " +
        "-HAS_CREATOR-> {as: p2} RETURN count(*) as count");
    while (rs.hasNext()) {
      System.out.println("  " + rs.next().toJSON());
    }
    rs.close();
    db.rollback();

    db.close();
  }
}
