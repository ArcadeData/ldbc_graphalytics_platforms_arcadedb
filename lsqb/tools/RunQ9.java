import com.arcadedb.database.*;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.query.sql.executor.*;
import java.util.concurrent.TimeUnit;

public class RunQ9 {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    if (!GraphTraversalProviderRegistry.awaitAll(db, 60, TimeUnit.SECONDS))
      System.err.println("WARNING: Some GAVs did not become ready within 60s");
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    System.out.println("GAV ready: " + (gav != null ? gav.getNodeMapping().size() + " nodes" : "null"));

    System.out.println("\nRunning Q9...");
    long start = System.currentTimeMillis();
    try {
      db.begin();
      ResultSet rs = db.query("opencypher",
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) " +
          "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");
      long count = rs.hasNext() ? ((Number) rs.next().getProperty("count")).longValue() : -1;
      rs.close();
      db.rollback();
      double elapsed = (System.currentTimeMillis() - start) / 1000.0;
      System.out.println("  Q9: " + elapsed + "s  (count=" + count + ")  expected=1596153418");
    } catch (Exception ex) {
      double elapsed = (System.currentTimeMillis() - start) / 1000.0;
      System.out.println("  Q9 failed (" + elapsed + "s): " + ex.getMessage());
      ex.printStackTrace();
    }
    db.close();
  }
}
