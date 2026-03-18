import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class RunQ5 {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav != null) { while (!gav.isReady()) Thread.sleep(100); }
    System.out.println("GAV ready: " + (gav != null ? gav.getNodeMapping().size() + " nodes" : "null"));

    System.out.println("\nRunning Q5...");
    long start = System.currentTimeMillis();
    try {
      db.begin();
      ResultSet rs = db.query("opencypher",
          "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) " +
          "WHERE t1 <> t2 RETURN count(*) AS count");
      long count = rs.hasNext() ? ((Number) rs.next().getProperty("count")).longValue() : -1;
      rs.close();
      db.rollback();
      double elapsed = (System.currentTimeMillis() - start) / 1000.0;
      System.out.println("  Q5: " + elapsed + "s  (count=" + count + ")  expected=13824510");
    } catch (Exception ex) {
      double elapsed = (System.currentTimeMillis() - start) / 1000.0;
      System.out.println("  Q5 failed (" + elapsed + "s): " + ex.getMessage());
      ex.printStackTrace();
    }
    db.close();
  }
}
