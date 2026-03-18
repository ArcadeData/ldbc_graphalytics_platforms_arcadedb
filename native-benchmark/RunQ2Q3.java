import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class RunQ2Q3 {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav != null) { while (!gav.isReady()) Thread.sleep(100); }
    System.out.println("GAV ready: " + (gav != null ? gav.getNodeMapping().size() + " nodes" : "null"));

    String[][] queries = {
      {"Q2", "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count"},
      {"Q3", "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(c1:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(c2:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(c3:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count"},
    };

    for (String[] q : queries) {
      System.out.println("\nRunning " + q[0] + "...");
      long start = System.currentTimeMillis();
      try {
        db.begin();
        ResultSet rs = db.query("opencypher", q[1]);
        long count = rs.hasNext() ? ((Number) rs.next().getProperty("count")).longValue() : -1;
        rs.close();
        db.rollback();
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("  " + q[0] + ": " + elapsed + "s  (count=" + count + ")");
      } catch (Exception ex) {
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("  " + q[0] + " failed (" + elapsed + "s): " + ex.getMessage());
        ex.printStackTrace();
      }
    }
    db.close();
  }
}
