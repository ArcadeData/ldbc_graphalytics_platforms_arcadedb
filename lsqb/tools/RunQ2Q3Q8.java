import com.arcadedb.database.*;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.query.sql.executor.*;
import java.util.concurrent.TimeUnit;

public class RunQ2Q3Q8 {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    if (!GraphTraversalProviderRegistry.awaitAll(db, 60, TimeUnit.SECONDS))
      System.err.println("WARNING: Some GAVs did not become ready within 60s");
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    System.out.println("GAV ready: " + (gav != null ? gav.getNodeMapping().size() + " nodes" : "null"));

    String[][] queries = {
      {"Q2", "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count", "1085627"},
      {"Q3", "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(c1:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(c2:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(c3:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count", "753570"},
      {"Q8", "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2 RETURN count(*) AS count", "6907213"},
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
        System.out.println("  " + q[0] + ": " + elapsed + "s  (count=" + count + ")  expected=" + q[2]);
      } catch (Exception ex) {
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("  " + q[0] + " failed (" + elapsed + "s): " + ex.getMessage());
        ex.printStackTrace();
      }
    }
    db.close();
  }
}
