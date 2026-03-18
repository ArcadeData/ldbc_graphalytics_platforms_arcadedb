import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class RunQueries {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav != null) { while (!gav.isReady()) Thread.sleep(100); }
    System.out.println("GAV ready: " + (gav != null ? gav.getNodeMapping().size() + " nodes" : "null"));

    String[][] queries = {
      {"Q1", "MATCH (co:Country)<-[:IS_PART_OF]-(ci:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum)-[:CONTAINER_OF]->(po:Post)<-[:REPLY_OF]-(cm:Comment)-[:HAS_TAG]->(t:Tag)-[:HAS_TYPE]->(tc:TagClass) RETURN count(*) AS count"},
      {"Q2", "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count"},
      // skip Q3 — too slow
      {"Q4", "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person), (m)<-[:LIKES]-(lk:Person), (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count"},
      {"Q5", "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count"},
      {"Q6", "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count"},
      {"Q7", "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person) OPTIONAL MATCH (m)<-[:LIKES]-(lk:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count"},
      {"Q8", "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2 RETURN count(*) AS count"},
      {"Q9", "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count"},
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
        System.out.println("  " + q[0] + " time: " + elapsed + "s  (count=" + count + ")");
      } catch (Exception ex) {
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("  " + q[0] + " failed (" + elapsed + "s): " + ex.getMessage());
      }
    }
    db.close();
  }
}
