import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class ExplainCypher {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();
    // Wait for GAV
    var gav = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(db, "lsqb");
    if (gav != null) {
      while (!gav.isReady()) Thread.sleep(100);
      System.out.println("GAV ready: " + gav.getNodeMapping().size() + " nodes");
    } else {
      System.out.println("No GAV found");
    }

    String[] queries = {
      // Q1 original (anonymous nodes)
      "EXPLAIN MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF]-(:Comment)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass) RETURN count(*) AS count",
      // Q1 with named variables
      "EXPLAIN MATCH (co:Country)<-[:IS_PART_OF]-(ci:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum)-[:CONTAINER_OF]->(po:Post)<-[:REPLY_OF]-(cm:Comment)-[:HAS_TAG]->(t:Tag)-[:HAS_TYPE]->(tc:TagClass) RETURN count(*) AS count",
      // Q2
      "EXPLAIN MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count",
      // Q6
      "EXPLAIN MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count",
      // Simple chain
      "EXPLAIN MATCH (a:City)-[:IS_PART_OF]->(b:Country) RETURN count(*) AS count",
    };

    for (String q : queries) {
      System.out.println("\n--- " + q.substring(8, Math.min(80, q.length())) + "... ---");
      db.begin();
      var rs = db.query("opencypher", q);
      while (rs.hasNext()) {
        var r = rs.next();
        // Print all properties
        for (String prop : r.getPropertyNames())
          System.out.println("  " + prop + ": " + r.getProperty(prop));
      }
      rs.close();
      db.rollback();
    }
    db.close();
  }
}
