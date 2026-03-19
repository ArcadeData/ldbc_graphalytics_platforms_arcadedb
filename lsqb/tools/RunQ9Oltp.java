import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class RunQ9Oltp {
  public static void main(String[] args) throws Exception {
    System.out.println("Opening database (OLTP only, no GAV)...");
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();

    System.out.println("Running Q9 (OLTP)...");
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
      System.out.printf("  Q9 OLTP: %.2fs  (count=%d)  expected=1596153418  %s%n",
          elapsed, count, count == 1596153418L ? "CORRECT" : "WRONG");
    } catch (Exception ex) {
      double elapsed = (System.currentTimeMillis() - start) / 1000.0;
      System.out.printf("  Q9 OLTP failed (%.2fs): %s%n", elapsed, ex.getMessage());
      ex.printStackTrace();
    }
    db.close();
  }
}
