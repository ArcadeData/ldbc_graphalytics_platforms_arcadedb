import com.arcadedb.database.*;
import com.arcadedb.query.sql.executor.*;

public class DiagnoseQ3 {
  public static void main(String[] args) throws Exception {
    var db = new DatabaseFactory("/tmp/arcadedb_lsqb").open();

    db.begin();

    // 1. How many Persons have IS_LOCATED_IN edges?
    var rs = db.query("sql", "SELECT count(*) AS c FROM Person WHERE out('IS_LOCATED_IN').size() > 0");
    System.out.println("Persons with IS_LOCATED_IN: " + rs.next().getProperty("c"));
    rs.close();

    // Total persons
    rs = db.query("sql", "SELECT count(*) AS c FROM Person");
    System.out.println("Total Persons: " + rs.next().getProperty("c"));
    rs.close();

    // 2. Total IS_LOCATED_IN edges
    rs = db.query("sql", "SELECT count(*) AS c FROM IS_LOCATED_IN");
    System.out.println("IS_LOCATED_IN edges: " + rs.next().getProperty("c"));
    rs.close();

    // 3. KNOWS edges (should be unidirectional in storage — loaded from CSV once)
    rs = db.query("sql", "SELECT count(*) AS c FROM KNOWS");
    System.out.println("KNOWS edges: " + rs.next().getProperty("c"));
    rs.close();

    // 4. How many KNOWS edges exist per direction check?
    // In LSQB, KNOWS is undirected. DuckDB loads both directions.
    // ArcadeDB stores edges once — Cypher -[:KNOWS]- traverses both directions.
    // Check: for a sample person, how many KNOWS neighbors via BOTH?
    rs = db.query("sql", "SELECT first(cid) AS id FROM Person");
    long sampleId = ((Number) rs.next().getProperty("id")).longValue();
    rs.close();

    rs = db.query("sql", "SELECT out('KNOWS').size() AS outK, in('KNOWS').size() AS inK, both('KNOWS').size() AS bothK FROM Person WHERE cid = " + sampleId);
    var row = rs.next();
    System.out.println("Sample Person " + sampleId + ": outKNOWS=" + row.getProperty("outK") + " inKNOWS=" + row.getProperty("inK") + " bothKNOWS=" + row.getProperty("bothK"));
    rs.close();

    // 5. Countries and cities
    rs = db.query("sql", "SELECT count(*) AS c FROM Country");
    System.out.println("Countries: " + rs.next().getProperty("c"));
    rs.close();
    rs = db.query("sql", "SELECT count(*) AS c FROM City");
    System.out.println("Cities: " + rs.next().getProperty("c"));
    rs.close();
    rs = db.query("sql", "SELECT count(*) AS c FROM IS_PART_OF");
    System.out.println("IS_PART_OF edges: " + rs.next().getProperty("c"));
    rs.close();

    // 6. Run Q3 via SQL MATCH to cross-check
    System.out.println("\nRunning Q3 via OpenCypher...");
    long start = System.currentTimeMillis();
    rs = db.query("opencypher",
        "MATCH (co:Country) " +
        "MATCH (p1:Person)-[:IS_LOCATED_IN]->(c1:City)-[:IS_PART_OF]->(co) " +
        "MATCH (p2:Person)-[:IS_LOCATED_IN]->(c2:City)-[:IS_PART_OF]->(co) " +
        "MATCH (p3:Person)-[:IS_LOCATED_IN]->(c3:City)-[:IS_PART_OF]->(co) " +
        "MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) " +
        "RETURN count(*) AS count");
    long count = ((Number) rs.next().getProperty("count")).longValue();
    rs.close();
    System.out.println("Q3 OpenCypher count: " + count + " (expected 753570) time: " + (System.currentTimeMillis() - start) + "ms");

    // 7. Run a simpler triangle count for comparison
    // Count triangles in the entire KNOWS graph (no country filter)
    System.out.println("\nCounting all KNOWS triangles (no country filter)...");
    start = System.currentTimeMillis();
    rs = db.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:KNOWS]-(p1) " +
        "WHERE id(p1) < id(p2) AND id(p2) < id(p3) " +
        "RETURN count(*) AS count");
    count = ((Number) rs.next().getProperty("count")).longValue();
    rs.close();
    System.out.println("Unique triangles (p1<p2<p3): " + count + " time: " + (System.currentTimeMillis() - start) + "ms");

    db.rollback();
    db.close();
  }
}
