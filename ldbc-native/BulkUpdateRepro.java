import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;

/**
 * Reproducer for ArcadeDB #8660 (quadratic LocalBucket.findAvailableSpace on bulk updates that grow records).
 *
 * Mode 1 BFS / SSSP initialise every vertex with one SQL bulk UPDATE ("UPDATE Vertex SET DISTANCE = ..."). Each new
 * property makes every record bigger; once the pages are full, every record that no longer fits asks the bucket for
 * free space. With the quadratic gather this took 65-260 s instead of ~3 s.
 *
 * Opens an already loaded Graphalytics database (never the shared one: run it on a copy, scripts/bulk_update_repro.py
 * does that) and times one bulk UPDATE per algorithm property, in the order given by -Dorder.
 *
 *   java -Ddb.path=<copy> -Dorder=BFS,WCC,PR,CDLP,LCC,SSSP -cp ... BulkUpdateRepro
 *
 * Output lines: `UPDATE <ALGO> <seconds>s` and `TOTAL <seconds>s`.
 */
public class BulkUpdateRepro {
  static final String DB_PATH = System.getProperty("db.path");
  static final String ORDER   = System.getProperty("order", "BFS,WCC,PR,CDLP,LCC,SSSP");

  public static void main(final String[] args) {
    if (DB_PATH == null)
      throw new IllegalArgumentException("-Ddb.path=<copy of a loaded Graphalytics database> is required");
    final Database db = new DatabaseFactory(DB_PATH).open();
    try {
      System.out.println("[repro] database " + DB_PATH + ", order " + ORDER);
      double total = 0;
      for (final String algo : ORDER.split(",")) {
        final String set = switch (algo.trim()) {
          case "BFS" -> "DISTANCE = 9223372036854775807";
          case "WCC" -> "COMPONENT = 1234567890123";
          case "PR" -> "PAGERANK = 0.15";
          case "CDLP" -> "LABEL = 1234567890123";
          case "LCC" -> "LCC = 0.5";
          case "SSSP" -> "SSSP = Infinity";
          default -> throw new IllegalArgumentException("unknown algorithm " + algo);
        };
        final long t0 = System.nanoTime();
        db.begin();
        db.command("sql", "UPDATE Vertex SET " + set);
        db.commit();
        final double s = (System.nanoTime() - t0) / 1e9;
        total += s;
        System.out.printf("UPDATE %s %.2fs%n", algo.trim(), s);
      }
      System.out.printf("TOTAL %.2fs%n", total);
    } finally {
      db.close();
    }
  }
}
