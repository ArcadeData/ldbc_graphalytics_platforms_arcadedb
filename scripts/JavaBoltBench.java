import org.neo4j.driver.*;
import java.util.*;

public class JavaBoltBench {
  public static void main(String[] a) {
    String[][] calls = {
      {"vertex ids (633K x 1)", "MATCH (v:Vertex) RETURN v.VID AS id"},
      {"edge sample (2M x 2)", "MATCH (a:Vertex)-[:EDGE]->(b:Vertex) RETURN a.VID AS src, b.VID AS dst LIMIT 2000000"}};
    for (long fetch : new long[] {1000, -1}) {
      Config cfg = Config.builder().withFetchSize(fetch).build();
      try (Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", "benchmark"), cfg)) {
        driver.verifyConnectivity();
        for (String[] c : calls) {
          double[] t = new double[4]; long rows = 0;
          for (int i = 0; i < 4; i++) {
            long t0 = System.nanoTime(); rows = 0; long sum = 0;
            try (Session s = driver.session(SessionConfig.forDatabase("bench"))) {
              Result r = s.run(c[1]);
              while (r.hasNext()) { org.neo4j.driver.Record rec = r.next(); sum += rec.get(0).asLong(); rows++; }
            }
            t[i] = (System.nanoTime() - t0) / 1e9;
          }
          double[] timed = Arrays.copyOfRange(t, 1, 4); Arrays.sort(timed);
          System.out.printf("JAVA fetch=%d %-22s rows=%d warmup=%.2fs median=%.2fs runs=%s%n", fetch, c[0], rows, t[0], timed[1], Arrays.toString(Arrays.copyOfRange(t, 1, 4)));
        }
      }
    }
  }
}
