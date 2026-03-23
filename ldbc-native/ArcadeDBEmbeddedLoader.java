
import com.arcadedb.database.*;
import com.arcadedb.graph.*;
import com.arcadedb.schema.*;
import java.io.*;
import java.util.*;

public class ArcadeDBEmbeddedLoader {
    public static void main(String[] args) throws Exception {
        String dbPath = args[0];
        String vertexFile = args[1];
        String edgeFile = args[2];

        Database db = new DatabaseFactory(dbPath).create();
        db.begin();
        db.getSchema().createVertexType("Vertex", 8);
        db.getSchema().createEdgeType("EDGE", 8);
        db.getSchema().getType("Vertex").createProperty("VID", Type.LONG);
        db.getSchema().getType("Vertex").createTypeIndex(Schema.INDEX_TYPE.HASH, true, "VID");
        db.getSchema().getType("EDGE").createProperty("WEIGHT", Type.DOUBLE);
        db.commit();

        Map<Long, RID> vidToRid = new HashMap<>(700_000);
        db.begin();
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(vertexFile), 1 << 20)) {
            String line;
            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.trim());
                MutableVertex v = db.newVertex("Vertex");
                v.set("VID", vid);
                v.save();
                vidToRid.put(vid, v.getIdentity());
                if (++count % 10_000 == 0) { db.commit(); db.begin(); }
            }
        }
        db.commit();
        System.out.println("  Vertices: " + count);

        GraphBatch importer = GraphBatch.builder(db)
            .withBatchSize(100_000).withLightEdges(false).withWAL(false).build();
        int edgeCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(edgeFile), 1 << 20)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                RID srcRid = vidToRid.get(Long.parseLong(parts[0]));
                RID dstRid = vidToRid.get(Long.parseLong(parts[1]));
                if (srcRid != null && dstRid != null)
                    importer.newEdge(srcRid, "EDGE", dstRid, "WEIGHT", Double.parseDouble(parts[2]));
                edgeCount++;
            }
        }
        importer.close();
        System.out.println("  Edges: " + edgeCount);
        db.close();
        System.out.println("  Database ready at: " + dbPath);
    }
}
