"""ArcadeDB LSQB benchmark module."""

import time
import os

from ._common import data_dir_merged, CYPHER_QUERIES, bench_common


def run_benchmark():
    import requests
    import csv
    print("\n" + "=" * 70)
    print("ARCADEDB LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    auth = ('root', 'benchmark')
    base = "http://localhost:2480"
    db = "lsqb"
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        return {"error": "Dataset not found"}

    # Check connectivity
    try:
        r = requests.get(f"{base}/api/v1/server", auth=auth, timeout=5)
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}")
    except Exception as e:
        print(f"  Cannot connect to ArcadeDB: {e}")
        print("  Start with: docker run -d --name arcadedb-lsqb -p 2480:2480 "
              '-e JAVA_OPTS="-Darcadedb.server.rootPassword=benchmark" '
              '-e ARCADEDB_OPTS_MEMORY="-Xms12g -Xmx12g" '
              "arcadedata/arcadedb:latest")
        return {"error": str(e)}

    def sql(cmd, timeout=bench_common.QUERY_TIMEOUT):
        return requests.post(f"{base}/api/v1/command/{db}", auth=auth,
            json={"language": "sql", "command": cmd}, timeout=timeout)

    def cypher(cmd, timeout=bench_common.QUERY_TIMEOUT):
        return requests.post(f"{base}/api/v1/command/{db}", auth=auth,
            json={"language": "opencypher", "command": cmd}, timeout=timeout)

    def sql_batch(cmds, timeout=bench_common.QUERY_TIMEOUT):
        return requests.post(f"{base}/api/v1/command/{db}", auth=auth,
            json={"language": "sqlscript",
                  "command": "BEGIN;\n" + "\n".join(cmds) + "\nCOMMIT;"},
            timeout=timeout)

    # Check if data loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r2 = sql("SELECT count(*) FROM Comment")
            if r2.status_code == 200 and r2.json()["result"][0]["count(*)"] > 0:
                needs_load = False
                print("\n[ArcadeDB] Data already loaded, skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[ArcadeDB] Loading LSQB data...")
        start = time.perf_counter()

        # Drop and recreate database
        try:
            requests.post(f"{base}/api/v1/server", auth=auth,
                          json={"command": f"drop database {db}"})
        except Exception:
            pass
        requests.post(f"{base}/api/v1/server", auth=auth,
                      json={"command": f"create database {db}"})

        # Schema — Message supertype with Post and Comment extending it
        print("  Creating schema...")
        sql("CREATE VERTEX TYPE Country")
        sql("CREATE VERTEX TYPE City")
        sql("CREATE VERTEX TYPE TagClass")
        sql("CREATE VERTEX TYPE Tag")
        sql("CREATE VERTEX TYPE Person")
        sql("CREATE VERTEX TYPE Forum")
        sql("CREATE VERTEX TYPE Message")
        sql("CREATE VERTEX TYPE Post EXTENDS Message")
        sql("CREATE VERTEX TYPE Comment EXTENDS Message")

        # ID property on all types
        for t in ["Country", "City", "TagClass", "Tag", "Person", "Forum", "Message"]:
            sql(f"CREATE PROPERTY {t}.cid IF NOT EXISTS LONG")
            sql(f"CREATE INDEX ON {t}(cid) UNIQUE")

        # Edge types
        for e in ["IS_PART_OF", "IS_LOCATED_IN", "HAS_MEMBER", "CONTAINER_OF",
                   "REPLY_OF", "HAS_TAG", "HAS_TYPE", "HAS_CREATOR", "KNOWS",
                   "LIKES", "HAS_INTEREST"]:
            sql(f"CREATE EDGE TYPE {e}")

        # --- Bulk load using /api/v1/batch (JSONL + GraphBatch) ---
        # Stream all vertices first, then all edges, in a single HTTP request.
        # This uses ArcadeDB's high-performance GraphBatch API on the server side.
        import json as jsonlib
        import io

        d = lambda f: os.path.join(data_dir, f)

        def csv_rows(csvfile):
            with open(csvfile) as f:
                yield from csv.DictReader(f, delimiter='|')

        # Build JSONL payload: vertices first (with temp IDs "Type:cid"), then edges
        print("  Building JSONL payload...")
        buf = io.StringIO()
        v_count = 0

        # Vertex types and their CSV files
        vertex_files = [
            ("Country", "Country.csv"),
            ("City", "City.csv"),
            ("TagClass", "TagClass.csv"),
            ("Tag", "Tag.csv"),
            ("Person", "Person.csv"),
            ("Forum", "Forum.csv"),
            ("Post", "Post.csv"),
            ("Comment", "Comment.csv"),
        ]
        for vtype, csvfile in vertex_files:
            for row in csv_rows(d(csvfile)):
                rec = {"@type": "vertex", "@class": vtype,
                       "@id": f"{vtype}:{row['id']}", "cid": int(row['id'])}
                buf.write(jsonlib.dumps(rec) + "\n")
                v_count += 1

        # Edge definitions: (type, csv_file, from_type, from_col, to_type, to_col)
        edge_defs = [
            # FK-based edges (from vertex CSV files)
            ("IS_PART_OF", "City.csv", "City", "id", "Country", "ispartof_country"),
            ("IS_LOCATED_IN", "Person.csv", "Person", "id", "City", "islocatedin_city"),
            ("HAS_TYPE", "Tag.csv", "Tag", "id", "TagClass", "hastype_tagclass"),
            ("HAS_CREATOR", "Post.csv", "Post", "id", "Person", "hascreator_person"),
            ("CONTAINER_OF", "Post.csv", "Forum", "forum_containerof", "Post", "id"),
            ("HAS_CREATOR", "Comment.csv", "Comment", "id", "Person", "hascreator_person"),
            ("REPLY_OF", "Comment.csv", "Comment", "id", "Post", "replyof_post"),
            ("REPLY_OF", "Comment.csv", "Comment", "id", "Comment", "replyof_comment"),
            # Separate edge tables
            ("HAS_MEMBER", "Forum_hasMember_Person.csv", "Forum", "id", "Person", "hasmember_person"),
            ("HAS_TAG", "Comment_hasTag_Tag.csv", "Comment", "id", "Tag", "hastag_tag"),
            ("HAS_TAG", "Post_hasTag_Tag.csv", "Post", "id", "Tag", "hastag_tag"),
            ("KNOWS", "Person_knows_Person.csv", "Person", "person1id", "Person", "person2id"),
            ("LIKES", "Person_likes_Comment.csv", "Person", "id", "Comment", "likes_comment"),
            ("LIKES", "Person_likes_Post.csv", "Person", "id", "Post", "likes_post"),
            ("HAS_INTEREST", "Person_hasInterest_Tag.csv", "Person", "id", "Tag", "hasinterest_tag"),
        ]
        e_count = 0
        for etype, csvfile, from_type, from_col, to_type, to_col in edge_defs:
            for row in csv_rows(d(csvfile)):
                src, dst = row.get(from_col, ""), row.get(to_col, "")
                if src == "" or dst == "":
                    continue
                rec = {"@type": "edge", "@class": etype,
                       "@from": f"{from_type}:{src}", "@to": f"{to_type}:{dst}"}
                buf.write(jsonlib.dumps(rec) + "\n")
                e_count += 1

        print(f"  Streaming {v_count} vertices + {e_count} edges via /batch...")

        # Stream as chunked transfer to avoid loading entire payload in memory
        # and to avoid hitting OS socket buffer limits
        payload_str = buf.getvalue()
        buf.close()

        def chunk_generator(data, chunk_size=4 * 1024 * 1024):
            encoded = data.encode("utf-8")
            for i in range(0, len(encoded), chunk_size):
                yield encoded[i:i + chunk_size]

        r = requests.post(
            f"{base}/api/v1/batch/{db}?wal=false&lightEdges=false",
            auth=auth,
            data=chunk_generator(payload_str),
            headers={"Content-Type": "application/x-ndjson",
                     "Transfer-Encoding": "chunked"},
            timeout=600)
        if r.status_code != 200:
            print(f"  Batch load failed: {r.text[:500]}")
            return {"error": "Batch load failed"}
        batch_result = r.json()
        print(f"  Batch result: {batch_result.get('verticesCreated', '?')} vertices, "
              f"{batch_result.get('edgesCreated', '?')} edges, "
              f"{batch_result.get('elapsedMs', '?')}ms")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Build GAV for OLAP acceleration
    print("\n[ArcadeDB] Building Graph Analytical View...")
    gav_start = time.perf_counter()

    # Drop any existing GAV first
    try:
        sql("DROP GRAPH ANALYTICAL VIEW lsqb")
    except Exception:
        pass

    # Create the GAV definition
    sql("CREATE GRAPH ANALYTICAL VIEW lsqb "
        "VERTEX TYPES (Country, City, TagClass, Tag, Person, Forum, Post, Comment) "
        "EDGE TYPES (IS_PART_OF, IS_LOCATED_IN, HAS_MEMBER, CONTAINER_OF, "
        "REPLY_OF, HAS_TAG, HAS_TYPE, HAS_CREATOR, KNOWS, LIKES, HAS_INTEREST)")

    # REBUILD is synchronous and ensures the CSR is built AND traversal providers
    # are registered with the query optimizer (equivalent to awaitAll() in embedded mode)
    r_rebuild = sql("REBUILD GRAPH ANALYTICAL VIEW lsqb", timeout=300)
    if r_rebuild.status_code != 200:
        print(f"  GAV rebuild failed: {r_rebuild.text[:300]}")
    else:
        print("  GAV rebuild OK")

    # Double-check status
    for _ in range(60):
        try:
            r_gav = sql("SELECT status FROM schema:graphAnalyticalViews WHERE name = 'lsqb'")
            if r_gav.status_code == 200:
                result = r_gav.json().get("result", [])
                if result and len(result) > 0:
                    status = result[0].get("status", "")
                    if status.upper() == "READY":
                        break
        except Exception:
            pass
        time.sleep(1)

    gav_time = time.perf_counter() - gav_start
    print(f"  GAV ready: {gav_time:.2f}s")

    # Run LSQB queries using Cypher.
    # ArcadeDB supports openCypher and Post/Comment both extend Message,
    # so :Message label matches both.
    for qid in [f"q{i}" for i in range(1, 10)]:
        query = CYPHER_QUERIES[qid]
        print(f"\n[ArcadeDB] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            r = cypher(query.strip())
            elapsed = time.perf_counter() - start
            if r.status_code == 200:
                data = r.json()
                count = data["result"][0]["count"]
                results[qid] = elapsed
                print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
            else:
                print(f"  {qid.upper()} failed ({elapsed:.2f}s): {r.text[:300]}")
                results[qid] = "N/A"
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    bench_common.cleanup_docker("arcadedb-lsqb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("arcadedb-lsqb")
