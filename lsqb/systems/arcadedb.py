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
              "arcadedata/arcadedb:latest")
        return {"error": str(e)}

    def sql(cmd, timeout=600):
        return requests.post(f"{base}/api/v1/command/{db}", auth=auth,
            json={"language": "sql", "command": cmd}, timeout=timeout)

    def cypher(cmd, timeout=600):
        return requests.post(f"{base}/api/v1/command/{db}", auth=auth,
            json={"language": "cypher", "command": cmd}, timeout=timeout)

    def sql_batch(cmds, timeout=600):
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

        # Helper to load CSV rows
        d = lambda f: os.path.join(data_dir, f)
        BATCH = 1000

        def load_vertices(vtype, csvfile, extra_props=None):
            """Load vertices from CSV. Sets cid from 'id' column + optional extra props."""
            with open(csvfile) as f:
                reader = csv.DictReader(f, delimiter='|')
                batch = []
                for row in reader:
                    parts = [f"cid = {row['id']}"]
                    if extra_props:
                        for csv_col, prop in extra_props.items():
                            v = row.get(csv_col, "")
                            if v != "":
                                parts.append(f"{prop} = {v}")
                    batch.append(f"INSERT INTO {vtype} SET {', '.join(parts)};")
                    if len(batch) >= BATCH:
                        sql_batch(batch)
                        batch = []
                if batch:
                    sql_batch(batch)

        def load_edges(etype, csvfile, from_type, from_col, to_type, to_col):
            with open(csvfile) as f:
                reader = csv.DictReader(f, delimiter='|')
                batch = []
                for row in reader:
                    src, dst = row[from_col], row[to_col]
                    if src == "" or dst == "":
                        continue
                    batch.append(
                        f"CREATE EDGE {etype} FROM "
                        f"(SELECT FROM {from_type} WHERE cid = {src}) TO "
                        f"(SELECT FROM {to_type} WHERE cid = {dst});")
                    if len(batch) >= BATCH:
                        sql_batch(batch)
                        batch = []
                if batch:
                    sql_batch(batch)

        # Load vertices
        print("  Loading vertices...")
        load_vertices("Country", d("Country.csv"))
        load_vertices("City", d("City.csv"))
        load_vertices("TagClass", d("TagClass.csv"))
        load_vertices("Tag", d("Tag.csv"))
        load_vertices("Person", d("Person.csv"))
        load_vertices("Forum", d("Forum.csv"))
        load_vertices("Post", d("Post.csv"))
        load_vertices("Comment", d("Comment.csv"))

        # Load edges from FK columns
        print("  Loading edges from FKs...")
        load_edges("IS_PART_OF", d("City.csv"), "City", "id", "Country", "ispartof_country")
        load_edges("IS_LOCATED_IN", d("Person.csv"), "Person", "id", "City", "islocatedin_city")
        load_edges("HAS_TYPE", d("Tag.csv"), "Tag", "id", "TagClass", "hastype_tagclass")
        load_edges("HAS_CREATOR", d("Post.csv"), "Post", "id", "Person", "hascreator_person")
        load_edges("CONTAINER_OF", d("Post.csv"), "Forum", "forum_containerof", "Post", "id")
        load_edges("HAS_CREATOR", d("Comment.csv"), "Comment", "id", "Person", "hascreator_person")
        load_edges("REPLY_OF", d("Comment.csv"), "Comment", "id", "Post", "replyof_post")
        load_edges("REPLY_OF", d("Comment.csv"), "Comment", "id", "Comment", "replyof_comment")

        # Load edges from edge tables
        print("  Loading edge tables...")
        load_edges("HAS_MEMBER", d("Forum_hasMember_Person.csv"), "Forum", "id", "Person", "hasmember_person")
        load_edges("HAS_TAG", d("Comment_hasTag_Tag.csv"), "Comment", "id", "Tag", "hastag_tag")
        load_edges("HAS_TAG", d("Post_hasTag_Tag.csv"), "Post", "id", "Tag", "hastag_tag")
        load_edges("KNOWS", d("Person_knows_Person.csv"), "Person", "person1id", "Person", "person2id")
        load_edges("LIKES", d("Person_likes_Comment.csv"), "Person", "id", "Comment", "likes_comment")
        load_edges("LIKES", d("Person_likes_Post.csv"), "Person", "id", "Post", "likes_post")
        load_edges("HAS_INTEREST", d("Person_hasInterest_Tag.csv"), "Person", "id", "Tag", "hasinterest_tag")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Build GAV for OLAP acceleration
    print("\n[ArcadeDB] Building Graph Analytical View...")
    gav_start = time.perf_counter()
    sql("CREATE GRAPH ANALYTICAL VIEW lsqb "
        "VERTEX TYPES (Country, City, TagClass, Tag, Person, Forum, Post, Comment) "
        "EDGE TYPES (IS_PART_OF, IS_LOCATED_IN, HAS_MEMBER, CONTAINER_OF, "
        "REPLY_OF, HAS_TAG, HAS_TYPE, HAS_CREATOR, KNOWS, LIKES, HAS_INTEREST)")
    # Wait for GAV to be ready
    import requests as _req
    for _ in range(600):
        time.sleep(1)
        try:
            r_gav = sql("SELECT status FROM schema:graphAnalyticalViews WHERE name = 'lsqb'")
            if r_gav.status_code == 200:
                status = r_gav.json().get("result", [{}])[0].get("status", "")
                if status == "READY" or status == "ready":
                    break
        except Exception:
            pass
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
