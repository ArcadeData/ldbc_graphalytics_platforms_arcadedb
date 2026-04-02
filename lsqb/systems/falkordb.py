"""FalkorDB LSQB benchmark module."""

import time
import os
import shutil

from ._common import data_dir_merged, CYPHER_QUERIES, bench_common

FALKORDB_DATA_DIR = "/tmp/falkordb_lsqb"


def run_benchmark():
    import redis
    from falkordb import FalkorDB
    print("\n" + "=" * 70)
    print("FALKORDB LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        return {"error": "Dataset not found"}

    if bench_common.RESET and os.path.isdir(FALKORDB_DATA_DIR):
        print("  [FalkorDB] --reset: removing persisted data...")
        shutil.rmtree(FALKORDB_DATA_DIR)

    os.makedirs(FALKORDB_DATA_DIR, exist_ok=True)

    try:
        fdb = FalkorDB(host='localhost', port=6379)
        g = fdb.select_graph('lsqb')
    except Exception as e:
        print(f"  Cannot connect to FalkorDB: {e}")
        print(f"  Start with: docker run -d --name falkordb-lsqb -p 6379:6379 "
              f"-v {FALKORDB_DATA_DIR}:/var/lib/falkordb/data falkordb/falkordb:latest")
        return {"error": str(e)}

    # Disable query timeout (default 1000ms is too low for complex queries)
    try:
        rc = redis.Redis(host='localhost', port=6379)
        rc.execute_command("GRAPH.CONFIG", "SET", "TIMEOUT", 0)
        print("  Query timeout disabled")
    except Exception:
        pass

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = g.ro_query("MATCH (p:Person) RETURN count(p) AS c")
            if r.result_set and r.result_set[0][0] > 0:
                needs_load = False
                print(f"\n[FalkorDB] Data already loaded ({r.result_set[0][0]} persons), skipping import")
        except Exception:
            pass

    if bench_common.RESET:
        try:
            g.delete()
            g = fdb.select_graph('lsqb')
        except Exception:
            pass

    if needs_load:
        print("\n[FalkorDB] Loading LSQB data...")
        start = time.perf_counter()

        import csv
        d = lambda f: os.path.join(data_dir, f)

        def load_csv(path, delim='|'):
            with open(path) as f:
                reader = csv.DictReader(f, delimiter=delim)
                return list(reader)

        def batch_query(query, data, batch_size=5000):
            for i in range(0, len(data), batch_size):
                g.query(query, {"rows": data[i:i+batch_size]})

        # Create indexes
        for label in ["Country", "City", "TagClass", "Tag", "Person", "Forum", "Post", "Comment", "Message"]:
            try:
                g.query(f"CREATE INDEX FOR (n:{label}) ON (n.id)")
            except Exception:
                pass

        # Nodes
        print("  Loading nodes...")
        batch_query("UNWIND $rows AS r CREATE (:Country {id: toInteger(r.id)})", load_csv(d('Country.csv')))
        batch_query("UNWIND $rows AS r CREATE (:City {id: toInteger(r.id)})", load_csv(d('City.csv')))
        batch_query("UNWIND $rows AS r CREATE (:TagClass {id: toInteger(r.id)})", load_csv(d('TagClass.csv')))
        batch_query("UNWIND $rows AS r CREATE (:Tag {id: toInteger(r.id)})", load_csv(d('Tag.csv')))
        batch_query("UNWIND $rows AS r CREATE (:Person {id: toInteger(r.id)})", load_csv(d('Person.csv')))
        batch_query("UNWIND $rows AS r CREATE (:Forum {id: toInteger(r.id)})", load_csv(d('Forum.csv')))
        batch_query("UNWIND $rows AS r CREATE (:Post:Message {id: toInteger(r.id)})", load_csv(d('Post.csv')))
        batch_query("UNWIND $rows AS r CREATE (:Comment:Message {id: toInteger(r.id)})", load_csv(d('Comment.csv')))

        # Edges from FK columns
        print("  Loading edges from FKs...")
        batch_query("UNWIND $rows AS r MATCH (a:City {id: toInteger(r.id)}), (b:Country {id: toInteger(r.ispartof_country)}) CREATE (a)-[:IS_PART_OF]->(b)",
                     load_csv(d('City.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:City {id: toInteger(r.islocatedin_city)}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
                     load_csv(d('Person.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Tag {id: toInteger(r.id)}), (b:TagClass {id: toInteger(r.hastype_tagclass)}) CREATE (a)-[:HAS_TYPE]->(b)",
                     load_csv(d('Tag.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Post {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                     load_csv(d('Post.csv')))
        batch_query("UNWIND $rows AS r MATCH (f:Forum {id: toInteger(r.forum_containerof)}), (p:Post {id: toInteger(r.id)}) CREATE (f)-[:CONTAINER_OF]->(p)",
                     load_csv(d('Post.csv')))
        rows_comment = load_csv(d('Comment.csv'))
        batch_query("UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                     rows_comment)
        # FalkorDB doesn't support WITH r WHERE in the same way — filter in Python
        rows_reply_post = [r for r in rows_comment if r.get('replyof_post', '') != '']
        batch_query("UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Post {id: toInteger(r.replyof_post)}) CREATE (a)-[:REPLY_OF]->(b)",
                     rows_reply_post)
        rows_reply_comment = [r for r in rows_comment if r.get('replyof_comment', '') != '']
        batch_query("UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Comment {id: toInteger(r.replyof_comment)}) CREATE (a)-[:REPLY_OF]->(b)",
                     rows_reply_comment)

        # Edge tables
        print("  Loading edge tables...")
        batch_query("UNWIND $rows AS r MATCH (a:Forum {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hasmember_person)}) CREATE (a)-[:HAS_MEMBER]->(b)",
                     load_csv(d('Forum_hasMember_Person.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hastag_tag)}) CREATE (a)-[:HAS_TAG]->(b)",
                     load_csv(d('Comment_hasTag_Tag.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Post {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hastag_tag)}) CREATE (a)-[:HAS_TAG]->(b)",
                     load_csv(d('Post_hasTag_Tag.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.person1id)}), (b:Person {id: toInteger(r.person2id)}) CREATE (a)-[:KNOWS]->(b)",
                     load_csv(d('Person_knows_Person.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Comment {id: toInteger(r.likes_comment)}) CREATE (a)-[:LIKES]->(b)",
                     load_csv(d('Person_likes_Comment.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Post {id: toInteger(r.likes_post)}) CREATE (a)-[:LIKES]->(b)",
                     load_csv(d('Person_likes_Post.csv')))
        batch_query("UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hasinterest_tag)}) CREATE (a)-[:HAS_INTEREST]->(b)",
                     load_csv(d('Person_hasInterest_Tag.csv')))

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Run queries (300s timeout per query)
    for qid in [f"q{i}" for i in range(1, 10)]:
        query = CYPHER_QUERIES[qid]
        print(f"\n[FalkorDB] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            r = g.ro_query(query)
            count = r.result_set[0][0]
            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "timeout" if elapsed >= bench_common.QUERY_TIMEOUT - 1 else "N/A"

    bench_common.cleanup_docker("falkordb-lsqb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("falkordb-lsqb")
