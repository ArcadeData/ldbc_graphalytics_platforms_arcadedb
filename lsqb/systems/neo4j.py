"""Neo4j LSQB benchmark module."""

import time
import os

from ._common import data_dir_merged, CYPHER_QUERIES, bench_common


def run_benchmark():
    from neo4j import GraphDatabase
    print("\n" + "=" * 70)
    print("NEO4J LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    data_dir = data_dir_merged()
    # Files are flat in the data directory (no subdirs).

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        return {"error": "Dataset not found"}

    try:
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "benchmark123"))
        driver.verify_connectivity()
    except Exception as e:
        print(f"  Cannot connect to Neo4j: {e}")
        print("  Start with: docker run -d --name neo4j-lsqb -p 7474:7474 -p 7688:7687 "
              "-e NEO4J_AUTH=neo4j/benchmark123 neo4j:2026-community")
        return {"error": str(e)}

    needs_load = True
    if not bench_common.RESET:
        try:
            with driver.session() as session:
                r = session.run("MATCH (p:Person) RETURN count(p) AS c").single()
                if r and r["c"] > 0:
                    needs_load = False
                    print(f"\n[Neo4j] Data already loaded ({r['c']} persons), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[Neo4j] Loading LSQB data...")
        start = time.perf_counter()

        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        # Load CSVs via LOAD CSV (files must be accessible — use file:/// for local)
        # Neo4j needs files in its import directory or allow file URLs.
        # For simplicity, we load via UNWIND batches from Python.
        import csv

        d = lambda f: os.path.join(data_dir, f)

        def load_csv(path, delim='|'):
            with open(path) as f:
                reader = csv.DictReader(f, delimiter=delim)
                return list(reader)

        def batch_run(session, query, data, batch_size=10000):
            for i in range(0, len(data), batch_size):
                session.run(query, rows=data[i:i+batch_size])

        with driver.session() as s:
            # Create indexes first
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Country) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:City) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:TagClass) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Tag) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Forum) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Post) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Comment) ON (n.id)")
            s.run("CREATE INDEX IF NOT EXISTS FOR (n:Message) ON (n.id)")

            # Nodes (merged-fk uses lowercase: id, ispartof_country, etc.)
            print("  Loading nodes...")
            batch_run(s, "UNWIND $rows AS r CREATE (:Country {id: toInteger(r.id)})",
                      load_csv(d('Country.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:City {id: toInteger(r.id)})",
                      load_csv(d('City.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:TagClass {id: toInteger(r.id)})",
                      load_csv(d('TagClass.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Tag {id: toInteger(r.id)})",
                      load_csv(d('Tag.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Person {id: toInteger(r.id)})",
                      load_csv(d('Person.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Forum {id: toInteger(r.id)})",
                      load_csv(d('Forum.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Post:Message {id: toInteger(r.id)})",
                      load_csv(d('Post.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Comment:Message {id: toInteger(r.id)})",
                      load_csv(d('Comment.csv')))

            # Edges from FK columns in entity tables
            print("  Loading edges from FKs...")
            batch_run(s, "UNWIND $rows AS r MATCH (a:City {id: toInteger(r.id)}), (b:Country {id: toInteger(r.ispartof_country)}) CREATE (a)-[:IS_PART_OF]->(b)",
                      load_csv(d('City.csv')))
            # IS_LOCATED_IN: Person -> City
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:City {id: toInteger(r.islocatedin_city)}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
                      load_csv(d('Person.csv')))
            # HAS_TYPE: Tag -> TagClass
            batch_run(s, "UNWIND $rows AS r MATCH (a:Tag {id: toInteger(r.id)}), (b:TagClass {id: toInteger(r.hastype_tagclass)}) CREATE (a)-[:HAS_TYPE]->(b)",
                      load_csv(d('Tag.csv')))
            # HAS_CREATOR: Post -> Person, CONTAINER_OF: Forum -> Post
            batch_run(s, "UNWIND $rows AS r MATCH (a:Post {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                      load_csv(d('Post.csv')))
            # CONTAINER_OF: Forum -> Post (from Post.forum_containerof)
            batch_run(s, "UNWIND $rows AS r MATCH (f:Forum {id: toInteger(r.forum_containerof)}), (p:Post {id: toInteger(r.id)}) CREATE (f)-[:CONTAINER_OF]->(p)",
                      load_csv(d('Post.csv')))
            # HAS_CREATOR: Comment -> Person, REPLY_OF: Comment -> Post
            rows_comment = load_csv(d('Comment.csv'))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                      rows_comment)
            batch_run(s, "UNWIND $rows AS r WITH r WHERE r.replyof_post <> '' MATCH (a:Comment {id: toInteger(r.id)}), (b:Post {id: toInteger(r.replyof_post)}) CREATE (a)-[:REPLY_OF]->(b)",
                      rows_comment)
            batch_run(s, "UNWIND $rows AS r WITH r WHERE r.replyof_comment <> '' MATCH (a:Comment {id: toInteger(r.id)}), (b:Comment {id: toInteger(r.replyof_comment)}) CREATE (a)-[:REPLY_OF]->(b)",
                      rows_comment)

            # Edges from edge tables
            print("  Loading edge tables...")
            batch_run(s, "UNWIND $rows AS r MATCH (a:Forum {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hasmember_person)}) CREATE (a)-[:HAS_MEMBER]->(b)",
                      load_csv(d('Forum_hasMember_Person.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hastag_tag)}) CREATE (a)-[:HAS_TAG]->(b)",
                      load_csv(d('Comment_hasTag_Tag.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Post {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hastag_tag)}) CREATE (a)-[:HAS_TAG]->(b)",
                      load_csv(d('Post_hasTag_Tag.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.person1id)}), (b:Person {id: toInteger(r.person2id)}) CREATE (a)-[:KNOWS]->(b)",
                      load_csv(d('Person_knows_Person.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Comment {id: toInteger(r.likes_comment)}) CREATE (a)-[:LIKES]->(b)",
                      load_csv(d('Person_likes_Comment.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Post {id: toInteger(r.likes_post)}) CREATE (a)-[:LIKES]->(b)",
                      load_csv(d('Person_likes_Post.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:Tag {id: toInteger(r.hasinterest_tag)}) CREATE (a)-[:HAS_INTEREST]->(b)",
                      load_csv(d('Person_hasInterest_Tag.csv')))

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Run queries
    for qid in [f"q{i}" for i in range(1, 10)]:
        query = CYPHER_QUERIES[qid]
        print(f"\n[Neo4j] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            with driver.session() as session:
                r = session.run(query).single()
                count = r["count"]
            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    driver.close()
    bench_common.cleanup_docker("neo4j-lsqb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("neo4j-lsqb")
