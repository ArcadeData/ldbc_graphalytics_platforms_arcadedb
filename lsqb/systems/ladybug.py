"""LadybugDB LSQB benchmark module."""

import time
import os
import shutil

from . import _common
from ._common import data_dir_projected, bench_common


def run_benchmark():
    import ladybug as kuzu
    print("\n" + "=" * 70)
    print("LADYBUGDB LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    db_path = bench_common.embedded_db_path("lsqb", "ladybug", "db")
    data_dir = data_dir_projected()
    # Files are flat in the data directory (no subdirs).

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        print(f"  Download SF{_common.SF} from https://datasets.ldbcouncil.org/lsqb/")
        return {"error": "Dataset not found"}

    needs_load = True
    if not bench_common.RESET and os.path.exists(db_path):
        try:
            db = kuzu.Database(db_path)
            conn = kuzu.Connection(db)
            r = conn.execute("MATCH (p:Person) RETURN count(p) AS c")
            row = r.get_next()
            r2 = conn.execute("MATCH ()-[x:REPLY_OF_C]->() RETURN count(x) AS c")  # absent in databases from before Q4/Q5/Q7/Q8
            row2 = r2.get_next()
            if row and row[0] > 0 and row2 and row2[0] > 0:
                needs_load = False
                print(f"\n[LadybugDB] Data already loaded ({row[0]} persons), skipping import")
        except Exception:
            # DB exists but is corrupt or incomplete — will be rebuilt
            try:
                del conn, db
            except Exception:
                pass

    if needs_load:
        if os.path.exists(db_path):
            bench_common.remove_db(db_path)
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)

        print("\n[LadybugDB] Loading LSQB data...")
        start = time.perf_counter()

        # Projected-fk CSVs: entity files have 1 column (id), edge files have 2 columns.
        # Headers use Neo4j format (id:ID(Type), :START_ID(X)|:END_ID(Y)) — skip them.

        # Node tables (single ID column each)
        conn.execute("CREATE NODE TABLE Country(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE City(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE TagClass(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Tag(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Forum(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Post(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Comment(id INT64, PRIMARY KEY(id))")

        # Relationship tables
        conn.execute("CREATE REL TABLE IS_PART_OF(FROM City TO Country)")
        conn.execute("CREATE REL TABLE IS_LOCATED_IN(FROM Person TO City)")
        conn.execute("CREATE REL TABLE HAS_MEMBER(FROM Forum TO Person)")
        conn.execute("CREATE REL TABLE CONTAINER_OF(FROM Forum TO Post)")
        conn.execute("CREATE REL TABLE REPLY_OF(FROM Comment TO Post)")
        conn.execute("CREATE REL TABLE REPLY_OF_C(FROM Comment TO Comment)")
        conn.execute("CREATE REL TABLE HAS_TAG_C(FROM Comment TO Tag)")
        conn.execute("CREATE REL TABLE HAS_TAG_P(FROM Post TO Tag)")
        conn.execute("CREATE REL TABLE HAS_TYPE(FROM Tag TO TagClass)")
        conn.execute("CREATE REL TABLE HAS_CREATOR_C(FROM Comment TO Person)")
        conn.execute("CREATE REL TABLE HAS_CREATOR_P(FROM Post TO Person)")
        conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)")
        conn.execute("CREATE REL TABLE LIKES_C(FROM Person TO Comment)")
        conn.execute("CREATE REL TABLE LIKES_P(FROM Person TO Post)")
        conn.execute("CREATE REL TABLE HAS_INTEREST(FROM Person TO Tag)")

        # Load data — skip Neo4j-format headers
        d = lambda f: os.path.join(data_dir, f)
        for tbl, f in [("Country", "Country.csv"), ("City", "City.csv"),
                        ("TagClass", "TagClass.csv"), ("Tag", "Tag.csv"),
                        ("Person", "Person.csv"), ("Forum", "Forum.csv"),
                        ("Post", "Post.csv"), ("Comment", "Comment.csv")]:
            conn.execute(f"COPY {tbl} FROM '{d(f)}' (HEADER=true, DELIM='|')")

        for rel, f in [("IS_PART_OF", "City_isPartOf_Country.csv"),
                        ("IS_LOCATED_IN", "Person_isLocatedIn_City.csv"),
                        ("HAS_MEMBER", "Forum_hasMember_Person.csv"),
                        ("CONTAINER_OF", "Forum_containerOf_Post.csv"),
                        ("REPLY_OF", "Comment_replyOf_Post.csv"),
                        ("REPLY_OF_C", "Comment_replyOf_Comment.csv"),
                        ("HAS_TAG_C", "Comment_hasTag_Tag.csv"),
                        ("HAS_TAG_P", "Post_hasTag_Tag.csv"),
                        ("HAS_TYPE", "Tag_hasType_TagClass.csv"),
                        ("HAS_CREATOR_C", "Comment_hasCreator_Person.csv"),
                        ("HAS_CREATOR_P", "Post_hasCreator_Person.csv"),
                        ("KNOWS", "Person_knows_Person.csv"),
                        ("LIKES_C", "Person_likes_Comment.csv"),
                        ("LIKES_P", "Person_likes_Post.csv"),
                        ("HAS_INTEREST", "Person_hasInterest_Tag.csv")]:
            conn.execute(f"COPY {rel} FROM '{d(f)}' (HEADER=true, DELIM='|')")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
    else:
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)

    # LadybugDB uses separate relationship types, so we need adapted queries.
    # Queries involving :Message or undirected KNOWS need adjustment.
    ladybug_queries = {
        "q1": """
MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF]-(:Comment)-[:HAS_TAG_C]->(:Tag)-[:HAS_TYPE]->(:TagClass)
RETURN count(*) AS count
""",
        "q2": """
MATCH (person1:Person)-[:KNOWS]-(person2:Person),
  (person1)<-[:HAS_CREATOR_C]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR_P]->(person2)
RETURN count(*) AS count
""",
        "q3": """
MATCH (country:Country)
MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country)
MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country)
MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country)
MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)
RETURN count(*) AS count
""",
        "q6": """
MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-(person3:Person)-[:HAS_INTEREST]->(tag:Tag)
WHERE person1 <> person3
RETURN count(*) AS count
""",
        "q9": """
MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-(person3:Person)-[:HAS_INTEREST]->(tag:Tag)
WHERE NOT EXISTS { MATCH (person1)-[:KNOWS]-(person3) }
  AND person1 <> person3
RETURN count(*) AS count
""",
    }
    # Q4, Q5, Q7, Q8 range over :Message (Post + Comment), which LadybugDB models as two node tables with separate
    # relationship tables (_P / _C, REPLY_OF for replies to a post, REPLY_OF_C for replies to a comment). Each query is
    # therefore run once per Message kind and the counts are added (all four are plain counts, so the sum is exact);
    # the reported time is the time of both parts.
    msg = {"P": ("Post", "HAS_TAG_P", "HAS_CREATOR_P", "LIKES_P", "REPLY_OF"),
           "C": ("Comment", "HAS_TAG_C", "HAS_CREATOR_C", "LIKES_C", "REPLY_OF_C")}
    ladybug_queries["q4"] = [f"""
MATCH (:Tag)<-[:{tag}]-(m:{label})-[:{creator}]->(:Person), (m)<-[:{likes}]-(:Person), (m)<-[:{reply}]-(:Comment)
RETURN count(*) AS count""" for label, tag, creator, likes, reply in msg.values()]
    ladybug_queries["q5"] = [f"""
MATCH (tag1:Tag)<-[:{tag}]-(m:{label})<-[:{reply}]-(c:Comment)-[:HAS_TAG_C]->(tag2:Tag)
WHERE tag1 <> tag2
RETURN count(*) AS count""" for label, tag, creator, likes, reply in msg.values()]
    ladybug_queries["q7"] = [f"""
MATCH (:Tag)<-[:{tag}]-(m:{label})-[:{creator}]->(:Person)
OPTIONAL MATCH (m)<-[:{likes}]-(:Person)
OPTIONAL MATCH (m)<-[:{reply}]-(:Comment)
RETURN count(*) AS count""" for label, tag, creator, likes, reply in msg.values()]
    ladybug_queries["q8"] = [f"""
MATCH (tag1:Tag)<-[:{tag}]-(m:{label})<-[:{reply}]-(c:Comment)-[:HAS_TAG_C]->(tag2:Tag)
WHERE tag1 <> tag2 AND NOT EXISTS {{ MATCH (c)-[:HAS_TAG_C]->(tag1) }}
RETURN count(*) AS count""" for label, tag, creator, likes, reply in msg.values()]

    # Set 300s query timeout
    conn.set_query_timeout(bench_common.QUERY_TIMEOUT * 1000)

    for qid in [f"q{i}" for i in range(1, 10)]:
        query = ladybug_queries.get(qid)
        if query is None:
            print(f"\n[LadybugDB] {qid.upper()}: skipped (requires :Message union type)")
            results[qid] = "N/A"
            continue

        print(f"\n[LadybugDB] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            def _once(query=query):
                parts = query if isinstance(query, list) else [query]
                return sum(conn.execute(q).get_next()[0] for q in parts)
            elapsed, count = bench_common.measure_repeated(_once, name=qid)
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = bench_common.LAST_CALL_SECONDS
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "timeout" if elapsed >= bench_common.QUERY_TIMEOUT - 1 else "N/A"

    return results
