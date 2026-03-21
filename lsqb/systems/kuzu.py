"""Kuzu LSQB benchmark module."""

import time
import os
import shutil

from . import _common
from ._common import data_dir_projected, bench_common


def run_benchmark():
    import kuzu
    print("\n" + "=" * 70)
    print("KUZU LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    db_path = "/tmp/kuzu_lsqb"
    data_dir = data_dir_projected()
    # Files are flat in the data directory (no subdirs).

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        print(f"  Download SF{_common.SF} from https://datasets.ldbcouncil.org/lsqb/")
        return {"error": "Dataset not found"}

    needs_load = True
    if not bench_common.RESET and os.path.isdir(db_path):
        try:
            db = kuzu.Database(db_path)
            conn = kuzu.Connection(db)
            r = conn.execute("MATCH (p:Person) RETURN count(p) AS c")
            row = r.get_next()
            if row and row[0] > 0:
                needs_load = False
                print(f"\n[Kuzu] Data already loaded ({row[0]} persons), skipping import")
        except Exception:
            # DB exists but is corrupt or incomplete — will be rebuilt
            try:
                del conn, db
            except Exception:
                pass

    if needs_load:
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)

        print("\n[Kuzu] Loading LSQB data...")
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

    # Kuzu uses separate relationship types, so we need adapted queries.
    # Queries involving :Message or undirected KNOWS need adjustment.
    kuzu_queries = {
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
    # Q4, Q5, Q7, Q8 require :Message (Post + Comment union) — skip for Kuzu

    for qid in [f"q{i}" for i in range(1, 10)]:
        query = kuzu_queries.get(qid)
        if query is None:
            print(f"\n[Kuzu] {qid.upper()}: skipped (requires :Message union type)")
            results[qid] = "N/A"
            continue

        print(f"\n[Kuzu] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            r = conn.execute(query)
            row = r.get_next()
            count = row[0]
            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    return results
