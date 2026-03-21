#!/usr/bin/env python3
"""
LSQB (Labelled Subgraph Query Benchmark): Kuzu vs Neo4j vs DuckDB vs SurrealDB vs Dgraph
Dataset: LDBC SNB social-network-sf1 (or configurable via --sf)
Queries: 9 subgraph pattern matching queries (Q1-Q9)

Setup:
  # Download dataset (SF1, ~50MB)
  curl -L -o ../datasets/lsqb-sf1-projected.tar.zst \\
    https://datasets.ldbcouncil.org/lsqb/social-network-sf1-projected-fk.tar.zst
  curl -L -o ../datasets/lsqb-sf1-merged.tar.zst \\
    https://datasets.ldbcouncil.org/lsqb/social-network-sf1-merged-fk.tar.zst
  cd ../datasets && tar --use-compress-program=unzstd -xf lsqb-sf1-projected.tar.zst
  cd ../datasets && tar --use-compress-program=unzstd -xf lsqb-sf1-merged.tar.zst

Usage:
  python3 lsqb_benchmark.py                   # Run all systems
  python3 lsqb_benchmark.py --reset           # Delete all data and reload
  python3 lsqb_benchmark.py kuzu              # Run only Kuzu
  python3 lsqb_benchmark.py surrealdb         # Run only SurrealDB
  python3 lsqb_benchmark.py dgraph            # Run only Dgraph
  python3 lsqb_benchmark.py --sf 3 neo4j      # Use SF3 dataset, Neo4j only
"""

import time
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

import bench_common
from bench_common import GRAPHS_DIR

# Default scale factor (overridden by --sf)
SF = "1"

# Resolved per SF
DATA_DIR_PROJECTED = None   # for graph DBs (Cypher)
DATA_DIR_MERGED = None      # for relational DBs (SQL)


def data_dir_projected():
    return os.path.join(GRAPHS_DIR, f"social-network-sf{SF}-projected-fk")


def data_dir_merged():
    return os.path.join(GRAPHS_DIR, f"social-network-sf{SF}-merged-fk")


# =========================================================================
# CYPHER QUERIES (for Kuzu, Neo4j, Memgraph)
# =========================================================================
CYPHER_QUERIES = {
    "q1": """
MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF]-(:Comment)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass)
RETURN count(*) AS count
""",
    "q2": """
MATCH
  (person1:Person)-[:KNOWS]-(person2:Person),
  (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)
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
    "q4": """
MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person),
  (message)<-[:LIKES]-(liker:Person),
  (message)<-[:REPLY_OF]-(comment:Comment)
RETURN count(*) AS count
""",
    "q5": """
MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)
WHERE tag1 <> tag2
RETURN count(*) AS count
""",
    "q6": """
MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-(person3:Person)-[:HAS_INTEREST]->(tag:Tag)
WHERE person1 <> person3
RETURN count(*) AS count
""",
    "q7": """
MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person)
OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person)
OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Comment)
RETURN count(*) AS count
""",
    "q8": """
MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)
WHERE NOT (comment)-[:HAS_TAG]->(tag1)
  AND tag1 <> tag2
RETURN count(*) AS count
""",
    "q9": """
MATCH (person1:Person)-[:KNOWS]-(person2:Person)-[:KNOWS]-(person3:Person)-[:HAS_INTEREST]->(tag:Tag)
WHERE NOT (person1)-[:KNOWS]-(person3)
  AND person1 <> person3
RETURN count(*) AS count
""",
}


# =========================================================================
# SQL QUERIES (for DuckDB)
# =========================================================================
SQL_QUERIES = {
    "q1": """
SELECT count(*) AS count
FROM Country
JOIN City ON City.isPartOf_CountryId = Country.CountryId
JOIN Person ON Person.isLocatedIn_CityId = City.CityId
JOIN Forum_hasMember_Person ON Forum_hasMember_Person.PersonId = Person.PersonId
JOIN Forum ON Forum.ForumId = Forum_hasMember_Person.ForumId
JOIN Post ON Post.Forum_containerOfId = Forum.ForumId
JOIN Comment ON Comment.replyOf_PostId = Post.PostId
JOIN Comment_hasTag_Tag ON Comment_hasTag_Tag.CommentId = Comment.CommentId
JOIN Tag ON Tag.TagId = Comment_hasTag_Tag.TagId
JOIN TagClass ON Tag.hasType_TagClassId = TagClass.TagClassId
""",
    "q2": """
SELECT count(*) AS count
FROM Person_knows_Person
JOIN Comment ON Person_knows_Person.Person1Id = Comment.hasCreator_PersonId
JOIN Post ON Person_knows_Person.Person2Id = Post.hasCreator_PersonId
  AND Comment.replyOf_PostId = Post.PostId
""",
    "q3": """
SELECT count(*) AS count
FROM City AS CityA
JOIN City AS CityB ON CityB.isPartOf_CountryId = CityA.isPartOf_CountryId
JOIN City AS CityC ON CityC.isPartOf_CountryId = CityA.isPartOf_CountryId
JOIN Person AS PersonA ON PersonA.isLocatedIn_CityId = CityA.CityId
JOIN Person AS PersonB ON PersonB.isLocatedIn_CityId = CityB.CityId
JOIN Person AS PersonC ON PersonC.isLocatedIn_CityId = CityC.CityId
JOIN Person_knows_Person AS pkp1
  ON pkp1.Person1Id = PersonA.PersonId AND pkp1.Person2Id = PersonB.PersonId
JOIN Person_knows_Person AS pkp2
  ON pkp2.Person1Id = PersonB.PersonId AND pkp2.Person2Id = PersonC.PersonId
JOIN Person_knows_Person AS pkp3
  ON pkp3.Person1Id = PersonC.PersonId AND pkp3.Person2Id = PersonA.PersonId
""",
    "q4": """
SELECT count(*) AS count
FROM Message_hasTag_Tag
JOIN Message_hasCreator_Person
  ON Message_hasTag_Tag.MessageId = Message_hasCreator_Person.MessageId
JOIN Comment_replyOf_Message
  ON Comment_replyOf_Message.ParentMessageId = Message_hasTag_Tag.MessageId
JOIN Person_likes_Message
  ON Person_likes_Message.MessageId = Message_hasTag_Tag.MessageId
""",
    "q5": """
SELECT count(*) AS count
FROM Message_hasTag_Tag
JOIN Comment_replyOf_Message
  ON Message_hasTag_Tag.MessageId = Comment_replyOf_Message.ParentMessageId
JOIN Comment_hasTag_Tag AS cht
  ON Comment_replyOf_Message.CommentId = cht.CommentId
WHERE Message_hasTag_Tag.TagId != cht.TagId
""",
    "q6": """
SELECT count(*) AS count
FROM Person_knows_Person pkp1
JOIN Person_knows_Person pkp2
  ON pkp1.Person2Id = pkp2.Person1Id AND pkp1.Person1Id != pkp2.Person2Id
JOIN Person_hasInterest_Tag
  ON Person_hasInterest_Tag.PersonId = pkp2.Person2Id
""",
    "q7": """
SELECT count(*) AS count
FROM Message_hasTag_Tag
JOIN Message_hasCreator_Person
  ON Message_hasTag_Tag.MessageId = Message_hasCreator_Person.MessageId
LEFT JOIN Comment_replyOf_Message
  ON Comment_replyOf_Message.ParentMessageId = Message_hasTag_Tag.MessageId
LEFT JOIN Person_likes_Message
  ON Person_likes_Message.MessageId = Message_hasTag_Tag.MessageId
""",
    "q8": """
SELECT count(*) AS count
FROM Message_hasTag_Tag
JOIN Comment_replyOf_Message
  ON Message_hasTag_Tag.MessageId = Comment_replyOf_Message.ParentMessageId
JOIN Comment_hasTag_Tag AS cht1
  ON Comment_replyOf_Message.CommentId = cht1.CommentId
LEFT JOIN Comment_hasTag_Tag AS cht2
  ON Message_hasTag_Tag.TagId = cht2.TagId
  AND Comment_replyOf_Message.CommentId = cht2.CommentId
WHERE Message_hasTag_Tag.TagId != cht1.TagId AND cht2.TagId IS NULL
""",
    "q9": """
SELECT count(*) AS count
FROM Person_knows_Person pkp1
JOIN Person_knows_Person pkp2
  ON pkp1.Person2Id = pkp2.Person1Id AND pkp1.Person1Id != pkp2.Person2Id
JOIN Person_hasInterest_Tag
  ON pkp2.Person2Id = Person_hasInterest_Tag.PersonId
LEFT JOIN Person_knows_Person pkp3
  ON pkp3.Person1Id = pkp1.Person1Id AND pkp3.Person2Id = pkp2.Person2Id
WHERE pkp3.Person1Id IS NULL
""",
}

LSQB_METRICS = ["load"] + [f"q{i}" for i in range(1, 10)]


# =========================================================================
# KUZU BENCHMARK
# =========================================================================
def run_kuzu_benchmark():
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
        print(f"  Download SF{SF} from https://datasets.ldbcouncil.org/lsqb/")
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


# =========================================================================
# DUCKDB BENCHMARK
# =========================================================================
def run_duckdb_benchmark():
    import duckdb
    print("\n" + "=" * 70)
    print("DUCKDB LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    db_path = "/tmp/duckdb_lsqb.db"
    data_dir = data_dir_merged()
    # Files are flat in the data directory (no subdirs).

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        print(f"  Download SF{SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
        return {"error": "Dataset not found"}

    needs_load = True
    if not bench_common.RESET and os.path.exists(db_path):
        try:
            con = duckdb.connect(db_path)
            con.execute("SELECT count(*) FROM Person")
            needs_load = False
            print(f"\n[DuckDB] Data already loaded, skipping import")
        except Exception:
            pass

    if needs_load:
        if os.path.exists(db_path):
            os.remove(db_path)
        con = duckdb.connect(db_path)

        print("\n[DuckDB] Loading LSQB data...")
        start = time.perf_counter()

        d = lambda f: os.path.join(data_dir, f)

        # Load raw tables with renamed columns to match LSQB SQL schema.
        # Merged-fk CSVs use lowercase names: id, hascreator_person, etc.
        con.execute(f"CREATE TABLE Country AS SELECT id AS CountryId, ispartof_continent AS isPartOf_ContinentId FROM read_csv('{d('Country.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE City AS SELECT id AS CityId, ispartof_country AS isPartOf_CountryId FROM read_csv('{d('City.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE TagClass AS SELECT id AS TagClassId, issubclassof_tagclass AS isSubclassOf_TagClassId FROM read_csv('{d('TagClass.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Tag AS SELECT id AS TagId, hastype_tagclass AS hasType_TagClassId FROM read_csv('{d('Tag.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Forum AS SELECT id AS ForumId, hasmoderator_person AS hasModerator_PersonId FROM read_csv('{d('Forum.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Person AS SELECT id AS PersonId, islocatedin_city AS isLocatedIn_CityId FROM read_csv('{d('Person.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Post AS SELECT id AS PostId, hascreator_person AS hasCreator_PersonId, forum_containerof AS Forum_containerOfId, islocatedin_country AS isLocatedIn_CountryId FROM read_csv('{d('Post.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Comment AS SELECT id AS CommentId, hascreator_person AS hasCreator_PersonId, islocatedin_country AS isLocatedIn_CountryId, replyof_post AS replyOf_PostId, replyof_comment AS replyOf_CommentId FROM read_csv('{d('Comment.csv')}', header=true, delim='|')")

        # Relationship tables (edge CSVs have generic column names)
        con.execute(f"CREATE TABLE Forum_hasMember_Person AS SELECT id AS ForumId, hasmember_person AS PersonId FROM read_csv('{d('Forum_hasMember_Person.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Comment_hasTag_Tag AS SELECT id AS CommentId, hastag_tag AS TagId FROM read_csv('{d('Comment_hasTag_Tag.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Post_hasTag_Tag AS SELECT id AS PostId, hastag_tag AS TagId FROM read_csv('{d('Post_hasTag_Tag.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Person_hasInterest_Tag AS SELECT id AS PersonId, hasinterest_tag AS TagId FROM read_csv('{d('Person_hasInterest_Tag.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Person_likes_Comment AS SELECT id AS PersonId, likes_comment AS CommentId FROM read_csv('{d('Person_likes_Comment.csv')}', header=true, delim='|')")
        con.execute(f"CREATE TABLE Person_likes_Post AS SELECT id AS PersonId, likes_post AS PostId FROM read_csv('{d('Person_likes_Post.csv')}', header=true, delim='|')")

        # Person_knows_Person — load both directions
        con.execute(f"""
            CREATE TABLE Person_knows_Person AS
            SELECT person1id AS Person1Id, person2id AS Person2Id FROM read_csv('{d('Person_knows_Person.csv')}', header=true, delim='|')
            UNION ALL
            SELECT person2id AS Person1Id, person1id AS Person2Id FROM read_csv('{d('Person_knows_Person.csv')}', header=true, delim='|')
        """)

        # Views (Message = Post UNION Comment)
        con.execute("""
            CREATE VIEW Message_hasTag_Tag AS
            SELECT PostId AS MessageId, TagId FROM Post_hasTag_Tag
            UNION ALL
            SELECT CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
        """)
        con.execute("""
            CREATE VIEW Message_hasCreator_Person AS
            SELECT PostId AS MessageId, hasCreator_PersonId AS PersonId FROM Post
            UNION ALL
            SELECT CommentId AS MessageId, hasCreator_PersonId AS PersonId FROM Comment
        """)
        con.execute("""
            CREATE VIEW Comment_replyOf_Message AS
            SELECT CommentId, replyOf_PostId AS ParentMessageId FROM Comment WHERE replyOf_PostId IS NOT NULL
            UNION ALL
            SELECT CommentId, replyOf_CommentId AS ParentMessageId FROM Comment WHERE replyOf_CommentId IS NOT NULL
        """)
        con.execute("""
            CREATE VIEW Person_likes_Message AS
            SELECT PersonId, CommentId AS MessageId FROM Person_likes_Comment
            UNION ALL
            SELECT PersonId, PostId AS MessageId FROM Person_likes_Post
        """)

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")
    else:
        con = duckdb.connect(db_path)

    # Run queries
    for qid in [f"q{i}" for i in range(1, 10)]:
        query = SQL_QUERIES[qid]
        print(f"\n[DuckDB] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            r = con.execute(query).fetchone()
            count = r[0]
            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    return results


# =========================================================================
# NEO4J BENCHMARK
# =========================================================================
def run_neo4j_benchmark():
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
    return results


# =========================================================================
# ARCADEDB BENCHMARK (Cypher via REST API)
# =========================================================================
def run_arcadedb_benchmark():
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

    return results


# =========================================================================
# MEMGRAPH BENCHMARK (Cypher via Bolt, uses neo4j driver)
# =========================================================================
def run_memgraph_benchmark():
    from neo4j import GraphDatabase
    print("\n" + "=" * 70)
    print("MEMGRAPH LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        return {"error": "Dataset not found"}

    try:
        driver = GraphDatabase.driver("bolt://localhost:7689", auth=("", ""))
        driver.verify_connectivity()
    except Exception as e:
        print(f"  Cannot connect to Memgraph: {e}")
        print("  Start with: docker run -d --name memgraph-lsqb -p 7689:7687 memgraph/memgraph-mage")
        return {"error": str(e)}

    needs_load = True
    if not bench_common.RESET:
        try:
            with driver.session() as session:
                r = session.run("MATCH (p:Person) RETURN count(p) AS c").single()
                if r and r["c"] > 0:
                    needs_load = False
                    print(f"\n[Memgraph] Data already loaded ({r['c']} persons), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[Memgraph] Loading LSQB data...")
        start = time.perf_counter()

        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

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
            # Create indexes
            for label in ["Country", "City", "TagClass", "Tag", "Person", "Forum", "Post", "Comment", "Message"]:
                s.run(f"CREATE INDEX ON :{label}(id)")

            # Nodes
            print("  Loading nodes...")
            batch_run(s, "UNWIND $rows AS r CREATE (:Country {id: toInteger(r.id)})", load_csv(d('Country.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:City {id: toInteger(r.id)})", load_csv(d('City.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:TagClass {id: toInteger(r.id)})", load_csv(d('TagClass.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Tag {id: toInteger(r.id)})", load_csv(d('Tag.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Person {id: toInteger(r.id)})", load_csv(d('Person.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Forum {id: toInteger(r.id)})", load_csv(d('Forum.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Post:Message {id: toInteger(r.id)})", load_csv(d('Post.csv')))
            batch_run(s, "UNWIND $rows AS r CREATE (:Comment:Message {id: toInteger(r.id)})", load_csv(d('Comment.csv')))

            # Edges from FK columns
            print("  Loading edges from FKs...")
            batch_run(s, "UNWIND $rows AS r MATCH (a:City {id: toInteger(r.id)}), (b:Country {id: toInteger(r.ispartof_country)}) CREATE (a)-[:IS_PART_OF]->(b)",
                      load_csv(d('City.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Person {id: toInteger(r.id)}), (b:City {id: toInteger(r.islocatedin_city)}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
                      load_csv(d('Person.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Tag {id: toInteger(r.id)}), (b:TagClass {id: toInteger(r.hastype_tagclass)}) CREATE (a)-[:HAS_TYPE]->(b)",
                      load_csv(d('Tag.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Post {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                      load_csv(d('Post.csv')))
            batch_run(s, "UNWIND $rows AS r MATCH (f:Forum {id: toInteger(r.forum_containerof)}), (p:Post {id: toInteger(r.id)}) CREATE (f)-[:CONTAINER_OF]->(p)",
                      load_csv(d('Post.csv')))
            rows_comment = load_csv(d('Comment.csv'))
            batch_run(s, "UNWIND $rows AS r MATCH (a:Comment {id: toInteger(r.id)}), (b:Person {id: toInteger(r.hascreator_person)}) CREATE (a)-[:HAS_CREATOR]->(b)",
                      rows_comment)
            batch_run(s, "UNWIND $rows AS r WITH r WHERE r.replyof_post <> '' MATCH (a:Comment {id: toInteger(r.id)}), (b:Post {id: toInteger(r.replyof_post)}) CREATE (a)-[:REPLY_OF]->(b)",
                      rows_comment)
            batch_run(s, "UNWIND $rows AS r WITH r WHERE r.replyof_comment <> '' MATCH (a:Comment {id: toInteger(r.id)}), (b:Comment {id: toInteger(r.replyof_comment)}) CREATE (a)-[:REPLY_OF]->(b)",
                      rows_comment)

            # Edge tables
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
        print(f"\n[Memgraph] Running {qid.upper()}...")
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
    return results


# =========================================================================
# POSTGRESQL BENCHMARK (SQL via psycopg2)
# =========================================================================
def run_postgresql_benchmark():
    import psycopg2
    print("\n" + "=" * 70)
    print("POSTGRESQL LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        return {"error": "Dataset not found"}

    try:
        con = psycopg2.connect(host="localhost", port=5433, dbname="lsqb",
                               user="postgres", password="benchmark")
    except Exception:
        # DB might not exist yet — connect to default and create it
        try:
            con0 = psycopg2.connect(host="localhost", port=5433, dbname="postgres",
                                    user="postgres", password="benchmark")
            con0.autocommit = True
            con0.cursor().execute("CREATE DATABASE lsqb")
            con0.close()
            con = psycopg2.connect(host="localhost", port=5433, dbname="lsqb",
                                   user="postgres", password="benchmark")
        except Exception as e:
            print(f"  Cannot connect to PostgreSQL: {e}")
            print("  Start with: docker run -d --name postgres-lsqb -p 5433:5432 "
                  "-e POSTGRES_PASSWORD=benchmark postgres:17")
            return {"error": str(e)}

    con.autocommit = True
    cur = con.cursor()

    needs_load = True
    if not bench_common.RESET:
        try:
            cur.execute("SELECT count(*) FROM person")
            if cur.fetchone()[0] > 0:
                needs_load = False
                print("\n[PostgreSQL] Data already loaded, skipping import")
        except Exception:
            con.rollback()

    if needs_load:
        print("\n[PostgreSQL] Loading LSQB data...")
        start = time.perf_counter()

        d = lambda f: os.path.join(data_dir, f)

        # Drop and recreate tables
        cur.execute("DROP TABLE IF EXISTS person_knows_person, forum_hasmember_person, comment_hastag_tag, post_hastag_tag, person_hasinterest_tag, person_likes_comment, person_likes_post, comment, post, forum, person, tag, tagclass, city, country CASCADE")

        cur.execute("CREATE TABLE country (countryid BIGINT PRIMARY KEY)")
        cur.execute("CREATE TABLE city (cityid BIGINT PRIMARY KEY, ispartof_countryid BIGINT)")
        cur.execute("CREATE TABLE tagclass (tagclassid BIGINT PRIMARY KEY)")
        cur.execute("CREATE TABLE tag (tagid BIGINT PRIMARY KEY, hastype_tagclassid BIGINT)")
        cur.execute("CREATE TABLE person (personid BIGINT PRIMARY KEY, islocatedin_cityid BIGINT)")
        cur.execute("CREATE TABLE forum (forumid BIGINT PRIMARY KEY)")
        cur.execute("CREATE TABLE post (postid BIGINT PRIMARY KEY, hascreator_personid BIGINT, forum_containerofid BIGINT)")
        cur.execute("CREATE TABLE comment (commentid BIGINT PRIMARY KEY, hascreator_personid BIGINT, replyof_postid BIGINT, replyof_commentid BIGINT)")
        cur.execute("CREATE TABLE forum_hasmember_person (forumid BIGINT, personid BIGINT)")
        cur.execute("CREATE TABLE comment_hastag_tag (commentid BIGINT, tagid BIGINT)")
        cur.execute("CREATE TABLE post_hastag_tag (postid BIGINT, tagid BIGINT)")
        cur.execute("CREATE TABLE person_knows_person (person1id BIGINT, person2id BIGINT)")
        cur.execute("CREATE TABLE person_likes_comment (personid BIGINT, commentid BIGINT)")
        cur.execute("CREATE TABLE person_likes_post (personid BIGINT, postid BIGINT)")
        cur.execute("CREATE TABLE person_hasinterest_tag (personid BIGINT, tagid BIGINT)")

        # Load data using COPY
        import csv
        def copy_csv(table, csvfile, columns, col_indices):
            with open(csvfile) as f:
                reader = csv.reader(f, delimiter='|')
                header = next(reader)
                buf = []
                for row in reader:
                    vals = []
                    for idx in col_indices:
                        v = row[idx] if idx < len(row) else ''
                        vals.append(v if v != '' else '\\N')
                    buf.append('\t'.join(vals))
                data = '\n'.join(buf)
            from io import StringIO
            cur.copy_from(StringIO(data), table, columns=columns, null='\\N')

        print("  Loading tables...")
        copy_csv("country", d("Country.csv"), ("countryid",), (0,))
        copy_csv("city", d("City.csv"), ("cityid", "ispartof_countryid"), (0, 1))
        copy_csv("tagclass", d("TagClass.csv"), ("tagclassid",), (0,))
        copy_csv("tag", d("Tag.csv"), ("tagid", "hastype_tagclassid"), (0, 1))
        copy_csv("person", d("Person.csv"), ("personid", "islocatedin_cityid"), (0, 1))
        copy_csv("forum", d("Forum.csv"), ("forumid",), (0,))
        copy_csv("post", d("Post.csv"), ("postid", "hascreator_personid", "forum_containerofid"), (0, 1, 2))
        copy_csv("comment", d("Comment.csv"), ("commentid", "hascreator_personid", "replyof_postid", "replyof_commentid"), (0, 1, 3, 4))
        copy_csv("forum_hasmember_person", d("Forum_hasMember_Person.csv"), ("forumid", "personid"), (0, 1))
        copy_csv("comment_hastag_tag", d("Comment_hasTag_Tag.csv"), ("commentid", "tagid"), (0, 1))
        copy_csv("post_hastag_tag", d("Post_hasTag_Tag.csv"), ("postid", "tagid"), (0, 1))
        copy_csv("person_knows_person", d("Person_knows_Person.csv"), ("person1id", "person2id"), (0, 1))
        copy_csv("person_likes_comment", d("Person_likes_Comment.csv"), ("personid", "commentid"), (0, 1))
        copy_csv("person_likes_post", d("Person_likes_Post.csv"), ("personid", "postid"), (0, 1))
        copy_csv("person_hasinterest_tag", d("Person_hasInterest_Tag.csv"), ("personid", "tagid"), (0, 1))

        # Create views for Message = Post UNION Comment (same as DuckDB)
        cur.execute("""
            CREATE OR REPLACE VIEW message_hastag_tag AS
            SELECT postid AS messageid, tagid FROM post_hastag_tag
            UNION ALL
            SELECT commentid AS messageid, tagid FROM comment_hastag_tag
        """)
        cur.execute("""
            CREATE OR REPLACE VIEW message_hascreator_person AS
            SELECT postid AS messageid, hascreator_personid AS personid FROM post
            UNION ALL
            SELECT commentid AS messageid, hascreator_personid AS personid FROM comment
        """)
        cur.execute("""
            CREATE OR REPLACE VIEW comment_replyof_message AS
            SELECT commentid, replyof_postid AS parentmessageid FROM comment WHERE replyof_postid IS NOT NULL
            UNION ALL
            SELECT commentid, replyof_commentid AS parentmessageid FROM comment WHERE replyof_commentid IS NOT NULL
        """)
        cur.execute("""
            CREATE OR REPLACE VIEW person_likes_message AS
            SELECT personid, commentid AS messageid FROM person_likes_comment
            UNION ALL
            SELECT personid, postid AS messageid FROM person_likes_post
        """)

        # Also need bidirectional KNOWS
        cur.execute("""
            CREATE OR REPLACE VIEW person_knows_person_bidi AS
            SELECT person1id, person2id FROM person_knows_person
            UNION ALL
            SELECT person2id AS person1id, person1id AS person2id FROM person_knows_person
        """)

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # PostgreSQL uses the same SQL queries as DuckDB but with bidirectional KNOWS view
    pg_queries = dict(SQL_QUERIES)
    # Replace Person_knows_Person with bidirectional view
    for qid in pg_queries:
        pg_queries[qid] = pg_queries[qid].replace("Person_knows_Person", "person_knows_person_bidi")

    for qid in [f"q{i}" for i in range(1, 10)]:
        query = pg_queries[qid]
        print(f"\n[PostgreSQL] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            con.rollback()
            results[qid] = "N/A"

    con.close()
    return results


# =========================================================================
# SURREALDB BENCHMARK (requires Docker)
# =========================================================================

# SurrealQL queries for LSQB.
#
# Data model:
#   Entity tables: Country, City, TagClass, Tag, Person, Forum, Post, Comment
#   Record link FKs: City.isPartOf->Country, Tag.hasType->TagClass,
#     Person.isLocatedIn->City, Post.hasCreator->Person, Post.containerOf->Forum,
#     Comment.hasCreator->Person, Comment.replyOfPost->Post,
#     Comment.replyOfComment->Comment
#   Edge tables (RELATE): HAS_MEMBER (Forum->Person), HAS_TAG_C (Comment->Tag),
#     HAS_TAG_P (Post->Tag), KNOWS (Person->Person, both directions),
#     LIKES_C (Person->Comment), LIKES_P (Person->Post),
#     HAS_INTEREST (Person->Tag)
#
# SurrealDB has no Cypher MATCH or SQL JOIN; queries use subqueries with
# record link dereferencing ($parent.field.field) and array::len() for
# cross-product counting.
#
# For :Message (Post | Comment) queries, we run separate queries per type
# and sum results.

SURREALQL_QUERIES = {
    # Q1: Country<-City<-Person<-Forum->Post<-Comment->Tag->TagClass
    # Drive from HAS_TAG_C (Comment,Tag), multiply by HAS_MEMBER fan-out per Forum
    "q1": """
        SELECT math::sum(mc) AS count FROM (
            SELECT array::len(
                (SELECT * FROM HAS_MEMBER
                 WHERE in = $parent.in.replyOfPost.containerOf
                 AND out.isLocatedIn != NONE
                 AND out.isLocatedIn.isPartOf != NONE)
            ) AS mc FROM HAS_TAG_C
            WHERE out.hasType != NONE
              AND in.replyOfPost != NONE
              AND in.replyOfPost.containerOf != NONE
        );
    """,

    # Q2: Person1-KNOWS-Person2, Comment by Person1 replies to Post by Person2
    "q2": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT array::len(
                (SELECT * FROM Comment
                 WHERE hasCreator = $parent.in
                 AND replyOfPost != NONE
                 AND replyOfPost.hasCreator = $parent.out)
            ) AS cnt FROM KNOWS
        );
    """,

    # Q3: Triangle (P1-KNOWS-P2-KNOWS-P3-KNOWS-P1) all in same Country
    # SurrealDB does not support table aliases (AS k2) in subqueries, and
    # nested $parent scoping cannot reference the outer query. Not expressible.
    "q3": None,

    # Q4: Tag<-HAS_TAG-Message->HAS_CREATOR->Person, Message<-LIKES, Message<-REPLY_OF
    # Split: Post as message + Comment as message
    "q4_post": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT
                array::len((SELECT * FROM LIKES_P WHERE out = $parent.in))
                *
                array::len((SELECT * FROM Comment WHERE replyOfPost = $parent.in))
            AS cnt FROM HAS_TAG_P
            WHERE in.hasCreator != NONE
        );
    """,
    "q4_comment": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT
                array::len((SELECT * FROM LIKES_C WHERE out = $parent.in))
                *
                array::len((SELECT * FROM Comment AS c2 WHERE c2.replyOfComment = $parent.in))
            AS cnt FROM HAS_TAG_C
            WHERE in.hasCreator != NONE
        );
    """,

    # Q5: Tag1<-HAS_TAG-Message<-REPLY_OF-Comment->HAS_TAG->Tag2, Tag1!=Tag2
    "q5_post": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT array::len(
                (SELECT * FROM HAS_TAG_C
                 WHERE in.replyOfPost = $parent.in
                 AND out != $parent.out)
            ) AS cnt FROM HAS_TAG_P
        );
    """,
    "q5_comment": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT array::len(
                (SELECT * FROM HAS_TAG_C AS ht2
                 WHERE ht2.in.replyOfComment = $parent.in
                 AND ht2.out != $parent.out)
            ) AS cnt FROM HAS_TAG_C
        );
    """,

    # Q6: P1-KNOWS-P2-KNOWS-P3->HAS_INTEREST->Tag, P1!=P3
    # SurrealDB does not support table aliases (AS k2) in subqueries.
    # Not expressible without aliases for self-join on KNOWS.
    "q6": None,

    # Q7: OPTIONAL MATCH — Tag<-Message->Creator, OPTIONAL Message<-Likes,
    #     OPTIONAL Message<-Reply. count(*) = for each (Tag,Message,Creator):
    #     max(1, likes) * max(1, replies)
    # math::max() in SurrealDB takes an array, not two args.
    "q7_post": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT
                math::max([1, array::len((SELECT * FROM LIKES_P WHERE out = $parent.in))])
                *
                math::max([1, array::len((SELECT * FROM Comment WHERE replyOfPost = $parent.in))])
            AS cnt FROM HAS_TAG_P
            WHERE in.hasCreator != NONE
        );
    """,
    "q7_comment": """
        SELECT math::sum(cnt) AS count FROM (
            SELECT
                math::max([1, array::len((SELECT * FROM LIKES_C WHERE out = $parent.in))])
                *
                math::max([1, array::len((SELECT * FROM Comment WHERE replyOfComment = $parent.in))])
            AS cnt FROM HAS_TAG_C
            WHERE in.hasCreator != NONE
        );
    """,

    # Q8: Like Q5 but anti-join: Comment must NOT have Tag1
    # Requires table aliases (AS ht2, AS ht3) in nested subqueries to
    # distinguish between different scans of HAS_TAG_C. Not expressible
    # without aliases.
    "q8_post": None,
    "q8_comment": None,

    # Q9: Like Q6 but anti-join: P1 must NOT know P3
    # Requires table alias (AS k2) for self-join on KNOWS. Not expressible.
    "q9": None,
}


def run_surrealdb_benchmark():
    """
    SurrealDB LSQB benchmark via Docker (HTTP API).

    SurrealDB uses SurrealQL (not Cypher or SQL). Queries are expressed using
    subqueries with record link dereferencing and array operations for
    cross-product counting. For :Message queries (Q4, Q5, Q7, Q8), separate
    queries run for Post and Comment message types, then results are summed.

    Performance tuning: RocksDB storage, max worker threads, large block cache.

    Setup:
      docker run -d --name surrealdb -p 8000:8000 \\
        -e SURREAL_RUNTIME_WORKER_THREADS=$(nproc) \\
        -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE=8589934592 \\
        -e SURREAL_ROCKSDB_WRITE_BUFFER_SIZE=134217728 \\
        -e SURREAL_ROCKSDB_JOBS_COUNT=16 \\
        -e SURREAL_LOG=warn \\
        -v /tmp/surrealdb_data:/data \\
        surrealdb/surrealdb:v2 start \\
        --user root --pass benchmark \\
        rocksdb:///data/bench.db
    """
    import requests
    import csv

    print("\n" + "=" * 70)
    print("SURREALDB LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        print(f"  Download SF{SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
        return {"error": "Dataset not found"}

    def sql(query, timeout=600):
        """Execute SurrealQL via HTTP API."""
        r = requests.post("http://localhost:8000/sql",
            headers={
                "Accept": "application/json",
                "surreal-ns": "test",
                "surreal-db": "lsqb",
            },
            data=query.encode("utf-8"),
            auth=("root", "benchmark"),
            timeout=timeout)
        r.raise_for_status()
        resp = r.json()
        for stmt in resp:
            if stmt.get("status") == "ERR":
                raise Exception(stmt.get("result", "SurrealDB error"))
        return resp

    # Check connectivity
    try:
        requests.get("http://localhost:8000/health", timeout=5)
        print("  SurrealDB server: OK")
    except Exception as e:
        print(f"  Cannot connect to SurrealDB: {e}")
        print("  Start with:")
        print("    docker run -d --name surrealdb -p 8000:8000 \\")
        print("      -e SURREAL_RUNTIME_WORKER_THREADS=$(nproc) \\")
        print("      -e SURREAL_ROCKSDB_BLOCK_CACHE_SIZE=8589934592 \\")
        print("      -e SURREAL_LOG=warn \\")
        print("      -v /tmp/surrealdb_data:/data \\")
        print("      surrealdb/surrealdb:v2 start \\")
        print("        --user root --pass benchmark \\")
        print("        rocksdb:///data/bench.db")
        return {"error": str(e)}

    # Check if data already loaded
    needs_load = True
    if not bench_common.RESET:
        try:
            r = sql("SELECT count() FROM Comment GROUP ALL;")
            cnt = r[0]["result"][0]["count"]
            if cnt > 0:
                needs_load = False
                print(f"\n[SurrealDB] Data already loaded ({cnt} comments), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[SurrealDB] Loading LSQB data...")
        start = time.perf_counter()

        # Clean slate + define schema in one call
        sql("""
            REMOVE TABLE IF EXISTS KNOWS;
            REMOVE TABLE IF EXISTS HAS_MEMBER;
            REMOVE TABLE IF EXISTS HAS_TAG_C;
            REMOVE TABLE IF EXISTS HAS_TAG_P;
            REMOVE TABLE IF EXISTS LIKES_C;
            REMOVE TABLE IF EXISTS LIKES_P;
            REMOVE TABLE IF EXISTS HAS_INTEREST;
            REMOVE TABLE IF EXISTS Comment;
            REMOVE TABLE IF EXISTS Post;
            REMOVE TABLE IF EXISTS Forum;
            REMOVE TABLE IF EXISTS Person;
            REMOVE TABLE IF EXISTS Tag;
            REMOVE TABLE IF EXISTS TagClass;
            REMOVE TABLE IF EXISTS City;
            REMOVE TABLE IF EXISTS Country;
        """)

        # Define schema — SCHEMAFULL + record links for FKs
        sql("""
            DEFINE TABLE Country SCHEMAFULL;
            DEFINE TABLE City SCHEMAFULL;
            DEFINE FIELD isPartOf ON City TYPE record<Country>;
            DEFINE TABLE TagClass SCHEMAFULL;
            DEFINE TABLE Tag SCHEMAFULL;
            DEFINE FIELD hasType ON Tag TYPE record<TagClass>;
            DEFINE TABLE Person SCHEMAFULL;
            DEFINE FIELD isLocatedIn ON Person TYPE record<City>;
            DEFINE TABLE Forum SCHEMAFULL;
            DEFINE TABLE Post SCHEMAFULL;
            DEFINE FIELD hasCreator ON Post TYPE record<Person>;
            DEFINE FIELD containerOf ON Post TYPE record<Forum>;
            DEFINE TABLE Comment SCHEMAFULL;
            DEFINE FIELD hasCreator ON Comment TYPE record<Person>;
            DEFINE FIELD replyOfPost ON Comment TYPE option<record<Post>>;
            DEFINE FIELD replyOfComment ON Comment TYPE option<record<Comment>>;
            DEFINE TABLE HAS_MEMBER TYPE RELATION FROM Forum TO Person SCHEMAFULL;
            DEFINE TABLE HAS_TAG_C TYPE RELATION FROM Comment TO Tag SCHEMAFULL;
            DEFINE TABLE HAS_TAG_P TYPE RELATION FROM Post TO Tag SCHEMAFULL;
            DEFINE TABLE KNOWS TYPE RELATION FROM Person TO Person SCHEMAFULL;
            DEFINE TABLE LIKES_C TYPE RELATION FROM Person TO Comment SCHEMAFULL;
            DEFINE TABLE LIKES_P TYPE RELATION FROM Person TO Post SCHEMAFULL;
            DEFINE TABLE HAS_INTEREST TYPE RELATION FROM Person TO Tag SCHEMAFULL;
        """)

        d = lambda f: os.path.join(data_dir, f)

        def load_csv_file(path, delim='|'):
            with open(path) as f:
                return list(csv.DictReader(f, delimiter=delim))

        # Load entities with record link FKs
        def load_entities(table, csvfile, fields_map):
            """Load entity CSV. fields_map: {csv_col: (surql_field, ref_table_or_None)}"""
            rows = load_csv_file(d(csvfile))
            batch_size = 10000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                inserts = []
                for row in batch:
                    parts = [f"id: {table}:{row['id']}"]
                    for csv_col, (field, ref_table) in fields_map.items():
                        v = row.get(csv_col, "")
                        if v:
                            if ref_table:
                                parts.append(f"{field}: {ref_table}:{v}")
                            else:
                                parts.append(f"{field}: {v}")
                    inserts.append("{ " + ", ".join(parts) + " }")
                sql(f"INSERT INTO {table} [{', '.join(inserts)}];")
            print(f"    {table}: {len(rows)}")

        print("  Loading entities...")
        load_entities("Country", "Country.csv", {})
        load_entities("City", "City.csv",
                       {"ispartof_country": ("isPartOf", "Country")})
        load_entities("TagClass", "TagClass.csv", {})
        load_entities("Tag", "Tag.csv",
                       {"hastype_tagclass": ("hasType", "TagClass")})
        load_entities("Person", "Person.csv",
                       {"islocatedin_city": ("isLocatedIn", "City")})
        load_entities("Forum", "Forum.csv", {})
        load_entities("Post", "Post.csv", {
            "hascreator_person": ("hasCreator", "Person"),
            "forum_containerof": ("containerOf", "Forum"),
        })

        # Comment needs special handling for optional replyOf fields
        rows = load_csv_file(d("Comment.csv"))
        batch_size = 10000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            inserts = []
            for row in batch:
                parts = [f"id: Comment:{row['id']}",
                         f"hasCreator: Person:{row['hascreator_person']}"]
                if row.get('replyof_post', ''):
                    parts.append(f"replyOfPost: Post:{row['replyof_post']}")
                if row.get('replyof_comment', ''):
                    parts.append(f"replyOfComment: Comment:{row['replyof_comment']}")
                inserts.append("{ " + ", ".join(parts) + " }")
            sql(f"INSERT INTO Comment [{', '.join(inserts)}];")
        print(f"    Comment: {len(rows)}")

        # Load edge tables using INSERT RELATION
        print("  Loading edges...")

        def load_edges(edge_table, csvfile, from_table, from_col, to_table, to_col):
            rows = load_csv_file(d(csvfile))
            batch_size = 10000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                inserts = []
                for row in batch:
                    src, dst = row[from_col], row[to_col]
                    if src and dst:
                        inserts.append(
                            f"{{ in: {from_table}:{src}, out: {to_table}:{dst} }}")
                if inserts:
                    sql(f"INSERT RELATION INTO {edge_table} [{', '.join(inserts)}];")
            print(f"    {edge_table} ({csvfile}): {len(rows)}")

        load_edges("HAS_MEMBER", "Forum_hasMember_Person.csv",
                    "Forum", "id", "Person", "hasmember_person")
        load_edges("HAS_TAG_C", "Comment_hasTag_Tag.csv",
                    "Comment", "id", "Tag", "hastag_tag")
        load_edges("HAS_TAG_P", "Post_hasTag_Tag.csv",
                    "Post", "id", "Tag", "hastag_tag")

        # KNOWS — load both directions for undirected semantics
        # Doubled entries per row, so use smaller batch to stay under 1MB HTTP limit
        rows = load_csv_file(d("Person_knows_Person.csv"))
        knows_batch_size = 5000
        for i in range(0, len(rows), knows_batch_size):
            batch = rows[i:i + knows_batch_size]
            inserts = []
            for row in batch:
                p1, p2 = row['person1id'], row['person2id']
                inserts.append(f"{{ in: Person:{p1}, out: Person:{p2} }}")
                inserts.append(f"{{ in: Person:{p2}, out: Person:{p1} }}")
            sql(f"INSERT RELATION INTO KNOWS [{', '.join(inserts)}];")
        print(f"    KNOWS (both dirs): {len(rows) * 2}")

        load_edges("LIKES_C", "Person_likes_Comment.csv",
                    "Person", "id", "Comment", "likes_comment")
        load_edges("LIKES_P", "Person_likes_Post.csv",
                    "Person", "id", "Post", "likes_post")
        load_edges("HAS_INTEREST", "Person_hasInterest_Tag.csv",
                    "Person", "id", "Tag", "hasinterest_tag")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Verify counts
    try:
        r = sql("SELECT count() FROM Person GROUP ALL;")
        print(f"  Persons: {r[0]['result'][0]['count']}")
        r = sql("SELECT count() FROM Comment GROUP ALL;")
        print(f"  Comments: {r[0]['result'][0]['count']}")
    except Exception:
        pass

    # Helper to extract count from SurrealDB response
    def extract_count(resp):
        """Extract numeric count from SurrealDB query response."""
        result = resp[-1]["result"]
        if isinstance(result, list) and len(result) > 0:
            rec = result[0]
            if isinstance(rec, dict):
                return rec.get("count", rec)
            return rec
        if isinstance(result, (int, float)):
            return result
        return result

    # Queries that need Post + Comment split (Message union)
    SPLIT_QUERIES = {"q4", "q5", "q7", "q8"}
    # SurrealDB lacks JOINs, so pattern-matching queries use nested subqueries
    # which are O(n*m) — much slower than native Cypher/SQL engines.
    # Use 120s timeout per query (generous; most Cypher engines finish in <10s).
    QUERY_TIMEOUT = 120

    for qid in [f"q{i}" for i in range(1, 10)]:
        print(f"\n[SurrealDB] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            if qid in SPLIT_QUERIES:
                # Run Post and Comment variants, sum results
                q_post = SURREALQL_QUERIES.get(f"{qid}_post")
                q_comment = SURREALQL_QUERIES.get(f"{qid}_comment")
                if not q_post or not q_comment:
                    raise Exception("Not expressible in SurrealQL (requires table aliases or JOINs)")
                r_post = sql(q_post, timeout=QUERY_TIMEOUT)
                count_post = extract_count(r_post)
                r_comment = sql(q_comment, timeout=QUERY_TIMEOUT)
                count_comment = extract_count(r_comment)
                count = (count_post or 0) + (count_comment or 0)
            else:
                query = SURREALQL_QUERIES.get(qid)
                if not query:
                    raise Exception("Not expressible in SurrealQL (requires table aliases or JOINs)")
                r = sql(query, timeout=QUERY_TIMEOUT)
                count = extract_count(r)

            elapsed = time.perf_counter() - start
            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            err_msg = str(e)
            if "timed out" in err_msg.lower() or "timeout" in err_msg.lower() or "ReadTimeout" in type(e).__name__:
                print(f"  {qid.upper()} timed out ({elapsed:.0f}s)")
            else:
                print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    return results


# =========================================================================
# DGRAPH BENCHMARK (requires Docker)
# =========================================================================
def run_dgraph_benchmark():
    """
    Dgraph LSQB benchmark via Docker (HTTP API).

    Dgraph uses DQL (formerly GraphQL+-), a graph traversal language that
    returns hierarchical JSON. DQL cannot express arbitrary subgraph pattern
    matching, but some LSQB queries can be computed using value variable
    propagation and math():

    - Q1 (chain): sum(val()) propagation from leaf to root counts paths
    - Q4 (star): math(tags × likes × replies) per message
    - Q7 (optional star): math(tags × max(likes,1) × max(replies,1))
    - Q2,Q3,Q5,Q6,Q8,Q9: require per-row joins/self-joins/anti-joins → N/A

    Performance tuning applied:
    - Badger cache = 8GB (fast reads)
    - Compression = none (fastest writes)
    - High mutation/query limits

    Setup:
      docker network create dgraph-net 2>/dev/null
      docker run -d --name dgraph-zero --network dgraph-net \\
        -p 5080:5080 -p 6080:6080 \\
        dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080
      docker run -d --name dgraph-alpha --network dgraph-net \\
        -p 8080:8080 -p 9080:9080 \\
        -v /tmp/dgraph_data:/dgraph \\
        dgraph/dgraph:latest dgraph alpha \\
          --my=dgraph-alpha:7080 \\
          --zero=dgraph-zero:5080 \\
          --cache size-mb=8192 \\
          --badger "compression=none; numgoroutines=8" \\
          --security whitelist=0.0.0.0/0 \\
          --limit "mutations-nquad=5000000; query-edge=10000000"
    """
    import requests
    import csv

    print("\n" + "=" * 70)
    print("DGRAPH LSQB BENCHMARK")
    print("=" * 70)

    results = {}
    alpha_url = "http://localhost:8080"
    data_dir = data_dir_merged()

    if not os.path.isdir(data_dir):
        print(f"  Dataset not found: {data_dir}")
        print(f"  Download SF{SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
        return {"error": "Dataset not found"}

    def alter(schema_or_json, timeout=60):
        """Alter schema or drop data (with retry for post-drop transient errors)."""
        for attempt in range(3):
            if isinstance(schema_or_json, dict):
                r = requests.post(f"{alpha_url}/alter",
                                  json=schema_or_json, timeout=timeout)
            else:
                r = requests.post(f"{alpha_url}/alter",
                                  data=schema_or_json.encode("utf-8"), timeout=timeout)
            r.raise_for_status()
            resp = r.json()
            if "errors" not in resp:
                return resp
            err = resp["errors"][0].get("message", "")
            if "aborted" in err.lower() and attempt < 2:
                time.sleep(1)
                continue
            raise Exception(f"Dgraph alter error: {err}")
        return resp

    def mutate(nquads, timeout=600):
        """Execute an RDF N-Quad mutation with immediate commit (with retry)."""
        payload = f'{{\n  set {{\n{nquads}\n  }}\n}}'
        for attempt in range(3):
            r = requests.post(f"{alpha_url}/mutate?commitNow=1",
                              data=payload.encode("utf-8"),
                              headers={"Content-Type": "application/rdf"},
                              timeout=timeout)
            r.raise_for_status()
            resp = r.json()
            if resp.get("data") is not None:
                return resp
            errs = resp.get("errors", [])
            err_msg = errs[0].get("message", "") if errs else ""
            if "aborted" in err_msg.lower() and attempt < 2:
                time.sleep(1)
                continue
            raise Exception(f"Dgraph mutate error: {err_msg}")
        return resp

    def query(dql, timeout=600):
        """Execute a DQL query."""
        r = requests.post(f"{alpha_url}/query",
                          data=dql.encode("utf-8"),
                          headers={"Content-Type": "application/dql"},
                          timeout=timeout)
        r.raise_for_status()
        return r.json()

    # Check connectivity
    try:
        r = requests.get(f"{alpha_url}/health", timeout=5)
        r.raise_for_status()
        print("  Dgraph Alpha server: OK")
    except Exception as e:
        print(f"  Cannot connect to Dgraph Alpha: {e}")
        print("  Start with:")
        print("    docker network create dgraph-net 2>/dev/null")
        print("    docker run -d --name dgraph-zero --network dgraph-net \\")
        print("      -p 5080:5080 -p 6080:6080 \\")
        print("      dgraph/dgraph:latest dgraph zero --my=dgraph-zero:5080")
        print("    docker run -d --name dgraph-alpha --network dgraph-net \\")
        print("      -p 8080:8080 -p 9080:9080 \\")
        print("      -v /tmp/dgraph_data:/dgraph \\")
        print("      dgraph/dgraph:latest dgraph alpha \\")
        print("        --my=dgraph-alpha:7080 \\")
        print("        --zero=dgraph-zero:5080 \\")
        print("        --cache size-mb=8192 \\")
        print('        --badger "compression=none; numgoroutines=8" \\')
        print("        --security whitelist=0.0.0.0/0 \\")
        print('        --limit "mutations-nquad=5000000; query-edge=10000000"')
        return {"error": str(e)}

    # Check if data already loaded (look for Person nodes from LSQB schema)
    needs_load = True
    if not bench_common.RESET:
        try:
            resp = query('{ count(func: type(Person)) { total: count(uid) } }')
            cnt = resp["data"]["count"][0]["total"]
            if cnt > 0:
                needs_load = False
                print(f"\n[Dgraph] Data already loaded ({cnt} persons), skipping import")
        except Exception:
            pass

    if needs_load:
        print("\n[Dgraph] Loading LSQB data...")
        start = time.perf_counter()

        # Drop all existing data and schema
        alter({"drop_all": True})
        time.sleep(2)  # Let Dgraph settle after drop_all

        # Define schema — predicates for entity IDs and relationships.
        # @reverse enables ~predicate traversal (required for Q1, Q4, Q7).
        alter("""
            entity_id: int @index(int) .
            is_part_of: uid @reverse .
            is_located_in: uid @reverse .
            has_type: uid .
            has_creator: uid @reverse .
            container_of: uid @reverse .
            reply_of_post: uid @reverse .
            reply_of_comment: uid @reverse .
            knows: [uid] @reverse .
            has_member: [uid] @reverse .
            has_tag: [uid] @reverse .
            likes: [uid] @reverse .
            has_interest: [uid] @reverse .

            type Country {
                entity_id
            }
            type City {
                entity_id
                is_part_of
            }
            type TagClass {
                entity_id
            }
            type Tag {
                entity_id
                has_type
            }
            type Person {
                entity_id
                is_located_in
                knows
                has_interest
            }
            type Forum {
                entity_id
                has_member
            }
            type Post {
                entity_id
                has_creator
                container_of
                has_tag
            }
            type Comment {
                entity_id
                has_creator
                reply_of_post
                reply_of_comment
                has_tag
            }
        """)

        d = lambda f: os.path.join(data_dir, f)

        def load_csv_file(path, delim='|'):
            with open(path) as f:
                return list(csv.DictReader(f, delimiter=delim))

        # Phase 1: Load entities, collect ID → UID mappings per type
        # We use blank nodes _:Type_ID to get consistent UIDs
        uid_maps = {}  # type_name -> {entity_id -> uid}

        def load_entity_table(type_name, csvfile, fk_fields=None):
            """Load entity CSV into Dgraph. Returns {entity_id: uid} map.
            fk_fields: dict of {csv_col: (predicate, ref_type)} for FK refs.
            FK targets MUST already be loaded (in uid_maps) — blank nodes
            are per-mutation, so cross-mutation refs need actual UIDs."""
            rows = load_csv_file(d(csvfile))
            id_to_uid = {}
            batch_size = 10000
            if fk_fields is None:
                fk_fields = {}

            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                lines = []
                for row in batch:
                    eid = row['id']
                    bnode = f"_:{type_name}_{eid}"
                    lines.append(f'    {bnode} <entity_id> "{eid}"^^<xs:int> .')
                    lines.append(f'    {bnode} <dgraph.type> "{type_name}" .')
                    for csv_col, (pred, ref_type) in fk_fields.items():
                        fk_val = row.get(csv_col, "")
                        if fk_val:
                            # Use actual UID from previously loaded entity
                            ref_uid = uid_maps.get(ref_type, {}).get(int(fk_val))
                            if ref_uid:
                                lines.append(
                                    f'    {bnode} <{pred}> <{ref_uid}> .')
                if lines:
                    resp = mutate("\n".join(lines))
                    uids = resp.get("data", {}).get("uids", {})
                    for key, uid in uids.items():
                        # key is "Type_ID"
                        parts = key.split("_", 1)
                        if len(parts) == 2:
                            id_to_uid[int(parts[1])] = uid

            uid_maps[type_name] = id_to_uid
            print(f"    {type_name}: {len(rows)} (mapped {len(id_to_uid)} UIDs)")

        print("  Loading entities...")
        load_entity_table("Country", "Country.csv")
        load_entity_table("City", "City.csv",
                          {"ispartof_country": ("is_part_of", "Country")})
        load_entity_table("TagClass", "TagClass.csv")
        load_entity_table("Tag", "Tag.csv",
                          {"hastype_tagclass": ("has_type", "TagClass")})
        load_entity_table("Person", "Person.csv",
                          {"islocatedin_city": ("is_located_in", "City")})
        load_entity_table("Forum", "Forum.csv")
        load_entity_table("Post", "Post.csv", {
            "hascreator_person": ("has_creator", "Person"),
            "forum_containerof": ("container_of", "Forum"),
        })

        # Comment — two-pass loading because the CSV is NOT sorted by ID.
        # Pass 1: Load all Comments with has_creator and reply_of_post
        #   (Person and Post UIDs are already resolved).
        # Pass 2: Add reply_of_comment edges (now all Comment UIDs exist).
        person_map = uid_maps.get("Person", {})
        post_map = uid_maps.get("Post", {})
        rows = load_csv_file(d("Comment.csv"))
        comment_id_to_uid = {}
        batch_size = 10000

        # Pass 1: create Comment nodes + has_creator + reply_of_post
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            lines = []
            for row in batch:
                eid = row['id']
                bnode = f"_:Comment_{eid}"
                lines.append(f'    {bnode} <entity_id> "{eid}"^^<xs:int> .')
                lines.append(f'    {bnode} <dgraph.type> "Comment" .')
                creator_uid = person_map.get(int(row["hascreator_person"]))
                if creator_uid:
                    lines.append(
                        f'    {bnode} <has_creator> <{creator_uid}> .')
                if row.get('replyof_post', ''):
                    post_uid = post_map.get(int(row['replyof_post']))
                    if post_uid:
                        lines.append(
                            f'    {bnode} <reply_of_post> <{post_uid}> .')
            if lines:
                resp = mutate("\n".join(lines))
                uids = resp.get("data", {}).get("uids", {})
                for key, uid in uids.items():
                    parts = key.split("_", 1)
                    if len(parts) == 2 and parts[0] == "Comment":
                        comment_id_to_uid[int(parts[1])] = uid
        uid_maps["Comment"] = comment_id_to_uid
        print(f"    Comment: {len(rows)} (mapped {len(comment_id_to_uid)} UIDs)")

        # Pass 2: add reply_of_comment edges (all Comment UIDs now exist)
        reply_count = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            lines = []
            for row in batch:
                if row.get('replyof_comment', ''):
                    src_uid = comment_id_to_uid.get(int(row['id']))
                    parent_uid = comment_id_to_uid.get(
                        int(row['replyof_comment']))
                    if src_uid and parent_uid:
                        lines.append(
                            f'    <{src_uid}> <reply_of_comment> <{parent_uid}> .')
                        reply_count += 1
            if lines:
                mutate("\n".join(lines))
        print(f"    Comment reply_of_comment edges: {reply_count}")

        # Phase 2: Load edge tables using resolved UIDs
        print("  Loading edges...")

        def load_edge_table(edge_name, predicate, csvfile,
                            from_type, from_col, to_type, to_col,
                            bidirectional=False):
            """Load edge CSV using UID lookups."""
            rows = load_csv_file(d(csvfile))
            from_map = uid_maps.get(from_type, {})
            to_map = uid_maps.get(to_type, {})
            batch_size = 20000
            loaded = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                lines = []
                for row in batch:
                    src_id = int(row[from_col])
                    dst_id = int(row[to_col])
                    src_uid = from_map.get(src_id)
                    dst_uid = to_map.get(dst_id)
                    if src_uid and dst_uid:
                        lines.append(f'    <{src_uid}> <{predicate}> <{dst_uid}> .')
                        if bidirectional:
                            lines.append(
                                f'    <{dst_uid}> <{predicate}> <{src_uid}> .')
                        loaded += 1
                if lines:
                    mutate("\n".join(lines))
            count_str = f"{loaded * 2} (both dirs)" if bidirectional else str(loaded)
            print(f"    {edge_name}: {count_str}")

        load_edge_table("HAS_MEMBER", "has_member",
                        "Forum_hasMember_Person.csv",
                        "Forum", "id", "Person", "hasmember_person")
        load_edge_table("HAS_TAG (Comment)", "has_tag",
                        "Comment_hasTag_Tag.csv",
                        "Comment", "id", "Tag", "hastag_tag")
        load_edge_table("HAS_TAG (Post)", "has_tag",
                        "Post_hasTag_Tag.csv",
                        "Post", "id", "Tag", "hastag_tag")
        load_edge_table("KNOWS", "knows",
                        "Person_knows_Person.csv",
                        "Person", "person1id", "Person", "person2id",
                        bidirectional=True)
        load_edge_table("LIKES (Comment)", "likes",
                        "Person_likes_Comment.csv",
                        "Person", "id", "Comment", "likes_comment")
        load_edge_table("LIKES (Post)", "likes",
                        "Person_likes_Post.csv",
                        "Person", "id", "Post", "likes_post")
        load_edge_table("HAS_INTEREST", "has_interest",
                        "Person_hasInterest_Tag.csv",
                        "Person", "id", "Tag", "hasinterest_tag")

        load_time = time.perf_counter() - start
        results["load"] = load_time
        print(f"  Load time: {load_time:.2f}s")

    # Verify counts
    try:
        resp = query('{ count(func: type(Person)) { total: count(uid) } }')
        print(f"  Persons: {resp['data']['count'][0]['total']}")
        resp = query('{ count(func: type(Comment)) { total: count(uid) } }')
        print(f"  Comments: {resp['data']['count'][0]['total']}")
    except Exception:
        pass

    # ---------------------------------------------------------------
    # DQL QUERIES
    # ---------------------------------------------------------------
    # Q1, Q4, Q7 can be expressed in DQL using value variable
    # propagation and math(). Q2,Q3,Q5,Q6,Q8,Q9 require per-row
    # joins/correlations that DQL cannot express.
    #
    # Q1 (chain): propagate count(has_type) upward through the chain
    #   using sum(val()) at each level — gives total path count.
    #
    # Q4 (star from Message): for each Message with tags, likes, and
    #   replies, the tuple count = tags × likes × replies. Computed
    #   with math(t * l * r). Post and Comment counted separately.
    #
    # Q7 (Q4 with OPTIONAL MATCH): like Q4 but messages without likes
    #   or replies still contribute. Use max(count, 1) for optionals.
    # ---------------------------------------------------------------

    DQL_QUERIES = {
        # Q1: Country←City←Person←Forum→Post←Comment→Tag→TagClass
        # Value variables propagate path counts from leaf to root.
        "q1": """{
  var(func: type(Country)) {
    ~is_part_of @filter(type(City)) {
      ~is_located_in @filter(type(Person)) {
        ~has_member @filter(type(Forum)) {
          ~container_of @filter(type(Post)) {
            ~reply_of_post @filter(type(Comment)) {
              has_tag @filter(type(Tag)) {
                g as count(has_type)
              }
              f as sum(val(g))
            }
            e as sum(val(f))
          }
          d as sum(val(e))
        }
        c as sum(val(d))
      }
      b as sum(val(c))
    }
    a as sum(val(b))
  }

  total() {
    count: sum(val(a))
  }
}""",

        # Q4: (Tag)←[HAS_TAG]-(Message)-[HAS_CREATOR]→(Person),
        #     (Message)←[LIKES]-(Person), (Message)←[REPLY_OF]-(Comment)
        # count = sum over messages of: tags × likes × replies
        # Only messages with ≥1 of each contribute (INNER JOIN).
        "q4": """{
  var(func: type(Post)) {
    tp as count(has_tag)
    lp as count(~likes)
    rp as count(~reply_of_post)
    pp as math(tp * lp * rp)
  }

  var(func: type(Comment)) {
    tc as count(has_tag)
    lc as count(~likes)
    rc as count(~reply_of_comment)
    pc as math(tc * lc * rc)
  }

  total() {
    post_count: sum(val(pp))
    comment_count: sum(val(pc))
  }
}""",

        # Q7: like Q4 but with OPTIONAL MATCH on likes and replies.
        # Messages without likes or replies still contribute (NULL = 1 row).
        # count = sum of: tags × max(likes,1) × max(replies,1)
        "q7": """{
  var(func: type(Post)) @filter(has(has_tag)) {
    tp7 as count(has_tag)
    lp7 as count(~likes)
    rp7 as count(~reply_of_post)
    pp7 as math(tp7 * max(lp7, 1) * max(rp7, 1))
  }

  var(func: type(Comment)) @filter(has(has_tag)) {
    tc7 as count(has_tag)
    lc7 as count(~likes)
    rc7 as count(~reply_of_comment)
    pc7 as math(tc7 * max(lc7, 1) * max(rc7, 1))
  }

  total() {
    post_count: sum(val(pp7))
    comment_count: sum(val(pc7))
  }
}""",
    }

    # Queries that return separate post/comment totals (need summing)
    SPLIT_QUERIES = {"q4", "q7"}

    # Reasons for N/A queries
    NA_REASONS = {
        "q2": "requires per-row join between KNOWS and HAS_CREATOR paths",
        "q3": "requires triangle pattern with self-joins on Person",
        "q5": "requires cross-reference inequality (tag1 ≠ tag2) across nesting levels",
        "q6": "requires per-row inequality (person1 ≠ person3) across nesting levels",
        "q8": "requires NOT EXISTS (anti-join) not available in DQL",
        "q9": "requires NOT EXISTS + per-row inequality not available in DQL",
    }

    QUERY_TIMEOUT = 120

    for qid in [f"q{i}" for i in range(1, 10)]:
        dql = DQL_QUERIES.get(qid)
        if not dql:
            reason = NA_REASONS.get(qid, "not expressible in DQL")
            print(f"\n[Dgraph] {qid.upper()}: N/A ({reason})")
            results[qid] = "N/A"
            continue

        print(f"\n[Dgraph] Running {qid.upper()}...")
        start = time.perf_counter()
        try:
            resp = query(dql, timeout=QUERY_TIMEOUT)
            elapsed = time.perf_counter() - start
            total_list = resp.get("data", {}).get("total", [])
            # DQL returns aggregations as a list of dicts — merge all
            data = {}
            if isinstance(total_list, list):
                for item in total_list:
                    if isinstance(item, dict):
                        data.update(item)
            elif isinstance(total_list, dict):
                data = total_list

            if qid in SPLIT_QUERIES:
                count = int(data.get("post_count", 0) or 0) + int(data.get("comment_count", 0) or 0)
            else:
                count = int(data.get("count", 0) or 0)

            results[qid] = elapsed
            print(f"  {qid.upper()} time: {elapsed:.2f}s  (count={count})")
        except Exception as e:
            elapsed = time.perf_counter() - start
            err_msg = str(e)
            if "timed out" in err_msg.lower() or "timeout" in err_msg.lower():
                print(f"  {qid.upper()} timed out ({elapsed:.0f}s)")
            else:
                print(f"  {qid.upper()} failed ({elapsed:.2f}s): {e}")
            results[qid] = "N/A"

    return results


# =========================================================================
# MAIN
# =========================================================================
AVAILABLE_SYSTEMS = {
    "kuzu": ("Kuzu", run_kuzu_benchmark),
    "duckdb": ("DuckDB", run_duckdb_benchmark),
    "neo4j": ("Neo4j", run_neo4j_benchmark),
    "memgraph": ("Memgraph", run_memgraph_benchmark),
    "postgresql": ("PostgreSQL", run_postgresql_benchmark),
    "arcadedb": ("ArcadeDB-Server", run_arcadedb_benchmark),
    "surrealdb": ("SurrealDB", run_surrealdb_benchmark),
    "dgraph": ("Dgraph", run_dgraph_benchmark),
}

# Systems excluded from default runs (must be explicitly named to run).
# SurrealDB and Dgraph lack pattern matching / join semantics — all queries
# return N/A. Still available via: python3 lsqb_benchmark.py surrealdb dgraph
DEFAULT_EXCLUDE = {"surrealdb", "dgraph"}

if __name__ == "__main__":
    import argparse as _ap

    # Custom arg parsing to handle --sf before delegating to bench_common
    parser = _ap.ArgumentParser(description="LSQB multi-vendor benchmark")
    parser.add_argument("--sf", default="1",
                        help="LDBC SNB scale factor (default: 1)")
    parser.add_argument("--reset", action="store_true",
                        help="Delete all data and reload from scratch")
    parser.add_argument("systems", nargs="*",
                        help=f"Systems to benchmark (default: all). "
                             f"Choices: {', '.join(AVAILABLE_SYSTEMS.keys())}")
    args = parser.parse_args()

    SF = args.sf
    bench_common.RESET = args.reset

    if args.systems:
        systems_to_run = args.systems
    else:
        systems_to_run = [k for k in AVAILABLE_SYSTEMS if k not in DEFAULT_EXCLUDE]

    all_results = {}
    for key in systems_to_run:
        key = key.lower()
        if key not in AVAILABLE_SYSTEMS:
            print(f"Unknown system: {key}. "
                  f"Available: {', '.join(AVAILABLE_SYSTEMS.keys())}")
            continue
        name, func = AVAILABLE_SYSTEMS[key]
        try:
            r = func()
            if isinstance(r, dict) and "error" not in r:
                all_results[name] = r
        except Exception as e:
            print(f"\n{name} failed: {e}")
            import traceback; traceback.print_exc()

    if all_results:
        bench_common.print_summary(
            f"LSQB SF{SF} (subgraph pattern matching)",
            LSQB_METRICS, all_results)
