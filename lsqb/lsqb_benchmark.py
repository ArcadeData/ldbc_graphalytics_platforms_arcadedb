#!/usr/bin/env python3
"""
LSQB (Labelled Subgraph Query Benchmark): Kuzu vs Neo4j vs DuckDB
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
# MAIN
# =========================================================================
AVAILABLE_SYSTEMS = {
    "kuzu": ("Kuzu", run_kuzu_benchmark),
    "duckdb": ("DuckDB", run_duckdb_benchmark),
    "neo4j": ("Neo4j", run_neo4j_benchmark),
    "memgraph": ("Memgraph", run_memgraph_benchmark),
    "postgresql": ("PostgreSQL", run_postgresql_benchmark),
    "arcadedb": ("ArcadeDB-Server", run_arcadedb_benchmark),
}

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

    systems_to_run = args.systems if args.systems else list(AVAILABLE_SYSTEMS.keys())

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
