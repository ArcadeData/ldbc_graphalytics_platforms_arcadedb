"""DuckDB LSQB benchmark module."""

import time
import os

from . import _common
from ._common import data_dir_merged, SQL_QUERIES, bench_common


def run_benchmark():
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
        print(f"  Download SF{_common.SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
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
