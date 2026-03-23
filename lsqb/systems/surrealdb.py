"""SurrealDB LSQB benchmark module."""

import time
import os

from . import _common
from ._common import data_dir_merged, bench_common


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


def run_benchmark():
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
        print(f"  Download SF{_common.SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
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

    bench_common.cleanup_docker("surrealdb")
    return results


run_benchmark._cleanup = lambda: bench_common.cleanup_docker("surrealdb")
