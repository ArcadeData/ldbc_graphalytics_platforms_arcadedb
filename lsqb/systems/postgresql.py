"""PostgreSQL LSQB benchmark module."""

import time
import os

from ._common import data_dir_merged, SQL_QUERIES, bench_common


def run_benchmark():
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
