"""Dgraph LSQB benchmark module."""

import time
import os

from . import _common
from ._common import data_dir_merged, bench_common


def run_benchmark():
    """
    Dgraph LSQB benchmark via Docker (HTTP API).

    Dgraph uses DQL (formerly GraphQL+-), a graph traversal language that
    returns hierarchical JSON. DQL cannot express arbitrary subgraph pattern
    matching, but some LSQB queries can be computed using value variable
    propagation and math():

    - Q1 (chain): sum(val()) propagation from leaf to root counts paths
    - Q4 (star): math(tags x likes x replies) per message
    - Q7 (optional star): math(tags x max(likes,1) x max(replies,1))
    - Q2,Q3,Q5,Q6,Q8,Q9: require per-row joins/self-joins/anti-joins -> N/A

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
        print(f"  Download SF{_common.SF} merged-fk from https://datasets.ldbcouncil.org/lsqb/")
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

        # Phase 1: Load entities, collect ID -> UID mappings per type
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
    #   replies, the tuple count = tags x likes x replies. Computed
    #   with math(t * l * r). Post and Comment counted separately.
    #
    # Q7 (Q4 with OPTIONAL MATCH): like Q4 but messages without likes
    #   or replies still contribute. Use max(count, 1) for optionals.
    # ---------------------------------------------------------------

    DQL_QUERIES = {
        # Q1: Country<-City<-Person<-Forum->Post<-Comment->Tag->TagClass
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

        # Q4: (Tag)<-[HAS_TAG]-(Message)-[HAS_CREATOR]->(Person),
        #     (Message)<-[LIKES]-(Person), (Message)<-[REPLY_OF]-(Comment)
        # count = sum over messages of: tags x likes x replies
        # Only messages with >=1 of each contribute (INNER JOIN).
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
        # count = sum of: tags x max(likes,1) x max(replies,1)
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
        "q5": "requires cross-reference inequality (tag1 != tag2) across nesting levels",
        "q6": "requires per-row inequality (person1 != person3) across nesting levels",
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
