"""
Shared constants, queries, and helpers for the LSQB benchmark systems.
"""

import time
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

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
