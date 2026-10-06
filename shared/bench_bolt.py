"""Bolt access to a remote (Docker) ArcadeDB for the benchmarks.

Every timed query and algorithm call of the ArcadeDB Docker drivers goes over Bolt, like Neo4j and Memgraph, so the protocol is
no longer a difference between the systems. Setup that Bolt cannot do (create or drop a database, SQL schema statements, the GAV
create/rebuild, the LSQB bulk load through /api/v1/batch) stays on HTTP.

  ARCADEDB_BENCH_PROTOCOL=http   run the timed calls over the HTTP API instead (the previous behaviour), for an A/B on one image
  ARCADEDB_BENCH_BOLT_PORT=7687  host port of the Bolt listener

The server needs the Bolt plugin: JAVA_OPTS must carry BOLT_PLUGIN_OPT and the container must publish 7687.
"""

import os
import time

# `<pluginName>:<pluginFullClass>`; the arcadedb image ships the Bolt module (arcadedb-bolt-*-shaded.jar in lib/)
BOLT_PLUGIN_OPT = "-Darcadedb.server.plugins=Bolt:com.arcadedb.bolt.BoltProtocolPlugin"


def protocol():
    return os.environ.get("ARCADEDB_BENCH_PROTOCOL", "bolt").lower()


def use_bolt():
    return protocol() == "bolt"


def bolt_port():
    return os.environ.get("ARCADEDB_BENCH_BOLT_PORT", "7687")


class ArcadeBolt:
    """One driver (connection pool) to one ArcadeDB database over Bolt."""

    def __init__(self, database, port=None, user="root", password="benchmark", wait=180):
        from neo4j import GraphDatabase   # only installed with the `neo4j` extra of pyproject.toml
        self.database = database
        self.uri = f"bolt://localhost:{port or bolt_port()}"
        self.driver = GraphDatabase.driver(self.uri, auth=(user, password))
        # the port opens before the plugin and the database are ready: retry until a query answers
        deadline = time.monotonic() + wait
        last = None
        while time.monotonic() < deadline:
            try:
                self.driver.verify_connectivity()
                self.run("RETURN 1 AS ok")
                return
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(2)
        raise RuntimeError(f"Bolt endpoint {self.uri} not ready after {wait}s: {last}")

    def run(self, cypher, params=None):
        """All rows of one statement, as dicts (the result is consumed inside the timed call)."""
        with self.driver.session(database=self.database) as session:
            return session.run(cypher, params or {}).data()

    def scalar(self, cypher, key):
        rows = self.run(cypher)
        if not rows:
            raise RuntimeError(f"no row from {cypher[:80]}")
        return rows[0][key]

    def rows(self, cypher, *cols):
        """Stream the rows of a statement as tuples (full per-vertex exports are millions of rows)."""
        with self.driver.session(database=self.database) as session:
            for record in session.run(cypher):
                yield tuple(record[c] for c in cols)

    def close(self):
        try:
            self.driver.close()
        except Exception:  # noqa: BLE001
            pass
