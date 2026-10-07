# Decisions log, 2026-10-07 (autonomous session; review and steer)

Context: the ArcadeDB Docker Graphalytics row timed `count(*)` calls; I changed it to full output, then found (code audit) that the vendors are inconsistent.
Owner's decision: one rule for everybody, **compute only**: the algorithm runs completely server-side and the timed call returns a count/summary (no per-vertex transfer).
If a compute-only call turns out to be optimised away for some vendor, the fallback is: **all vendors full output**.

## Audit of what each driver returned in the timed call (before this session)
| Vendor | PR | WCC | BFS | LCC | SSSP | CDLP |
|---|---|---|---|---|---|---|
| ArcadeDB Docker (before 10-07) | count | count | count | count | count | count |
| Neo4j | full | top-10 | one count | top-10 | n/a | n/a |
| Kuzu | full | full | full | n/a | n/a | n/a |
| LadybugDB | top-10 | top-10 | full | top-10 | n/a | n/a |
| DuckPGQ | full | full | full | full | n/a | n/a |
| Memgraph | full | full | full | n/a | full | full |
| FalkorDB | full | full | one row | n/a | n/a | full |
| ArangoDB | server-side Pregel | server-side Pregel | grouped by depth | top-10 | full | server-side |
| HugeGraph | server-side task | server-side | server-side | server-side | server-side | server-side |

## Decisions
1. 10-07 00:50 Fixed the false sentence in docs (the note claimed every other system returns full output) and pushed.
