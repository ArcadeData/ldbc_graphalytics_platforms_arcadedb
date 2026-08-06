# Java CI Workflow — Design

## Context

This repo has no CI today. As a first step (Python CI to follow separately), we need
a GitHub Actions workflow that builds and verifies the Java/Maven platform driver
(`pom.xml`, `src/main/java/...`) on every relevant push and pull request.

## Blocking findings from feasibility research

Two problems were discovered while validating that a CI build is even possible, and
both must be fixed before the workflow can pass:

1. **The project does not currently compile.** `src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java`
   declares `public class ArcadedbPlatform` (lowercase "db"), which doesn't match its
   filename. Java requires an exact match. This was introduced by an accidental
   rename in commit `3cfd55a` ("Refactoring to avoid building arcadedb locally").
   The correct name, `ArcadeDBPlatform`, is confirmed by both the filename and
   `src/main/resources/arcadedb.platform`, which registers the fully-qualified class
   `science.atlarge.graphalytics.arcadedb.ArcadeDBPlatform`. Fix: revert the class
   name to `ArcadeDBPlatform`.

2. **The pinned ArcadeDB engine version is unpublished.** `pom.xml` pins
   `arcadedb.version = 26.4.1-SNAPSHOT`, which only exists in a developer's local
   `~/.m2` cache (built from a sibling `arcadedb` checkout). It is not on Maven
   Central or any repository declared in this project's `pom.xml`, so a clean CI
   runner cannot resolve it. Fix: bump `arcadedb.version` to `26.8.1`, the latest
   release on Maven Central (confirmed via `https://repo1.maven.org/maven2/com/arcadedb/arcadedb-engine/maven-metadata.xml`).

Neither fix is CI configuration — they're prerequisite code/pom changes that unblock
the workflow. They're included as the first steps of the implementation plan.

## Design

### `.github/workflows/java-ci.yml`

**Triggers**: `push` to `main` and `pull_request`, both scoped with `paths:` to:
- `pom.xml`
- `src/**`
- `.github/workflows/java-ci.yml`

This keeps the Java workflow from running on Python-only or docs-only changes,
matching the intent that a separate Python workflow will exist alongside it.

**Job**: single job (`build`), `runs-on: ubuntu-latest`.

Steps:
1. `actions/checkout`
2. `actions/setup-java` — Temurin distribution, `java-version: 21` (matches
   `pom.xml`'s `maven-compiler-plugin` `source`/`target`), `cache: maven` for
   dependency caching between runs.
3. `mvn --batch-mode --no-transfer-progress verify`

`verify` runs the full lifecycle through `compile` → `test` → `package` → `verify`:
- Compiles `src/main/java`.
- Runs any tests under `src/test` (none exist today, but this means future tests
  are picked up automatically with zero workflow changes).
- Executes the `license-maven-plugin` `check` goal, which is already configured in
  `pom.xml` but is never invoked by the documented `mvn package -DskipTests` build
  command (that command stops at the `package` phase, before `verify`). This is the
  first time the license-header check will actually run — if any file is missing
  the required header, this workflow surfaces it for the first time. That's treated
  as a follow-up fix, not a workflow design change, if it happens.

**No artifact upload.** This workflow is a compile/verify gate, not a release or
publish workflow. The existing `maven-release-plugin` / manual `mvn package` flow
documented in `CLAUDE.md` remains how distributions get built for actual use.

### Out of scope (explicitly deferred)

- Python CI (separate follow-up workflow).
- Building/publishing release artifacts from CI.
- Running actual benchmark algorithms in CI (would require datasets and is a much
  heavier "smoke test" concern, not a build-verification concern).
- Matrix builds across JDK distributions or operating systems — this project has no
  OS-specific code, so a single Temurin 21 / ubuntu-latest runner is sufficient.

## Implementation plan outline

1. Fix `ArcadeDBPlatform.java` class name typo.
2. Bump `pom.xml`'s `arcadedb.version` to `26.8.1`.
3. Run `mvn verify` locally to confirm both fixes are sufficient (catches the license
   header risk noted above before it hits CI).
4. Add `.github/workflows/java-ci.yml` per the design above.
5. Verify the workflow via a PR (or `workflow_dispatch` if needed) before merging.
