# Java CI Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Get this repo's Java/Maven platform driver building again and add a GitHub Actions workflow that verifies it on every push/PR.

**Architecture:** Two prerequisite one-line fixes unblock local and CI builds (a class-name typo and an unpublished dependency version), then a single new GitHub Actions workflow file runs `mvn verify` on a Temurin 21 / ubuntu-latest runner, scoped to Java-relevant paths.

**Tech Stack:** Java 21, Maven 3, GitHub Actions (`actions/checkout`, `actions/setup-java`).

## Global Constraints

- JDK version: 21 (must match `pom.xml`'s `maven-compiler-plugin` `source`/`target`, both currently `21`).
- `arcadedb.version` must be a published Maven Central release, not a SNAPSHOT — target `26.8.1` (latest release per `https://repo1.maven.org/maven2/com/arcadedb/arcadedb-engine/maven-metadata.xml` at design time).
- CI workflow triggers only on `push` to `main` and `pull_request`, both scoped via `paths:` to `pom.xml`, `src/**`, and the workflow file itself.
- CI runs `mvn verify` (not `package`) so the already-configured `license-maven-plugin` check goal actually executes.
- No build-artifact upload in this workflow — it is a verify gate only.
- Runner: `ubuntu-latest`, JDK distribution: `temurin`, with Maven dependency caching enabled (`cache: maven` in `actions/setup-java`).

---

### Task 1: Fix the `ArcadeDBPlatform` class name typo

**Files:**
- Modify: `src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java:39`

**Interfaces:**
- Consumes: nothing new.
- Produces: a public class named `ArcadeDBPlatform` in this file, matching the filename and matching the existing registration `science.atlarge.graphalytics.arcadedb.ArcadeDBPlatform` in `src/main/resources/arcadedb.platform` (already correct, not modified by this task).

- [ ] **Step 1: Confirm the current broken state**

Run: `mvn compile -q 2>&1 | tail -20`
Expected: FAIL with `class ArcadedbPlatform is public, should be declared in a file named ArcadedbPlatform.java`

- [ ] **Step 2: Fix the class declaration**

In `src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java`, change line 39 from:

```java
public class ArcadedbPlatform implements Platform {
```

to:

```java
public class ArcadeDBPlatform implements Platform {
```

- [ ] **Step 3: Verify the fix compiles**

Run: `mvn compile -q 2>&1 | tail -20`
Expected: no output (success) and exit code `0`. Confirm with `echo $?`.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/science/atlarge/graphalytics/arcadedb/ArcadeDBPlatform.java
git commit -m "Fix ArcadeDBPlatform class name typo

Commit 3cfd55a accidentally renamed the public class to
ArcadedbPlatform, which no longer matches the file name or the
registration in src/main/resources/arcadedb.platform, breaking
compilation entirely."
```

---

### Task 2: Bump `arcadedb.version` to a published release

**Files:**
- Modify: `pom.xml:19`

**Interfaces:**
- Consumes: Task 1's fix must be in place first, since this task's verification step (`mvn compile`) requires the project to compile.
- Produces: `arcadedb.version` property resolving to `26.8.1`, a version publicly available on Maven Central — required for Task 4's CI runner to resolve dependencies.

- [ ] **Step 1: Confirm the current pinned version is a local-only SNAPSHOT**

Run: `grep -n "arcadedb.version" pom.xml`
Expected: `<arcadedb.version>26.4.1-SNAPSHOT</arcadedb.version>` on line 19.

- [ ] **Step 2: Change the version property**

In `pom.xml`, change line 19 from:

```xml
		<arcadedb.version>26.4.1-SNAPSHOT</arcadedb.version>
```

to:

```xml
		<arcadedb.version>26.8.1</arcadedb.version>
```

- [ ] **Step 3: Force a fresh dependency resolution and compile**

Run: `mvn compile -q -U 2>&1 | tail -40`

(`-U` forces Maven to check remote repositories rather than reusing whatever is already cached locally, so this genuinely proves the new version resolves rather than silently reusing the old local SNAPSHOT jar.)

Expected: no errors, exit code `0`. If it fails with a dependency resolution error, `26.8.1` is no longer the latest release — re-check `https://repo1.maven.org/maven2/com/arcadedb/arcadedb-engine/maven-metadata.xml` for the current `<release>` value and use that instead.

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "Bump arcadedb.version to published release 26.8.1

26.4.1-SNAPSHOT only exists in local ~/.m2 caches from building the
arcadedb repo by hand; it is not published anywhere this project's
pom.xml can reach, so a clean checkout (including CI) cannot resolve
it. 26.8.1 is the latest Maven Central release."
```

---

### Task 3: Run full local verification, including the license-header check

**Files:**
- None (no code changes expected unless the license check fails).

**Interfaces:**
- Consumes: Task 1 and Task 2's fixes.
- Produces: local proof that `mvn verify` — the exact command Task 4's CI workflow will run — passes end to end, including the `license-maven-plugin` check that has never actually been exercised by this project's documented `mvn package -DskipTests` build command (that command stops at the `package` phase, before `verify`).

- [ ] **Step 1: Run the full verify lifecycle**

Run: `mvn verify -DskipTests 2>&1 | tee /tmp/mvn-verify-output.txt | tail -80`

- [ ] **Step 2: Check whether the license check passed**

Run: `grep -i "license" /tmp/mvn-verify-output.txt`

Two possible outcomes:

**Outcome A — license check passed** (output contains something like `Checking licenses...` with no `Unapproved licenses` / `ERROR` lines, and the overall build ended with `BUILD SUCCESS`): skip to Step 4.

**Outcome B — license check failed** (build ends `BUILD FAILURE` and the log lists specific files under an "Unapproved licenses" section): continue to Step 3.

- [ ] **Step 3 (only if Outcome B): Auto-format the missing headers**

Run: `mvn license:format -q`

This uses the same `<header>` template already configured in `pom.xml` (`https://graphalytics.org/assets/copyright-notice-template`) to insert the missing header into every file the check flagged. Then re-run:

Run: `mvn verify -DskipTests 2>&1 | tail -40`
Expected: `BUILD SUCCESS`.

If files were modified by `license:format`, stage and commit them:

```bash
git add -A
git status --short   # confirm only expected source files changed, nothing unrelated
git commit -m "Add missing license headers flagged by license-maven-plugin

The license:check goal is configured in pom.xml but was never
actually invoked by the documented mvn package -DskipTests build
command, since that command stops at the package phase before
verify. Running mvn verify for the first time surfaced these."
```

- [ ] **Step 4: Confirm final green build**

Run: `mvn verify -DskipTests 2>&1 | tail -20`
Expected: `BUILD SUCCESS`.

(No commit for this step if Outcome A — Task 1 and Task 2's commits already cover the fix, and there's nothing new to stage.)

---

### Task 4: Add the Java CI GitHub Actions workflow

**Files:**
- Create: `.github/workflows/java-ci.yml`

**Interfaces:**
- Consumes: nothing from earlier tasks at the code level — this is a standalone workflow file — but functionally depends on Tasks 1–3 being merged first, or the workflow will fail on its first run.
- Produces: a `build` job that other workflows/branch-protection rules can reference by job name if needed later.

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/java-ci.yml`:

```yaml
name: Java CI

on:
  push:
    branches:
      - main
    paths:
      - "pom.xml"
      - "src/**"
      - ".github/workflows/java-ci.yml"
  pull_request:
    paths:
      - "pom.xml"
      - "src/**"
      - ".github/workflows/java-ci.yml"

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
          cache: maven

      - name: Build and verify
        run: mvn --batch-mode --no-transfer-progress verify
```

- [ ] **Step 2: Validate the YAML is well-formed**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/java-ci.yml'))" && echo "valid YAML"`
Expected: `valid YAML`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/java-ci.yml
git commit -m "Add Java CI workflow

Runs mvn verify (compile + test + license check) on Temurin 21 /
ubuntu-latest for pushes to main and pull requests, scoped to
Java-relevant paths so it doesn't run on Python-only changes."
```

---

### Task 5: Validate the workflow actually runs green on GitHub

**Files:**
- None (validation only; no new file changes expected).

**Interfaces:**
- Consumes: Tasks 1–4, pushed to a branch GitHub Actions can see.

- [ ] **Step 1: Push the branch**

Run: `git push -u origin infra/setup-ci`

(Adjust the branch name if it differs from what's currently checked out — confirm with `git branch --show-current` first.)

- [ ] **Step 2: Open a pull request**

Run: `gh pr create --title "Add Java CI workflow" --body "$(cat <<'EOF'
## Summary
- Fixes a class-name typo that broke compilation entirely (ArcadedbPlatform -> ArcadeDBPlatform)
- Bumps arcadedb.version to the published 26.8.1 release so CI (and any clean checkout) can resolve dependencies
- Adds .github/workflows/java-ci.yml: mvn verify on Temurin 21 / ubuntu-latest, scoped to Java-relevant paths

## Test plan
- [ ] Confirm the "Java CI" check appears on this PR and passes
EOF
)"`

- [ ] **Step 3: Watch the check run**

Run: `gh pr checks --watch`
Expected: the `build` job from the `Java CI` workflow completes with a green/success status.

- [ ] **Step 4: If it fails, diagnose from the Actions log**

Run: `gh run list --workflow=java-ci.yml --limit 1` to get the run ID, then `gh run view <run-id> --log-failed` to see exactly which step failed. Common causes at this point would be an environment difference from local (e.g. a locale or timezone-sensitive test) rather than the dependency/compile issues already fixed in Tasks 1–3, since those were verified locally with the identical `mvn verify` command.

Do not merge until Step 3 shows a passing check.
