window.BENCHMARK_DATA = {
  "lastUpdate": 1787685388826,
  "repoUrl": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb",
  "entries": {
    "ArcadeDB Mode 3 — LSQB Embedded (SF0.1)": [
      {
        "commit": {
          "author": {
            "name": "robfrank",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "committer": {
            "name": "robfrank",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "id": "0493ca2d1cb796dd425d2dfb6e0744a679b142d5",
          "message": "Fix benchmark-history findings: validate bench-data.json, add scripts CI\n\n- Validate mode1/mode2/mode3 bench-data.json is non-empty valid JSON\n  before it becomes the artifact, so an empty/invalid conversion result\n  fails the step instead of silently masquerading as valid data for the\n  publish-benchmarks hashFiles gate to find.\n- Add .github/workflows/scripts-ci.yml to run the .github/scripts unit\n  tests on push/PR, so future edits to the converters are covered by CI.\n- Add a test asserting convert() returns [] for an empty log, covering\n  the empty-result case that motivated the validation fix.\n- Pass step outcomes through env vars instead of interpolating\n  ${{ }} directly into the \"Fail if any mode regressed\" run: block.",
          "timestamp": "2026-08-25T18:40:00Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/0493ca2d1cb796dd425d2dfb6e0744a679b142d5"
        },
        "date": 1787685388514,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 22.8,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.15,
            "unit": "s"
          }
        ]
      }
    ]
  }
}