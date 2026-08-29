window.BENCHMARK_DATA = {
  "lastUpdate": 1787997169627,
  "repoUrl": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb",
  "entries": {
    "ArcadeDB Mode 2 — Embedded (datagen-7_5-fb)": [
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
        "date": 1787685386225,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 141.95,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.59,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.15,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.39,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.39,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.21,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Roberto Franchini",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "bf9cf4e9c3dbc534e46064bdafd743be31a02cfb",
          "message": "Add CNAME for bench.arcadedb.com custom domain (#25)\n\nCommitted alongside the landing page so it's version-controlled and\nrepublished automatically every publish-benchmarks run, rather than\nrelying solely on GitHub's UI-managed file. Requires a DNS CNAME record\n(bench -> arcadedata.github.io) and enabling the custom domain in repo\nSettings -> Pages, both outside what this commit can do.\n\nCo-authored-by: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-08-25T19:32:52Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/bf9cf4e9c3dbc534e46064bdafd743be31a02cfb"
        },
        "date": 1787686589226,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 120.74,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 1.31,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.54,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.63,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.62,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.1,
            "unit": "s"
          }
        ]
      },
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
          "id": "01f00a2649a1a354ce3ee7cac66571d1905a9552",
          "message": "Merge branch 'main' of github.com:ArcadeData/ldbc_graphalytics_platforms_arcadedb",
          "timestamp": "2026-08-26T13:21:05Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/01f00a2649a1a354ce3ee7cac66571d1905a9552"
        },
        "date": 1787751198960,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 141.59,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.53,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.16,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.75,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.27,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.25,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Roberto Franchini",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a8608d741a96ffcd99fc74089af84ee46f03aa7f",
          "message": "Run the benchmark workflow daily instead of weekly (#26)\n\nCo-authored-by: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-08-26T14:01:57Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/a8608d741a96ffcd99fc74089af84ee46f03aa7f"
        },
        "date": 1787838923840,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 138.26,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.58,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.51,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.46,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.92,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.49,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.7,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Roberto Franchini",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a8608d741a96ffcd99fc74089af84ee46f03aa7f",
          "message": "Run the benchmark workflow daily instead of weekly (#26)\n\nCo-authored-by: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-08-26T14:01:57Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/a8608d741a96ffcd99fc74089af84ee46f03aa7f"
        },
        "date": 1787929370312,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 144.33,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.77,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.7,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 15.63,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.3,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 10.08,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Roberto Franchini",
            "username": "robfrank",
            "email": "ro.franchini@gmail.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a8608d741a96ffcd99fc74089af84ee46f03aa7f",
          "message": "Run the benchmark workflow daily instead of weekly (#26)\n\nCo-authored-by: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-08-26T14:01:57Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/a8608d741a96ffcd99fc74089af84ee46f03aa7f"
        },
        "date": 1787997168966,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 131.57,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.49,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.06,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.43,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.61,
            "unit": "s"
          }
        ]
      }
    ]
  }
}