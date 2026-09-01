window.BENCHMARK_DATA = {
  "lastUpdate": 1788251231197,
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
        "date": 1787686592505,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 24.59,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.46,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.04,
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
            "value": 0.16,
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
        "date": 1787751201786,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 21.15,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.05,
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
            "value": 0.02,
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
            "value": 0.13,
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
        "date": 1787838927860,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 21.65,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.04,
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
            "value": 0.12,
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
        "date": 1787929373920,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.35,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.61,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.1,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.05,
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
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.17,
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
        "date": 1787997172351,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.95,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.61,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.11,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.05,
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
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.18,
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
        "date": 1788080789781,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 20.98,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.38,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.05,
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
        "date": 1788168515481,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 24.66,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.48,
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
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.15,
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
        "date": 1788251230862,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 27.03,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.55,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.2,
            "unit": "s"
          }
        ]
      }
    ]
  }
}