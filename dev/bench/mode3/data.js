window.BENCHMARK_DATA = {
  "lastUpdate": 1793157533437,
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
        "date": 1788335336711,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 22.09,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.38,
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
        "date": 1788422220734,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 23.44,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.52,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.07,
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
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.19,
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
        "date": 1788508355235,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 28.99,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
        "date": 1788593688285,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.21,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.6,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.08,
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
            "value": 0.21,
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
        "date": 1788680839948,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.61,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
            "value": 0.19,
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
        "date": 1788768325633,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.28,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.62,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
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
            "value": 0.25,
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
        "date": 1788854425677,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.54,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.65,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.02,
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
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1788940944851,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.72,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.52,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
            "value": 0.2,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789027290759,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.99,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.57,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
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
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.2,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789113438725,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 24.23,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.48,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789199458385,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.3,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.64,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
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
            "value": 0.16,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789286969962,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.28,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789375422549,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.3,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.64,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.09,
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
            "value": 0.04,
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
            "value": 0.18,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789461136595,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.19,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.54,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.09,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789547234199,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 32.24,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.57,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.08,
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
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.04,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789633958556,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 19.44,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.35,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.02,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789718865489,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.08,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.58,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
            "value": 0.16,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789804625505,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 23.73,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.05,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789892748280,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.23,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.1,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1789980357388,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.38,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
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
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790065439683,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.95,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.58,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790151961346,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 28.11,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.56,
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
            "value": 0.19,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790237817272,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 17.37,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.28,
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
            "value": 0.13,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790325655337,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 21.76,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790411258662,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.63,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.6,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
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
            "value": 0.22,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790499821137,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.29,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.11,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.07,
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
            "value": 0.19,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790587878498,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.17,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.62,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
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
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790674639374,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.99,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.62,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
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
            "value": 0.18,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790760493395,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.39,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.6,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790848546363,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.2,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.06,
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
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
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
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff",
          "message": "Bump actions/setup-java from 5.7.0 to 6.0.0 (#27)\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2026-09-08T08:35:44Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/02e1d824a2ff1bfc1574bbdda7cb9b3c2344bfff"
        },
        "date": 1790933453732,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 30.57,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.64,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.1,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.09,
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
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "cdbda3259bee365bd2e9077da9205dfd5b7aebd6",
          "message": "Updated results",
          "timestamp": "2026-10-03T04:37:25Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/cdbda3259bee365bd2e9077da9205dfd5b7aebd6"
        },
        "date": 1791018158172,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 29.55,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.54,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.1,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.06,
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
            "value": 0.21,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "cdbda3259bee365bd2e9077da9205dfd5b7aebd6",
          "message": "Updated results",
          "timestamp": "2026-10-03T04:37:25Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/cdbda3259bee365bd2e9077da9205dfd5b7aebd6"
        },
        "date": 1791106235116,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 24.2,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.45,
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
            "value": 0.14,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "190ea994c16c1d00a3afb9623dd441ca6b78d032",
          "message": "Move the release-over-release history out of the README into ArcadeDB-release-progress.md\n\nThe README keeps the current multi-vendor comparison and links the history. The new file holds the\n26.8.1 to 26.10.1-SNAPSHOT tables unchanged plus the later merged fixes (WCC union-find #9133,\nrestored-view wait #9220/#9221) measured on AC power.",
          "timestamp": "2026-10-04T20:03:55Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/190ea994c16c1d00a3afb9623dd441ca6b78d032"
        },
        "date": 1791194988936,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 31.57,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.63,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.21,
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
          "id": "959d0e7758554be571e904d1b11240fbf8a83851",
          "message": "add maven central snapshot repo (#34)",
          "timestamp": "2026-10-14T08:22:28Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/959d0e7758554be571e904d1b11240fbf8a83851"
        },
        "date": 1791966585967,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 18.62,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.39,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.99,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 1.71,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.74,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.5,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.28,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.09,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "41898282+github-actions[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f262f36cd5211def3974ffd9e392b4ce388c31be",
          "message": "Bump actions/setup-java from 6.0.0 to 6.0.1 (#31) [skip ci]\n\nBumps [actions/setup-java](https://github.com/actions/setup-java) from 6.0.0 to 6.0.1.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/actions/setup-java/releases\">actions/setup-java's releases</a>.</em></p>\n<blockquote>\n<h2>v6.0.1</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Fix failing GitHub Actions job for temurin 17 by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> with <a href=\"https://github.com/Copilot\"><code>@​Copilot</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1259\">actions/setup-java#1259</a></li>\n<li>Fix Temurin EA E2E signature verification by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> with <a href=\"https://github.com/Copilot\"><code>@​Copilot</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1260\">actions/setup-java#1260</a></li>\n<li>Fix Alpine GPG verification failures and support multiple keys by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1262\">actions/setup-java#1262</a></li>\n<li>Fix import-safe checks when scripts are run from a path with symlinks by <a href=\"https://github.com/otaconix\"><code>@​otaconix</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1265\">actions/setup-java#1265</a></li>\n<li>Avoid macOS GPG socket overflow on long runner paths by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> with <a href=\"https://github.com/Copilot\"><code>@​Copilot</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1266\">actions/setup-java#1266</a></li>\n<li>Complete v6 release highlights in README by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1254\">actions/setup-java#1254</a></li>\n<li>Group and slow routine Dependabot updates by <a href=\"https://github.com/brunoborges\"><code>@​brunoborges</code></a> in <a href=\"https://redirect.github.com/actions/setup-java/pull/1255\">actions/setup-java#1255</a></li>\n<li>chore(deps-dev): update eslint and globals by <a href=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot] in <a href=\"https://redirect.github.com/actions/setup-java/pull/1256\">actions/setup-java#1256</a></li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/otaconix\"><code>@​otaconix</code></a> made their first contribution in <a href=\"https://redirect.github.com/actions/setup-java/pull/1265\">actions/setup-java#1265</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a href=\"https://github.com/actions/setup-java/compare/v6.0.0...v6.0.1\">https://github.com/actions/setup-java/compare/v6.0.0...v6.0.1</a></p>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/actions/setup-java/commit/de7274f081f381c8f8158605e0321c36c376e2e6\"><code>de7274f</code></a> Avoid macOS GPG socket overflow on long runner paths (<a href=\"https://redirect.github.com/actions/setup-java/issues/1266\">#1266</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/134912a529122c8803d42fbefc4adb2bd177f986\"><code>134912a</code></a> Fix import-safe checks when scripts are run from a path with symlinks (<a href=\"https://redirect.github.com/actions/setup-java/issues/1265\">#1265</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/0781fc6af30d3e1f67f2d243034e1a15cf5f3677\"><code>0781fc6</code></a> Fix alpine failures by switching default back to only warn on verification fa...</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/4889c4aff54cecfc8d479c505d9893fd814df73f\"><code>4889c4a</code></a> Fix Temurin EA E2E signature verification (<a href=\"https://redirect.github.com/actions/setup-java/issues/1260\">#1260</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/8fd32400856216412ac77ad6dc67cca66ecffb93\"><code>8fd3240</code></a> [WIP] Fix failing GitHub Actions job for temurin 17 (<a href=\"https://redirect.github.com/actions/setup-java/issues/1259\">#1259</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/273229181560643261a71a1544c22304f2a40df8\"><code>2732291</code></a> chore(deps-dev): update eslint and globals (<a href=\"https://redirect.github.com/actions/setup-java/issues/1256\">#1256</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/1a8f22b7f7b115efae2d0dbe944753d561a53801\"><code>1a8f22b</code></a> chore: streamline Dependabot updates (<a href=\"https://redirect.github.com/actions/setup-java/issues/1255\">#1255</a>)</li>\n<li><a href=\"https://github.com/actions/setup-java/commit/85030b78b58b52f56c1ed3d0e893d2c93117274b\"><code>85030b7</code></a> docs: complete v6 release highlights (<a href=\"https://redirect.github.com/actions/setup-java/issues/1254\">#1254</a>)</li>\n<li>See full diff in <a href=\"https://github.com/actions/setup-java/compare/dd06d9cba3e5552c54d9f8ea23572deb30010f7c...de7274f081f381c8f8158605e0321c36c376e2e6\">compare view</a></li>\n</ul>\n</details>\n<br />",
          "timestamp": "2026-10-14T08:37:48Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/f262f36cd5211def3974ffd9e392b4ce388c31be"
        },
        "date": 1792051717414,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 19.43,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.04,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792137683600,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 18.18,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792222696947,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 14.75,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792310091345,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 19.23,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.04,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792399139785,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 18.54,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.06,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792484440673,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 13.87,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.01,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792571197288,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 19.95,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.02,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.06,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792657533717,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 20.35,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.02,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "committer": {
            "name": "Luca Garulli",
            "username": "lvca",
            "email": "lvca@users.noreply.github.com"
          },
          "id": "abc5b55ef7c4cbf170d6e78e077f423749692f1e",
          "message": "Using ArcadeDB 26.10.2",
          "timestamp": "2026-10-15T21:31:09Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/abc5b55ef7c4cbf170d6e78e077f423749692f1e"
        },
        "date": 1792743800140,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 19.96,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "41898282+github-actions[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "05691af13638f6dedd20a63f17616434448e4500",
          "message": "Bump actions/upload-artifact from 7.0.1 to 7.0.2 (#36) [skip ci]\n\nBumps [actions/upload-artifact](https://github.com/actions/upload-artifact) from 7.0.1 to 7.0.2.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/actions/upload-artifact/releases\">actions/upload-artifact's releases</a>.</em></p>\n<blockquote>\n<h2>v7.0.2</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Improves artifact download retries when the service returns HTTP 429 (rate limiting), including honoring valid Retry-After headers.</li>\n<li>Updates <code>@​actions/artifact</code> to v6.3.1.</li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/tunc-d\"><code>@​tunc-d</code></a> made their first contribution in <a href=\"https://redirect.github.com/actions/upload-artifact/pull/821\">actions/upload-artifact#821</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a href=\"https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2\">https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2</a></p>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/cf430e030ddbb5b0abf93d22962f4752f3646cd9\"><code>cf430e0</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/822\">#822</a> from actions/tunc-d-prepare-upload-v7-0-2</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/c2acdedd35a8824a1552c7137f8c18288c3ea469\"><code>c2acded</code></a> chore: prepare v7.0.2 release</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/4687c1bbaa0a58abb517d90b4bbade6754c80c1a\"><code>4687c1b</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/821\">#821</a> from actions/tunc-d-upload-artifact-package-bump</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/8cbe1bebdb858105d8fdd88f81404d4de4595719\"><code>8cbe1be</code></a> Bump <code>@​actions/artifact</code> to 6.3.1</li>\n<li>See full diff in <a href=\"https://github.com/actions/upload-artifact/compare/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a...cf430e030ddbb5b0abf93d22962f4752f3646cd9\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=actions/upload-artifact&package-manager=github_actions&previous-version=7.0.1&new-version=7.0.2)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-10-23T14:23:27Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/05691af13638f6dedd20a63f17616434448e4500"
        },
        "date": 1792828963790,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 18.91,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.05,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "41898282+github-actions[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "05691af13638f6dedd20a63f17616434448e4500",
          "message": "Bump actions/upload-artifact from 7.0.1 to 7.0.2 (#36) [skip ci]\n\nBumps [actions/upload-artifact](https://github.com/actions/upload-artifact) from 7.0.1 to 7.0.2.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/actions/upload-artifact/releases\">actions/upload-artifact's releases</a>.</em></p>\n<blockquote>\n<h2>v7.0.2</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Improves artifact download retries when the service returns HTTP 429 (rate limiting), including honoring valid Retry-After headers.</li>\n<li>Updates <code>@​actions/artifact</code> to v6.3.1.</li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/tunc-d\"><code>@​tunc-d</code></a> made their first contribution in <a href=\"https://redirect.github.com/actions/upload-artifact/pull/821\">actions/upload-artifact#821</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a href=\"https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2\">https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2</a></p>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/cf430e030ddbb5b0abf93d22962f4752f3646cd9\"><code>cf430e0</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/822\">#822</a> from actions/tunc-d-prepare-upload-v7-0-2</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/c2acdedd35a8824a1552c7137f8c18288c3ea469\"><code>c2acded</code></a> chore: prepare v7.0.2 release</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/4687c1bbaa0a58abb517d90b4bbade6754c80c1a\"><code>4687c1b</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/821\">#821</a> from actions/tunc-d-upload-artifact-package-bump</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/8cbe1bebdb858105d8fdd88f81404d4de4595719\"><code>8cbe1be</code></a> Bump <code>@​actions/artifact</code> to 6.3.1</li>\n<li>See full diff in <a href=\"https://github.com/actions/upload-artifact/compare/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a...cf430e030ddbb5b0abf93d22962f4752f3646cd9\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=actions/upload-artifact&package-manager=github_actions&previous-version=7.0.1&new-version=7.0.2)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-10-23T14:23:27Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/05691af13638f6dedd20a63f17616434448e4500"
        },
        "date": 1792916134156,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 15.21,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "41898282+github-actions[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "05691af13638f6dedd20a63f17616434448e4500",
          "message": "Bump actions/upload-artifact from 7.0.1 to 7.0.2 (#36) [skip ci]\n\nBumps [actions/upload-artifact](https://github.com/actions/upload-artifact) from 7.0.1 to 7.0.2.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/actions/upload-artifact/releases\">actions/upload-artifact's releases</a>.</em></p>\n<blockquote>\n<h2>v7.0.2</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Improves artifact download retries when the service returns HTTP 429 (rate limiting), including honoring valid Retry-After headers.</li>\n<li>Updates <code>@​actions/artifact</code> to v6.3.1.</li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/tunc-d\"><code>@​tunc-d</code></a> made their first contribution in <a href=\"https://redirect.github.com/actions/upload-artifact/pull/821\">actions/upload-artifact#821</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a href=\"https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2\">https://github.com/actions/upload-artifact/compare/v7.0.1...v7.0.2</a></p>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/cf430e030ddbb5b0abf93d22962f4752f3646cd9\"><code>cf430e0</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/822\">#822</a> from actions/tunc-d-prepare-upload-v7-0-2</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/c2acdedd35a8824a1552c7137f8c18288c3ea469\"><code>c2acded</code></a> chore: prepare v7.0.2 release</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/4687c1bbaa0a58abb517d90b4bbade6754c80c1a\"><code>4687c1b</code></a> Merge pull request <a href=\"https://redirect.github.com/actions/upload-artifact/issues/821\">#821</a> from actions/tunc-d-upload-artifact-package-bump</li>\n<li><a href=\"https://github.com/actions/upload-artifact/commit/8cbe1bebdb858105d8fdd88f81404d4de4595719\"><code>8cbe1be</code></a> Bump <code>@​actions/artifact</code> to 6.3.1</li>\n<li>See full diff in <a href=\"https://github.com/actions/upload-artifact/compare/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a...cf430e030ddbb5b0abf93d22962f4752f3646cd9\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=actions/upload-artifact&package-manager=github_actions&previous-version=7.0.1&new-version=7.0.2)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-10-23T14:23:27Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/05691af13638f6dedd20a63f17616434448e4500"
        },
        "date": 1792984966495,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 16.97,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.03,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.04,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9ef40c966b8c02daa691bc0a4bc1afc37c0cd3c4",
          "message": "Bump actions/setup-java from 6.0.1 to 6.1.0 (#37)\n\nBumps [actions/setup-java](https://github.com/actions/setup-java) from 6.0.1 to 6.1.0.\n- [Release notes](https://github.com/actions/setup-java/releases)\n- [Commits](https://github.com/actions/setup-java/compare/de7274f081f381c8f8158605e0321c36c376e2e6...b843ebf9b913146b38da2eaa7aeddf76074ae1ad)\n\n---\nupdated-dependencies:\n- dependency-name: actions/setup-java\n  dependency-version: 6.1.0\n  dependency-type: direct:production\n  update-type: version-update:semver-minor\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-10-26T17:50:24Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/9ef40c966b8c02daa691bc0a4bc1afc37c0cd3c4"
        },
        "date": 1793071347003,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 15.88,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.02,
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
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9ef40c966b8c02daa691bc0a4bc1afc37c0cd3c4",
          "message": "Bump actions/setup-java from 6.0.1 to 6.1.0 (#37)\n\nBumps [actions/setup-java](https://github.com/actions/setup-java) from 6.0.1 to 6.1.0.\n- [Release notes](https://github.com/actions/setup-java/releases)\n- [Commits](https://github.com/actions/setup-java/compare/de7274f081f381c8f8158605e0321c36c376e2e6...b843ebf9b913146b38da2eaa7aeddf76074ae1ad)\n\n---\nupdated-dependencies:\n- dependency-name: actions/setup-java\n  dependency-version: 6.1.0\n  dependency-type: direct:production\n  update-type: version-update:semver-minor\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-10-26T17:50:24Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/9ef40c966b8c02daa691bc0a4bc1afc37c0cd3c4"
        },
        "date": 1793157533166,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 16.76,
            "unit": "s"
          },
          {
            "name": "Q1",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q2",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q3",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q4",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q5",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q6",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q7",
            "value": 0.01,
            "unit": "s"
          },
          {
            "name": "Q8",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "Q9",
            "value": 0.03,
            "unit": "s"
          }
        ]
      }
    ]
  }
}