window.BENCHMARK_DATA = {
  "lastUpdate": 1794453305311,
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
        "date": 1788080787380,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 144.55,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.61,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.48,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.36,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 14.02,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.16,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.14,
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
        "date": 1788168511651,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 136.45,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.55,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.32,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.78,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.06,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.85,
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
        "date": 1788251227905,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 134.36,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.7,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.58,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.31,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.91,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.26,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.78,
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
        "date": 1788335333815,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 125.71,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 1.42,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.19,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.04,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.86,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.29,
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
        "date": 1788422217377,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 138.34,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.7,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.48,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.4,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.49,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.87,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.43,
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
        "date": 1788508353120,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 140.24,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.61,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.46,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.17,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.22,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.28,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.34,
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
        "date": 1788593685614,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 135.92,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.57,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.46,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.24,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.47,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.92,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.39,
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
        "date": 1788680837129,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 113.18,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.39,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.29,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.1,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.3,
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
        "date": 1788768322715,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 130,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.6,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.4,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.17,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.44,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.52,
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
        "date": 1788854422006,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 114.71,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 1.09,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.49,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.24,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.69,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.25,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.59,
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
        "date": 1788940941910,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 130.91,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.6,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.23,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.02,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.69,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.66,
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
        "date": 1789027287564,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 135.47,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.74,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.57,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.74,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.8,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.19,
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
        "date": 1789113435717,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 134.02,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.55,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.39,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.1,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.68,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.82,
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
        "date": 1789199455650,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 127.25,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.8,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.38,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.28,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.8,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.47,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.65,
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
        "date": 1789286967945,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 109.47,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.31,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.46,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.62,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.71,
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
        "date": 1789375419715,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 132.66,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.75,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.46,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.24,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.38,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.91,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.23,
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
        "date": 1789461133303,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 131.82,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.64,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.4,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.01,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.41,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.71,
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
        "date": 1789547231703,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 113.96,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.59,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.26,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.98,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.77,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.32,
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
        "date": 1789633956247,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 130.28,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.61,
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
            "value": 11.31,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.39,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.62,
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
        "date": 1789718862527,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 139.4,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.65,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.18,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.53,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.26,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.19,
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
        "date": 1789804622545,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 131.79,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.53,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.49,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.13,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.89,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.53,
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
        "date": 1789892744885,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 84.83,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 1.03,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.28,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.16,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.99,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.6,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.84,
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
        "date": 1789980354608,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 146.26,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.73,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.79,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.27,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 14.96,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.49,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 10.37,
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
        "date": 1790065436702,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 134.56,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.59,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.21,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.52,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.71,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.09,
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
        "date": 1790151957874,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 145.32,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.7,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.26,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.63,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.5,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.49,
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
        "date": 1790237814179,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 133.02,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.58,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.66,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.21,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.27,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.67,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.32,
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
        "date": 1790325651788,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 132.7,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.57,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.49,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.37,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.03,
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
        "date": 1790411255418,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 131.79,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.64,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.53,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.08,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.8,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.04,
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
        "date": 1790499817455,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 143.3,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.72,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.76,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.24,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.26,
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
        "date": 1790587876182,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 140.57,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.76,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.56,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.28,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.06,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.12,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.34,
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
        "date": 1790674636044,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 143.95,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.62,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.39,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.34,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.69,
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
        "date": 1790760490664,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 136.64,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.15,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.45,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.18,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.41,
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
        "date": 1790848543563,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 137.62,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.5,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.35,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.7,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.07,
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
        "date": 1790933450603,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 133.4,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.57,
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
            "value": 11.86,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.81,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 9.34,
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
        "date": 1791018156296,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 118.86,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.53,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.62,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.22,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.06,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.99,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.4,
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
        "date": 1791106232894,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 134.72,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.63,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.59,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.24,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.48,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.54,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.81,
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
        "date": 1791194986652,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 132.13,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.75,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.55,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.17,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 12.25,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.51,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 8.84,
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
        "date": 1791966583505,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 108.79,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.46,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.32,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.44,
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
        "date": 1792051714009,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 68.91,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.23,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 7.44,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.42,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.28,
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
        "date": 1792137679653,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 117,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.51,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.21,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.03,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.24,
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
        "date": 1792222693672,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 104.51,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.35,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.18,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.58,
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
        "date": 1792310089031,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 87.15,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.27,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.88,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.04,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.86,
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
        "date": 1792399136042,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 120.75,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.54,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.34,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.18,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.95,
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
        "date": 1792484437539,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 98.95,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.41,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.35,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.83,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.63,
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
        "date": 1792571194248,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 113.19,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.94,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.73,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.93,
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
        "date": 1792657530470,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 118.88,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.76,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.78,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.1,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.74,
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
        "date": 1792743796163,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 80.76,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.27,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.16,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.19,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.76,
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
        "date": 1792828960793,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 117.3,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.7,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.08,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.06,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.53,
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
        "date": 1792916131581,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 120.37,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.26,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.06,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.97,
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
        "date": 1792984962562,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 101.26,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.05,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.93,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.36,
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
        "date": 1793071344579,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 82.33,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.26,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.8,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.85,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.72,
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
        "date": 1793157530129,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 125.25,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.13,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.59,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.45,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.6,
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
        "date": 1793243858837,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 115.8,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.1,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.72,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.87,
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
          "id": "c9b52eaecd1943fe90fefe2eaa2b369761f02fab",
          "message": "JVM memory probe: live heap and real footprint of ArcadeDB Docker and Neo4j under the fixed 12 GB heap\n\nBENCH_JVM_PROBE=1 runs jcmd inside the vendor's container before the first timed operation and after the\ntimed runs of each one: GC.class_histogram for the live heap (a full GC on any collector; GC.run is a no-op\non Neo4j, which starts with -XX:+DisableExplicitGC), VM.native_memory summary for the JVM's native memory,\nand docker stats for the container footprint (working set minus the unused part of the fixed heap).\nStored in result[\"_memory\"][\"jvm\"]. ARCADEDB_SERVER_FLAGS passes extra server settings.\n\nFirst results on datagen-7_5-fb (results-jvm-memory-2026-10-29.md): ArcadeDB 2.4-2.5 GiB with the default\npage cache, 1.2-1.25 GiB with maxPageRAM=512 (no change in compute time); Neo4j + GDS 1.9-2.0 GiB.",
          "timestamp": "2026-10-30T01:44:02Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c9b52eaecd1943fe90fefe2eaa2b369761f02fab"
        },
        "date": 1793330321221,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 123.04,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.58,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3.42,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.89,
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
          "id": "1ee2053afba59e3938e29a22df73268fb164cf2d",
          "message": "Stream ArcadeDB results as NDJSON; re-measure minimum RAM after ArcadeDB #11013\n\nEvery row-returning call of the ArcadeDB drivers (Graphalytics and LSQB) now reads the result as NDJSON\n(Accept: application/x-ndjson, shared/arcadedb_http.stream_rows): the buffered application/json response\nbuilds the whole result in the server heap (about 1 GB for a 2.4M-row output) and set the minimum heap of\ngraph500-22. The stream fails on a missing stats line, a mid-stream error or a truncated result. PROFILE,\nstatus checks and DDL stay on application/json. Checked: datagen outputs 6/6 valid with the published\ncompute times, LSQB 9/9 counts.\n\nMinimum-memory experiment switches: the driver stops when the view reports FAILED (with buildError);\nARCADEDB_CLEAN_STOP=1 stops the server gracefully so the view is persisted and reused; ARCADEDB_GC replaces\nZGC; BENCH_ALLOW_BATTERY=1 skips the AC wait (result still flagged); min_heap.py --compute-only passes on\nthe summary checks without the full-output export.\n\nResults (results-min-memory-11013-2026-10-30.md, chart docs/images/ram-arcadedb-neo4j-memgraph.svg),\ncontainer RAM for the full validated suite with default settings:\ndatagen-7_5-fb: ArcadeDB 2.24 GiB, Neo4j + GDS 2.59 GiB, Memgraph 25.63 GiB.\ngraph500-22: ArcadeDB 2.80 GiB, Neo4j + GDS 5.21 GiB, Memgraph out of memory at 32 GiB.\nAt rest on datagen: ArcadeDB 0.92 GiB, Neo4j 2.0 GiB. With a 128 MB page cache and a clean restart\nArcadeDB runs graph500-22 in 2.24 GiB; without the edge weights in the view, 1.46 GiB.",
          "timestamp": "2026-10-30T21:50:08Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/1ee2053afba59e3938e29a22df73268fb164cf2d"
        },
        "date": 1793416628438,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 116.73,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.47,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.77,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 3,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.89,
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
          "id": "1ee2053afba59e3938e29a22df73268fb164cf2d",
          "message": "Stream ArcadeDB results as NDJSON; re-measure minimum RAM after ArcadeDB #11013\n\nEvery row-returning call of the ArcadeDB drivers (Graphalytics and LSQB) now reads the result as NDJSON\n(Accept: application/x-ndjson, shared/arcadedb_http.stream_rows): the buffered application/json response\nbuilds the whole result in the server heap (about 1 GB for a 2.4M-row output) and set the minimum heap of\ngraph500-22. The stream fails on a missing stats line, a mid-stream error or a truncated result. PROFILE,\nstatus checks and DDL stay on application/json. Checked: datagen outputs 6/6 valid with the published\ncompute times, LSQB 9/9 counts.\n\nMinimum-memory experiment switches: the driver stops when the view reports FAILED (with buildError);\nARCADEDB_CLEAN_STOP=1 stops the server gracefully so the view is persisted and reused; ARCADEDB_GC replaces\nZGC; BENCH_ALLOW_BATTERY=1 skips the AC wait (result still flagged); min_heap.py --compute-only passes on\nthe summary checks without the full-output export.\n\nResults (results-min-memory-11013-2026-10-30.md, chart docs/images/ram-arcadedb-neo4j-memgraph.svg),\ncontainer RAM for the full validated suite with default settings:\ndatagen-7_5-fb: ArcadeDB 2.24 GiB, Neo4j + GDS 2.59 GiB, Memgraph 25.63 GiB.\ngraph500-22: ArcadeDB 2.80 GiB, Neo4j + GDS 5.21 GiB, Memgraph out of memory at 32 GiB.\nAt rest on datagen: ArcadeDB 0.92 GiB, Neo4j 2.0 GiB. With a 128 MB page cache and a clean restart\nArcadeDB runs graph500-22 in 2.24 GiB; without the edge weights in the view, 1.46 GiB.",
          "timestamp": "2026-10-30T21:50:08Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/1ee2053afba59e3938e29a22df73268fb164cf2d"
        },
        "date": 1793502980371,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 121.74,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.5,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 11.56,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.11,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.93,
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
          "id": "1ee2053afba59e3938e29a22df73268fb164cf2d",
          "message": "Stream ArcadeDB results as NDJSON; re-measure minimum RAM after ArcadeDB #11013\n\nEvery row-returning call of the ArcadeDB drivers (Graphalytics and LSQB) now reads the result as NDJSON\n(Accept: application/x-ndjson, shared/arcadedb_http.stream_rows): the buffered application/json response\nbuilds the whole result in the server heap (about 1 GB for a 2.4M-row output) and set the minimum heap of\ngraph500-22. The stream fails on a missing stats line, a mid-stream error or a truncated result. PROFILE,\nstatus checks and DDL stay on application/json. Checked: datagen outputs 6/6 valid with the published\ncompute times, LSQB 9/9 counts.\n\nMinimum-memory experiment switches: the driver stops when the view reports FAILED (with buildError);\nARCADEDB_CLEAN_STOP=1 stops the server gracefully so the view is persisted and reused; ARCADEDB_GC replaces\nZGC; BENCH_ALLOW_BATTERY=1 skips the AC wait (result still flagged); min_heap.py --compute-only passes on\nthe summary checks without the full-output export.\n\nResults (results-min-memory-11013-2026-10-30.md, chart docs/images/ram-arcadedb-neo4j-memgraph.svg),\ncontainer RAM for the full validated suite with default settings:\ndatagen-7_5-fb: ArcadeDB 2.24 GiB, Neo4j + GDS 2.59 GiB, Memgraph 25.63 GiB.\ngraph500-22: ArcadeDB 2.80 GiB, Neo4j + GDS 5.21 GiB, Memgraph out of memory at 32 GiB.\nAt rest on datagen: ArcadeDB 0.92 GiB, Neo4j 2.0 GiB. With a 128 MB page cache and a clean restart\nArcadeDB runs graph500-22 in 2.24 GiB; without the edge weights in the view, 1.46 GiB.",
          "timestamp": "2026-10-30T21:50:08Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/1ee2053afba59e3938e29a22df73268fb164cf2d"
        },
        "date": 1793589567168,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 115.37,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.44,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.09,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.15,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.84,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.96,
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
          "id": "1ee2053afba59e3938e29a22df73268fb164cf2d",
          "message": "Stream ArcadeDB results as NDJSON; re-measure minimum RAM after ArcadeDB #11013\n\nEvery row-returning call of the ArcadeDB drivers (Graphalytics and LSQB) now reads the result as NDJSON\n(Accept: application/x-ndjson, shared/arcadedb_http.stream_rows): the buffered application/json response\nbuilds the whole result in the server heap (about 1 GB for a 2.4M-row output) and set the minimum heap of\ngraph500-22. The stream fails on a missing stats line, a mid-stream error or a truncated result. PROFILE,\nstatus checks and DDL stay on application/json. Checked: datagen outputs 6/6 valid with the published\ncompute times, LSQB 9/9 counts.\n\nMinimum-memory experiment switches: the driver stops when the view reports FAILED (with buildError);\nARCADEDB_CLEAN_STOP=1 stops the server gracefully so the view is persisted and reused; ARCADEDB_GC replaces\nZGC; BENCH_ALLOW_BATTERY=1 skips the AC wait (result still flagged); min_heap.py --compute-only passes on\nthe summary checks without the full-output export.\n\nResults (results-min-memory-11013-2026-10-30.md, chart docs/images/ram-arcadedb-neo4j-memgraph.svg),\ncontainer RAM for the full validated suite with default settings:\ndatagen-7_5-fb: ArcadeDB 2.24 GiB, Neo4j + GDS 2.59 GiB, Memgraph 25.63 GiB.\ngraph500-22: ArcadeDB 2.80 GiB, Neo4j + GDS 5.21 GiB, Memgraph out of memory at 32 GiB.\nAt rest on datagen: ArcadeDB 0.92 GiB, Neo4j 2.0 GiB. With a 128 MB page cache and a clean restart\nArcadeDB runs graph500-22 in 2.24 GiB; without the edge weights in the view, 1.46 GiB.",
          "timestamp": "2026-10-30T21:50:08Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/1ee2053afba59e3938e29a22df73268fb164cf2d"
        },
        "date": 1793675889253,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 96.39,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.69,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.79,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.62,
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
          "id": "1ee2053afba59e3938e29a22df73268fb164cf2d",
          "message": "Stream ArcadeDB results as NDJSON; re-measure minimum RAM after ArcadeDB #11013\n\nEvery row-returning call of the ArcadeDB drivers (Graphalytics and LSQB) now reads the result as NDJSON\n(Accept: application/x-ndjson, shared/arcadedb_http.stream_rows): the buffered application/json response\nbuilds the whole result in the server heap (about 1 GB for a 2.4M-row output) and set the minimum heap of\ngraph500-22. The stream fails on a missing stats line, a mid-stream error or a truncated result. PROFILE,\nstatus checks and DDL stay on application/json. Checked: datagen outputs 6/6 valid with the published\ncompute times, LSQB 9/9 counts.\n\nMinimum-memory experiment switches: the driver stops when the view reports FAILED (with buildError);\nARCADEDB_CLEAN_STOP=1 stops the server gracefully so the view is persisted and reused; ARCADEDB_GC replaces\nZGC; BENCH_ALLOW_BATTERY=1 skips the AC wait (result still flagged); min_heap.py --compute-only passes on\nthe summary checks without the full-output export.\n\nResults (results-min-memory-11013-2026-10-30.md, chart docs/images/ram-arcadedb-neo4j-memgraph.svg),\ncontainer RAM for the full validated suite with default settings:\ndatagen-7_5-fb: ArcadeDB 2.24 GiB, Neo4j + GDS 2.59 GiB, Memgraph 25.63 GiB.\ngraph500-22: ArcadeDB 2.80 GiB, Neo4j + GDS 5.21 GiB, Memgraph out of memory at 32 GiB.\nAt rest on datagen: ArcadeDB 0.92 GiB, Neo4j 2.0 GiB. With a 128 MB page cache and a clean restart\nArcadeDB runs graph500-22 in 2.24 GiB; without the edge weights in the view, 1.46 GiB.",
          "timestamp": "2026-10-30T21:50:08Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/1ee2053afba59e3938e29a22df73268fb164cf2d"
        },
        "date": 1793762252958,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 116.92,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.43,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.1,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.27,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.86,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.88,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1793848743395,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 78.67,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.26,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.02,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.01,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.71,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.33,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1793935050115,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 108.6,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.42,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.07,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.58,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.03,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.39,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794021330795,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 113.3,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.68,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.06,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.92,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.43,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794107899834,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 84.88,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.27,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.06,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.25,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.72,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.58,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794194360309,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 113.71,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.8,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.08,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 13.11,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.95,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 7.35,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794280569779,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 118.34,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.05,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.1,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 10.27,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 2.88,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 6.95,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794367057314,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 105.44,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.45,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.03,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 9.87,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.93,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 5.84,
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
          "id": "c2cc29a6275a257a64da32ae0d5e6f554931d52a",
          "message": "Bump ladybug from 0.21.2 to 0.22.0 (#38) [skip ci]\n\nBumps [ladybug](https://github.com/LadybugDB/ladybug) from 0.21.2 to 0.22.0.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a href=\"https://github.com/LadybugDB/ladybug/releases\">ladybug's releases</a>.</em></p>\n<blockquote>\n<h2>Release 0.22.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>fix(storage): refresh node group scan state after a checkpoint by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1094\">LadybugDB/ladybug#1094</a></li>\n<li>fix(storage): validate disk array and overflow page indices by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1095\">LadybugDB/ladybug#1095</a></li>\n<li>fix(aggregate): return [] instead of null for collect() on empty/all-null input by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1100\">LadybugDB/ladybug#1100</a></li>\n<li>c_api: add lbug_value_create_blob function to C API by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1103\">LadybugDB/ladybug#1103</a></li>\n<li>fix(binder): allow implicit cast STRING -&gt; UUID (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1102\">#1102</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1106\">LadybugDB/ladybug#1106</a></li>\n<li>feat: minimal engine enablers for the GQL extension (incl. standalone-call rewrite batch fix) by <a href=\"https://github.com/chiangchenghsin-hash\"><code>@​chiangchenghsin-hash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1104\">LadybugDB/ladybug#1104</a></li>\n<li>fix(storage): harden checkpoint crash recovery by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1099\">LadybugDB/ladybug#1099</a></li>\n<li>fix(storage): harden checkpoint crash recovery (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1098\">#1098</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1107\">LadybugDB/ladybug#1107</a></li>\n<li>fix(storage): detect stale pre-0.21 string hash indexes loudly (Fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1092\">#1092</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1108\">LadybugDB/ladybug#1108</a></li>\n<li>fix(storage): don't zero CSR header and length-chunk buffers by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1114\">LadybugDB/ladybug#1114</a></li>\n<li>fix(storage): bounds-check disk array updates in release builds by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1115\">LadybugDB/ladybug#1115</a></li>\n<li>fix(csv): remove shadowed variable in dialect detection by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1124\">LadybugDB/ladybug#1124</a></li>\n<li>Grouped aggregate over variable-length paths without materializing walks by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1080\">LadybugDB/ladybug#1080</a></li>\n<li>fix(planner): range selectivity from observed column min/max by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1126\">LadybugDB/ladybug#1126</a></li>\n<li>fix(posix): query F_GETLK before close and split lock error messages by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1131\">LadybugDB/ladybug#1131</a></li>\n<li>fix(processor): keep the child's selection vector intact in hash join build by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1119\">LadybugDB/ladybug#1119</a></li>\n<li>fix(opt): skip grouped reachable-count rewrite with rel predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1133\">LadybugDB/ladybug#1133</a></li>\n<li>fix(storage): clean stale spill file lazily after lock is held by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1132\">LadybugDB/ladybug#1132</a></li>\n<li>fix(gds): emit single length-0 row when closed walks fail node predicate by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1134\">LadybugDB/ladybug#1134</a></li>\n<li>fix(storage): drop unused selVector capture in ColumnStats::updateMinMax by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1138\">LadybugDB/ladybug#1138</a></li>\n<li>fix(storage): keep rel scans on the CSR state they started with by <a href=\"https://github.com/kory-io\"><code>@​kory-io</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1116\">LadybugDB/ladybug#1116</a></li>\n<li>fix(test): make vfs + checkpoint tests work on Windows by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1139\">LadybugDB/ladybug#1139</a></li>\n<li>fix(planner): slim scalar-seeded chain for correlated PK equalities by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1125\">LadybugDB/ladybug#1125</a></li>\n<li>fix(storage): pad uncompressed flush buffer to allocated page range on disk by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1091\">LadybugDB/ladybug#1091</a></li>\n<li>fix(processor): don't re-materialize probe-side exprs as join payloads by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1137\">LadybugDB/ladybug#1137</a></li>\n<li>fix(processor): do not compact flat join keys' selection vectors on null by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1141\">LadybugDB/ladybug#1141</a></li>\n<li>fix(binder): allow aggregating a value derived from an earlier aggregate by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1118\">LadybugDB/ladybug#1118</a></li>\n<li>fix(wal): tag WAL records with their owning catalog and route replay per owner by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1113\">LadybugDB/ladybug#1113</a></li>\n<li>fix(fs): resolve UTF-8 paths as Unicode on Windows by <a href=\"https://github.com/es-alficody\"><code>@​es-alficody</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1144\">LadybugDB/ladybug#1144</a></li>\n<li>fix(storage): version column min/max stats as STORAGE_VERSION_48 by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1150\">LadybugDB/ladybug#1150</a></li>\n<li>fix(processor): null the current row when SET returns a NULL value (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1136\">#1136</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1146\">LadybugDB/ladybug#1146</a></li>\n<li>perf(wal): cache last owner catalog across replayed records by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1147\">LadybugDB/ladybug#1147</a></li>\n<li>feat(binder): fold arithmetic identities to expose index scans by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1151\">LadybugDB/ladybug#1151</a></li>\n<li>feat(function): add list_has_any for list overlap checks by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1154\">LadybugDB/ladybug#1154</a></li>\n<li>perf(transaction): resolve a node's local row with one local-table lookup by <a href=\"https://github.com/aikins01\"><code>@​aikins01</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1155\">LadybugDB/ladybug#1155</a></li>\n<li>fix(planner): resolve segfault in mapExpressionsScan when enable_plan_optimizer is false (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1142\">#1142</a>) by <a href=\"https://github.com/Tyagiquamar\"><code>@​Tyagiquamar</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1148\">LadybugDB/ladybug#1148</a></li>\n<li>chore(planner): add an explanatory comment by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1167\">LadybugDB/ladybug#1167</a></li>\n<li>fix(main): reset useInternalCatalogEntry on exception paths (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1162\">#1162</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1164\">LadybugDB/ladybug#1164</a></li>\n<li>feat(function): add list_min and list_max for list extrema by <a href=\"https://github.com/mvanhorn\"><code>@​mvanhorn</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1157\">LadybugDB/ladybug#1157</a></li>\n<li>fix(catalog): look up replayed macro drops in the macros set (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1163\">#1163</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1165\">LadybugDB/ladybug#1165</a></li>\n<li>fix(table): reset file-scan shared state on cached plan reuse (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1161\">#1161</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1168\">LadybugDB/ladybug#1168</a></li>\n<li>fix(prepared): rebind cached plans when catalog version changes (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1158\">#1158</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1169\">LadybugDB/ladybug#1169</a></li>\n<li>fix(wal): remove storage index holder when replaying DROP INDEX (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1152\">#1152</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1166\">LadybugDB/ladybug#1166</a></li>\n<li>fix(catalog): guard columnIDs against checkpoint vacuum race (fixes <a href=\"https://redirect.github.com/LadybugDB/ladybug/issues/1160\">#1160</a>) by <a href=\"https://github.com/adsharma\"><code>@​adsharma</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1170\">LadybugDB/ladybug#1170</a></li>\n<li>perf(binder): leave large constant lists unfolded by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1189\">LadybugDB/ladybug#1189</a></li>\n<li>fix(parquet): do not write field values for a NULL STRUCT by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1178\">LadybugDB/ladybug#1178</a></li>\n<li>fix(arrow): keep child arrays aligned for NULL nested values by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1179\">LadybugDB/ladybug#1179</a></li>\n<li>fix(storage): read ART primary-key indexes written by 0.17 by <a href=\"https://github.com/zahariash\"><code>@​zahariash</code></a> in <a href=\"https://redirect.github.com/LadybugDB/ladybug/pull/1180\">LadybugDB/ladybug#1180</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/4ca22377b85a33a10247410fe3944a5224b1169d\"><code>4ca2237</code></a> fix(storage): widen both zone-map bounds when a write exceeds both</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/d348c6cf738fe2c33573d82beabd4c0020371af3\"><code>d348c6c</code></a> fix(function): throw BinderException for non-path LENGTH() input</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/72838e6b7919c335be9858d23e5a9493b0960bd2\"><code>72838e6</code></a> fix(function): route scalar list-slice through STRING implicit cast</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/134dcda8c08c2d0ae5fdc93ae201b695067e588f\"><code>134dcda</code></a> fix(binder): map COPY FROM subquery columns by position</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/25098848238fdd2ab0b394a70da6cc425215fd7f\"><code>2509884</code></a> test(copy): add reproducer for COPY FROM subquery reusing a variable</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/b14e2f88ecde61746ac9849928a0926f1b657a9a\"><code>b14e2f8</code></a> perf(optimizer): mask Intersect's probe scans with its build-side keys</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/339bd6ddab190379f3f08eba21d87a337736890c\"><code>339bd6d</code></a> perf(processor): reuse Intersect's per-probe scratch buffers</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/7bcc47d3b3c10f36576ab79c608fe89ffbf27ccd\"><code>7bcc47d</code></a> fix(planner): cap a lookup join by its flat outer rows</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/a8739f514d36f44ec61ad93aefceb96a5afe248b\"><code>a8739f5</code></a> style(optimizer): say why the NULL-key walk passes other operators through</li>\n<li><a href=\"https://github.com/LadybugDB/ladybug/commit/09aff4df1b6159491675024e18b733a7df8eb4ff\"><code>09aff4d</code></a> perf(planner): return a hash join's conditions by const reference</li>\n<li>Additional commits viewable in <a href=\"https://github.com/LadybugDB/ladybug/compare/v0.21.2...v0.22.0\">compare view</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility score](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=ladybug&package-manager=pip&previous-version=0.21.2&new-version=0.22.0)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't alter it yourself. You can also trigger a rebase manually by commenting `@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits that have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all of the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop Dependabot creating any more for this major version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop Dependabot creating any more for this minor version (unless you reopen the PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop Dependabot creating any more for this dependency (unless you reopen the PR or upgrade to it yourself)\n\n\n</details>",
          "timestamp": "2026-11-04T08:14:47Z",
          "url": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb/commit/c2cc29a6275a257a64da32ae0d5e6f554931d52a"
        },
        "date": 1794453304944,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "LOAD",
            "value": 80.97,
            "unit": "s"
          },
          {
            "name": "PR",
            "value": 0.28,
            "unit": "s"
          },
          {
            "name": "WCC",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "BFS",
            "value": 0.04,
            "unit": "s"
          },
          {
            "name": "LCC",
            "value": 8.45,
            "unit": "s"
          },
          {
            "name": "SSSP",
            "value": 1.64,
            "unit": "s"
          },
          {
            "name": "CDLP",
            "value": 4.59,
            "unit": "s"
          }
        ]
      }
    ]
  }
}