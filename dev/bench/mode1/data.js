window.BENCHMARK_DATA = {
  "lastUpdate": 1790325649411,
  "repoUrl": "https://github.com/ArcadeData/ldbc_graphalytics_platforms_arcadedb",
  "entries": {
    "ArcadeDB Mode 1 — Official LDBC Graphalytics": [
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
        "date": 1787685383901,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 85.182,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.114,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 95.34,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.02,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 95.34,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.164,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 95.34,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.816,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 95.34,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.129,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 95.34,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 29.832,
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
        "date": 1787686586319,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 86.037,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.218,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 78.929,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.669,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 78.929,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.422,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 78.929,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 23.214,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 78.929,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.368,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 78.929,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 31.582,
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
        "date": 1787751195916,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 99.834,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.147,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 98.776,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 28.847,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 98.776,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 14.429,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 98.776,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 26.294,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 98.776,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 14.746,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 98.776,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 37.278,
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
        "date": 1787838919627,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 75.472,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 22.607,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 93.017,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 21.023,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 93.017,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.17,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 93.017,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 19.321,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 93.017,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 11.678,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 93.017,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 26.085,
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
        "date": 1787929366919,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 88.313,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.513,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 87.825,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.096,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 87.825,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.899,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 87.825,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 24.69,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 87.825,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.086,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 87.825,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.052,
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
        "date": 1787997165839,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 85.349,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.965,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 93.096,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 23.504,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 93.096,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.518,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 93.096,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 25.199,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 93.096,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.735,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 93.096,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 29.646,
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
        "date": 1788080784697,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 99.621,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 37.107,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 117.065,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 27.155,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 117.065,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 13.631,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 117.065,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 30.083,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 117.065,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 14.217,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 117.065,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 36.234,
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
        "date": 1788168508420,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 73.099,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 21.494,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 76.546,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 21.151,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 76.546,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.629,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 76.546,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 20.219,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 76.546,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 11.309,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 76.546,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 26.315,
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
        "date": 1788251225476,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 85.592,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.683,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 90.664,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.214,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 90.664,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.269,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 90.664,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.182,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 90.664,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.897,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 90.664,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 30.004,
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
        "date": 1788335330924,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 91.763,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.874,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 85.868,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.438,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 85.868,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.804,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 85.868,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 23.079,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 85.868,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.977,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 85.868,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 34.101,
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
        "date": 1788422214254,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 89.479,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.948,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 88.777,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.535,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 88.777,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 11.69,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 88.777,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.703,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 88.777,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.368,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 88.777,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 30.461,
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
        "date": 1788508350570,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 86.295,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.668,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 87.44,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.965,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 87.44,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.631,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 87.44,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 24.25,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 87.44,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.11,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 87.44,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 31.834,
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
        "date": 1788593682477,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 86.975,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.437,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 85.541,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.093,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 85.541,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 14.32,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 85.541,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.276,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 85.541,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.003,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 85.541,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 30.317,
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
        "date": 1788680834433,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 87.938,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 20.592,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 84.203,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.822,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 84.203,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.269,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 84.203,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.54,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 84.203,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.252,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 84.203,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.314,
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
        "date": 1788768319621,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 88.31,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.01,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 90.866,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.484,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 90.866,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.681,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 90.866,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 23.486,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 90.866,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.275,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 90.866,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.671,
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
        "date": 1788854418296,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 81.415,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 22.203,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 98.231,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 22.997,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 98.231,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.637,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 98.231,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 26.599,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 98.231,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.466,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 98.231,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 29.321,
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
        "date": 1788940938230,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 89.97,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.215,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 89.331,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 26.279,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 89.331,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 13.022,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 89.331,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.177,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 89.331,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.897,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 89.331,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.246,
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
        "date": 1789027284414,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 79.81,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 24.532,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 89.834,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 22.047,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 89.834,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.202,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 89.834,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.011,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 89.834,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 10.539,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 89.834,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 25.731,
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
        "date": 1789113432894,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 83.649,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 20.264,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 89.645,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.61,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 89.645,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.852,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 89.645,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 24.806,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 89.645,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.863,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 89.645,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 29.949,
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
        "date": 1789199452492,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 90.661,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.222,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 88.381,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 26.723,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 88.381,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.88,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 88.381,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 23.522,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 88.381,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.857,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 88.381,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.408,
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
        "date": 1789286965475,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 92.815,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 20.619,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 89.019,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.826,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 89.019,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.601,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 89.019,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 25.655,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 89.019,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.485,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 89.019,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.572,
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
        "date": 1789375416344,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 94.106,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 25.965,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 95.626,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 26.561,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 95.626,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.269,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 95.626,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.001,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 95.626,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.682,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 95.626,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 33.549,
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
        "date": 1789461130249,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 87.826,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 16.779,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 82.924,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.703,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 82.924,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.583,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 82.924,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 25.905,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 82.924,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.074,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 82.924,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.673,
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
        "date": 1789547228788,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 77.379,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 30.12,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 124.772,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 21.297,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 124.772,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 9.824,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 124.772,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.257,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 124.772,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 11.044,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 124.772,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 25.272,
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
        "date": 1789633953832,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 86.682,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.759,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 87.063,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.156,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 87.063,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.715,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 87.063,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.523,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 87.063,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 18.532,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 87.063,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 30.233,
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
        "date": 1789718859188,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 89.821,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 21.51,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 90.493,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.646,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 90.493,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.791,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 90.493,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 23.755,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 90.493,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 13.552,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 90.493,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 33.047,
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
        "date": 1789804619785,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 76.326,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 27.959,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 76.55,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 20.488,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 76.55,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.397,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 76.55,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 24.041,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 76.55,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 10.566,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 76.55,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 25.573,
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
        "date": 1789892740708,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 74.353,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 18.706,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 73.738,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 20.069,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 73.738,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 10.219,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 73.738,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 17.822,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 73.738,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 10.499,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 73.738,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 25.795,
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
        "date": 1789980352173,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 90.208,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 22.968,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 106.049,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 25.018,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 106.049,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.068,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 106.049,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.414,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 106.049,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.919,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 106.049,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 32.838,
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
        "date": 1790065433317,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 85.48,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.662,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 88.409,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 24.703,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 88.409,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.76,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 88.409,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.404,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 88.409,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.805,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 88.409,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 30.65,
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
        "date": 1790151954815,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 73.419,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 23.675,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 90.985,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 20.04,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 90.985,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 9.954,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 90.985,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 19.073,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 90.985,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.188,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 90.985,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 26.667,
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
        "date": 1790237811599,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 73.551,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 19.202,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 75.178,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 20.177,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 75.178,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 9.985,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 75.178,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 21.56,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 75.178,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 10.591,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 75.178,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 25.787,
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
        "date": 1790325648170,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "SSSP load",
            "value": 84.804,
            "unit": "s"
          },
          {
            "name": "SSSP processing",
            "value": 20.401,
            "unit": "s"
          },
          {
            "name": "LCC load",
            "value": 87.03,
            "unit": "s"
          },
          {
            "name": "LCC processing",
            "value": 23.83,
            "unit": "s"
          },
          {
            "name": "WCC load",
            "value": 87.03,
            "unit": "s"
          },
          {
            "name": "WCC processing",
            "value": 12.453,
            "unit": "s"
          },
          {
            "name": "BFS load",
            "value": 87.03,
            "unit": "s"
          },
          {
            "name": "BFS processing",
            "value": 22.422,
            "unit": "s"
          },
          {
            "name": "PR load",
            "value": 87.03,
            "unit": "s"
          },
          {
            "name": "PR processing",
            "value": 12.619,
            "unit": "s"
          },
          {
            "name": "CDLP load",
            "value": 87.03,
            "unit": "s"
          },
          {
            "name": "CDLP processing",
            "value": 29.777,
            "unit": "s"
          }
        ]
      }
    ]
  }
}