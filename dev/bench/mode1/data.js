window.BENCHMARK_DATA = {
  "lastUpdate": 1788335331791,
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
      }
    ]
  }
}