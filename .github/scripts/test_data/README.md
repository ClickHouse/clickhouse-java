# `compare-jmh.py` test fixtures

Each subdirectory is a self-contained scenario for `compare-jmh.py`. The
layout is always:

```
<case>/
  baseline/jmh-results-baseline.json
  current/jmh-results-current.json
```

`compare-jmh.py` discovers result files by globbing for
`jmh-results-*.json` under the `--baseline` and `--current` directories,
so any filename starting with `jmh-results-` works.

JSON records mirror the structure produced by JMH 1.37's
`ResultFormatType.JSON`: an array of objects with `benchmark`, `params`,
`primaryMetric.{score,scoreError,scoreUnit}`, and optionally
`secondaryMetrics["gc.alloc.rate.norm"]`.

| Case | What it covers |
|---|---|
| `all_improvements` | Multiple benchmarks where both Time and Alloc/op fall well below the threshold; report should be all ⬆️ / 🟢 with no failure. |
| `all_regressions` | Multiple benchmarks where Time and/or Alloc/op rise well above the threshold; report should be ⬇️ / 🔴, script flags every row as `REGRESSION`, summary `regressions > 0`. |
| `mixed` | A blend of regressions, improvements, and within-noise rows including multiple variants of the same benchmark — verifies the bucket ordering and the param-discriminator (`[limit=…]`) logic. |
| `no_alloc` | Records with no `gc.alloc.rate.norm` at all; verifies the script falls back to Time-only and doesn't render `🔴`/`🟢` markers in the absence of the key (plus prints the diagnostic warning). |
| `noise_only` | All deltas are inside ±10%; report should be the ✅ "no changes" header and every row should carry the ➖ neutral arrow. |
| `only_in_pr` | A benchmark appears in `current` but not in `baseline`; verifies the "Benchmarks only in PR run" `<details>` block. |
| `only_in_baseline` | The mirror case — verifies the "Benchmarks only in baseline run" block. |
| `empty_intersection` | `baseline` and `current` contain different sets of benchmarks so no rows are matched; verifies the "_No benchmarks matched_" path. |

The companion runner `test_compare_jmh.py` exercises every case and
checks both the rendered markdown and the `--summary-output` counters.
