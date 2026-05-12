#!/usr/bin/env python3
"""Compare two sets of JMH JSON results and emit a markdown summary.

Used by `.github/workflows/benchmarks.yml` to diff the latest scheduled
`main` benchmark run against the run that just finished for a PR.

For each (benchmark, params) pair common to both runs we report two
metrics:

* `Time` — `primaryMetric.score`. In `SampleTime` mode this is the
  mean sampled latency per op; it's our best available proxy for CPU
  work since no dedicated CPU profiler is configured in
  `BenchmarkRunner`.
* `Alloc/op` — `secondaryMetrics["·gc.alloc.rate.norm"]`, populated by
  JMH's `GCProfiler`. This is bytes allocated per benchmark op and is
  the standard, low-noise JMH memory metric.

Both metrics are "lower is better", so a positive delta indicates the
PR is worse than the baseline. A run is considered failed when **any**
benchmark's worst metric delta exceeds `--threshold-pct` in the worse
direction. The script writes a `regressions=...`/`improvements=...`
summary file the workflow uses to set step outputs and decide whether
to fail the job.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

Key = Tuple[str, str]


# ---------------------------------------------------------------------------
# Metric model
# ---------------------------------------------------------------------------

# JMH's `GCProfiler` reports allocation rate normalised per op under this
# secondary metric key (the leading char is U+00B7 MIDDLE DOT, not a regular
# dot — that's JMH's convention for profiler-emitted metrics).
ALLOC_NORM_KEY = "\u00b7gc.alloc.rate.norm"


@dataclass(frozen=True)
class Metric:
    id: str
    label: str
    # Pull the `{score, scoreError, scoreUnit}`-shaped dict from a JMH record.
    extract: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    # True when a higher score is worse (regression). Both of our metrics
    # are lower-is-better so this is always True today, but the model
    # supports e.g. Throughput mode trivially.
    higher_is_worse: bool = True


def _primary(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pm = record.get("primaryMetric")
    return pm if isinstance(pm, dict) else None


def _secondary(record: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    sm = record.get("secondaryMetrics") or {}
    val = sm.get(key)
    return val if isinstance(val, dict) else None


METRICS: List[Metric] = [
    Metric(id="time", label="Time", extract=_primary),
    Metric(
        id="alloc",
        label="Alloc/op",
        extract=lambda r: _secondary(r, ALLOC_NORM_KEY),
    ),
]


# ---------------------------------------------------------------------------
# Loading & helpers
# ---------------------------------------------------------------------------


def load_results(directory: str) -> Dict[Key, Dict[str, Any]]:
    by_key: Dict[Key, Dict[str, Any]] = {}
    paths = sorted(
        glob.glob(os.path.join(directory, "**", "jmh-results-*.json"), recursive=True)
    )
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"warn: could not load {path}: {exc}", file=sys.stderr)
            continue
        if not isinstance(data, list):
            continue
        for record in data:
            bench = record.get("benchmark")
            if not bench:
                continue
            params = record.get("params") or {}
            param_str = ", ".join(f"{k}={params[k]}" for k in sorted(params))
            by_key[(bench, param_str)] = record
    return by_key


def _float(d: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    if not d:
        return None
    val = d.get(key)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def short_bench(name: str) -> str:
    parts = name.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else name


def fmt_score(v: Optional[float], err: Optional[float], unit: str) -> str:
    if v is None:
        return "—"
    body = f"{v:.3g} ± {err:.2g}" if err is not None else f"{v:.3g}"
    return f"{body} {unit}".rstrip()


def fmt_delta(d: Optional[float]) -> str:
    if d is None:
        return "—"
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.2f}%"


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


@dataclass
class MetricDelta:
    metric: Metric
    baseline: Optional[float]
    current: Optional[float]
    baseline_err: Optional[float]
    current_err: Optional[float]
    unit: str
    delta_pct: Optional[float]

    def regression(self, threshold: float) -> bool:
        if self.delta_pct is None:
            return False
        signed = self.delta_pct if self.metric.higher_is_worse else -self.delta_pct
        return signed > threshold

    def improvement(self, threshold: float) -> bool:
        if self.delta_pct is None:
            return False
        signed = self.delta_pct if self.metric.higher_is_worse else -self.delta_pct
        return signed < -threshold

    def cell(self) -> str:
        b = fmt_score(self.baseline, self.baseline_err, self.unit)
        c = fmt_score(self.current, self.current_err, self.unit)
        return f"{b} → {c} ({fmt_delta(self.delta_pct)})"


def metric_delta(metric: Metric, baseline_rec: Dict[str, Any], current_rec: Dict[str, Any]) -> MetricDelta:
    b = metric.extract(baseline_rec)
    c = metric.extract(current_rec)
    bs = _float(b, "score")
    cs = _float(c, "score")
    be = _float(b, "scoreError")
    ce = _float(c, "scoreError")
    unit = (c or {}).get("scoreUnit") or (b or {}).get("scoreUnit") or ""
    if bs is None or cs is None or bs == 0:
        delta_pct: Optional[float] = None
    else:
        delta_pct = (cs - bs) / bs * 100.0
    return MetricDelta(
        metric=metric,
        baseline=bs,
        current=cs,
        baseline_err=be,
        current_err=ce,
        unit=unit,
        delta_pct=delta_pct,
    )


@dataclass
class Row:
    key: Key
    deltas: List[MetricDelta]

    def worst_signed_pct(self) -> float:
        worst = 0.0
        for d in self.deltas:
            if d.delta_pct is None:
                continue
            signed = d.delta_pct if d.metric.higher_is_worse else -d.delta_pct
            if signed > worst:
                worst = signed
        return worst

    def sort_key(self) -> float:
        best = 0.0
        for d in self.deltas:
            if d.delta_pct is None:
                continue
            if abs(d.delta_pct) > best:
                best = abs(d.delta_pct)
        return best

    def status(self, threshold: float) -> str:
        regressed = [d for d in self.deltas if d.regression(threshold)]
        improved = [d for d in self.deltas if d.improvement(threshold)]
        if regressed:
            labels = ", ".join(d.metric.label for d in regressed)
            return f"REGRESSION ({labels})"
        if improved:
            labels = ", ".join(d.metric.label for d in improved)
            return f"improvement ({labels})"
        return ""


def build_rows(
    baseline: Dict[Key, Dict[str, Any]],
    current: Dict[Key, Dict[str, Any]],
) -> List[Row]:
    rows: List[Row] = []
    for key in sorted(set(current) & set(baseline)):
        b = baseline[key]
        c = current[key]
        deltas = [metric_delta(m, b, c) for m in METRICS]
        rows.append(Row(key=key, deltas=deltas))
    return rows


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def build_markdown(
    rows: List[Row],
    only_current: List[Key],
    only_baseline: List[Key],
    current: Dict[Key, Dict[str, Any]],
    *,
    threshold: float,
    repo: str,
    server_url: str,
    baseline_run_id: str,
    current_run_id: str,
) -> Tuple[str, int, int]:
    rows = sorted(rows, key=lambda r: r.sort_key(), reverse=True)
    regressions = sum(1 for r in rows if any(d.regression(threshold) for d in r.deltas))
    improvements = sum(
        1 for r in rows
        if not any(d.regression(threshold) for d in r.deltas)
        and any(d.improvement(threshold) for d in r.deltas)
    )

    out: List[str] = ["<!-- jmh-benchmark-comparison -->"]
    if regressions:
        out.append(f"## ❌ JMH benchmark comparison — {regressions} regression(s) over {threshold:g}%")
    elif improvements:
        out.append(f"## ✅ JMH benchmark comparison — {improvements} improvement(s) over {threshold:g}%")
    else:
        out.append(f"## JMH benchmark comparison — no changes over {threshold:g}%")
    out.append("")

    if repo and baseline_run_id and current_run_id:
        base_url = f"{server_url}/{repo}/actions/runs/{baseline_run_id}"
        curr_url = f"{server_url}/{repo}/actions/runs/{current_run_id}"
        out.append(
            f"Baseline: [`main` run #{baseline_run_id}]({base_url}) — "
            f"PR: [run #{current_run_id}]({curr_url})"
        )
        out.append("")

    out.append(
        f"Threshold: **±{threshold:g}%**. "
        f"Metrics: **Time** (`primaryMetric.score`, `SampleTime` — proxy for CPU work) and "
        f"**Alloc/op** (`{ALLOC_NORM_KEY}`, GC allocations per op — memory pressure). "
        "Both are lower-is-better, so a positive Δ% means the PR is worse than baseline."
    )
    out.append("")

    if rows:
        header = "| Benchmark | Params | " + " | ".join(m.label for m in METRICS) + " | Status |"
        sep = "|---|---|" + "|".join(["---"] * len(METRICS)) + "|---|"
        out.append(header)
        out.append(sep)
        for r in rows:
            bench, params = r.key
            cells = " | ".join(d.cell() for d in r.deltas)
            out.append(
                f"| `{short_bench(bench)}` | {params or '—'} | {cells} | {r.status(threshold)} |"
            )
        out.append("")
    else:
        out.append("_No benchmarks matched between baseline and PR._")
        out.append("")

    if only_current:
        out.append("<details><summary>Benchmarks only in PR run</summary>")
        out.append("")
        for k in only_current:
            bench, params = k
            rec = current[k]
            time_d = _primary(rec) or {}
            alloc_d = _secondary(rec, ALLOC_NORM_KEY) or {}
            out.append(
                f"- `{short_bench(bench)}` ({params or '—'}): "
                f"time={fmt_score(_float(time_d, 'score'), _float(time_d, 'scoreError'), time_d.get('scoreUnit', ''))}, "
                f"alloc={fmt_score(_float(alloc_d, 'score'), _float(alloc_d, 'scoreError'), alloc_d.get('scoreUnit', ''))}"
            )
        out.append("")
        out.append("</details>")
        out.append("")

    if only_baseline:
        out.append("<details><summary>Benchmarks only in baseline run</summary>")
        out.append("")
        for k in only_baseline:
            bench, params = k
            out.append(f"- `{short_bench(bench)}` ({params or '—'})")
        out.append("")
        out.append("</details>")
        out.append("")

    return "\n".join(out), regressions, improvements


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="Directory containing baseline JMH JSON files")
    parser.add_argument("--current", required=True, help="Directory containing current JMH JSON files")
    parser.add_argument("--baseline-run-id", default="", help="Baseline workflow run id (for links)")
    parser.add_argument("--current-run-id", default="", help="Current workflow run id (for links)")
    parser.add_argument("--repo", default="", help="owner/name for run links")
    parser.add_argument("--server-url", default="https://github.com")
    parser.add_argument("--output", required=True, help="Output markdown file")
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=10.0,
        help="Δ%% beyond which a metric is flagged as a regression / improvement (default: 10)",
    )
    parser.add_argument(
        "--summary-output",
        default="",
        help="Optional path to write a key=value summary the workflow can source",
    )
    args = parser.parse_args()

    if args.threshold_pct < 0:
        print("error: --threshold-pct must be non-negative", file=sys.stderr)
        return 2

    baseline = load_results(args.baseline)
    current = load_results(args.current)

    if not current:
        print("error: no current JMH result files found", file=sys.stderr)
        return 2

    rows = build_rows(baseline, current)
    only_current = sorted(set(current) - set(baseline))
    only_baseline = sorted(set(baseline) - set(current))

    md, regressions, improvements = build_markdown(
        rows,
        only_current,
        only_baseline,
        current,
        threshold=args.threshold_pct,
        repo=args.repo,
        server_url=args.server_url,
        baseline_run_id=args.baseline_run_id,
        current_run_id=args.current_run_id,
    )

    with open(args.output, "w", encoding="utf-8") as fh:
        fh.write(md)

    if args.summary_output:
        with open(args.summary_output, "w", encoding="utf-8") as fh:
            fh.write(f"regressions={regressions}\n")
            fh.write(f"improvements={improvements}\n")
            fh.write(f"matched={len(rows)}\n")
            fh.write(f"threshold_pct={args.threshold_pct}\n")

    print(
        f"wrote {args.output}: {len(rows)} matched, "
        f"{regressions} regression(s) > {args.threshold_pct:g}%, "
        f"{improvements} improvement(s), "
        f"{len(only_current)} only-PR, {len(only_baseline)} only-baseline"
    )
    # We always exit 0; the workflow uses the summary file to decide
    # whether to fail the job so that the comparison comment is still
    # posted on regressions.
    return 0


if __name__ == "__main__":
    sys.exit(main())
