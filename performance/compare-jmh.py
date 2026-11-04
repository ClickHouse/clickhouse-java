#!/usr/bin/env python3
"""Compare matching primary metrics in two JMH JSON result files."""

import argparse
import csv
import json
import sys


def result_key(result):
    params = tuple(sorted((result.get("params") or {}).items()))
    return result["benchmark"], params


def load_results(path):
    with open(path, encoding="utf-8") as source:
        results = json.load(source)
    return {result_key(result): result for result in results}


def display_name(key):
    benchmark, params = key
    if not params:
        return benchmark
    return "{} [{}]".format(
        benchmark, ", ".join("{}={}".format(name, value) for name, value in params)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compare point estimates from matching JMH JSON results."
    )
    parser.add_argument("baseline", help="baseline JMH JSON file")
    parser.add_argument("candidate", help="candidate JMH JSON file")
    args = parser.parse_args()

    baseline = load_results(args.baseline)
    candidate = load_results(args.candidate)
    shared = sorted(set(baseline).intersection(candidate))
    if not shared:
        parser.error("the files contain no matching benchmark and parameter combinations")

    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "benchmark",
            "mode",
            "unit",
            "baseline",
            "candidate",
            "candidate/baseline",
            "change_percent",
        ]
    )
    for key in shared:
        base_result = baseline[key]
        candidate_result = candidate[key]
        base_metric = base_result["primaryMetric"]
        candidate_metric = candidate_result["primaryMetric"]
        if base_result["mode"] != candidate_result["mode"]:
            raise ValueError("mode differs for {}".format(display_name(key)))
        if base_metric["scoreUnit"] != candidate_metric["scoreUnit"]:
            raise ValueError("score unit differs for {}".format(display_name(key)))

        base_score = float(base_metric["score"])
        candidate_score = float(candidate_metric["score"])
        if base_score == 0:
            raise ValueError("baseline score is zero for {}".format(display_name(key)))
        ratio = candidate_score / base_score
        writer.writerow(
            [
                display_name(key),
                base_result["mode"],
                base_metric["scoreUnit"],
                "{:.9g}".format(base_score),
                "{:.9g}".format(candidate_score),
                "{:.6f}".format(ratio),
                "{:+.3f}".format((ratio - 1.0) * 100.0),
            ]
        )

    baseline_only = len(set(baseline).difference(candidate))
    candidate_only = len(set(candidate).difference(baseline))
    if baseline_only or candidate_only:
        print(
            "warning: skipped {} baseline-only and {} candidate-only results".format(
                baseline_only, candidate_only
            ),
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
