#!/usr/bin/env python3
"""Continuously query recent one-second signal slices."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass


LOCATION_NAMESPACE = uuid.UUID("7c3f6c4e-3cb5-4a57-a93e-8a98cc81a84c")


@dataclass
class QueryResult:
    status: int
    latency_ms: float
    row_count: int
    detail: str


def location_id(location_number: int) -> str:
    return str(uuid.uuid5(LOCATION_NAMESPACE, f"location-{location_number:03d}"))


def send(
    base_url: str,
    api_key: str,
    lookback: str,
    selected_location: str | None,
    timeout: float,
) -> QueryResult:
    parameters = {"lookback": lookback}
    if selected_location is not None:
        parameters["locationId"] = selected_location
    url = (
        f"{base_url.rstrip('/')}/api/v1/signals/slices?"
        f"{urllib.parse.urlencode(parameters)}"
    )
    request = urllib.request.Request(url, headers={"X-API-Key": api_key}, method="GET")
    started = time.monotonic()

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            latency_ms = (time.monotonic() - started) * 1000
            rows = json.loads(body)
            if not isinstance(rows, list):
                return QueryResult(response.status, latency_ms, 0, "response is not a JSON array")
            return QueryResult(response.status, latency_ms, len(rows), "")
    except urllib.error.HTTPError as error:
        detail = error.read(200).decode(errors="replace")
        return QueryResult(error.code, (time.monotonic() - started) * 1000, 0, detail)
    except (urllib.error.URLError, TimeoutError) as error:
        detail = str(getattr(error, "reason", error))
        return QueryResult(0, (time.monotonic() - started) * 1000, 0, detail)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        return QueryResult(200, (time.monotonic() - started) * 1000, 0, str(error))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080", help="API base URL")
    parser.add_argument("--api-key", default="dev-key-1", help="valid API key")
    parser.add_argument("--rate", type=float, default=2.0, help="queries per second")
    parser.add_argument("--duration", type=float, default=60, help="seconds; 0 runs until Ctrl-C")
    parser.add_argument(
        "--lookbacks",
        default="10s,1m,10m,1h",
        help="comma-separated lookbacks selected randomly per query",
    )
    parser.add_argument(
        "--locations",
        type=int,
        default=5,
        help="number of stable generated locations available for filtering",
    )
    parser.add_argument(
        "--location-filter-rate",
        type=float,
        default=0.7,
        help="fraction of queries filtered to one location (0.0-1.0)",
    )
    parser.add_argument("--timeout", type=float, default=3.0, help="request timeout in seconds")
    parser.add_argument("--seed", type=int, help="random seed for repeatable queries")
    parser.add_argument("--quiet", action="store_true", help="only print periodic summaries")
    args = parser.parse_args()

    args.lookbacks = [value.strip() for value in args.lookbacks.split(",") if value.strip()]
    if args.rate <= 0 or args.duration < 0 or args.locations <= 0:
        parser.error("rate and locations must be positive; duration must be non-negative")
    if not args.lookbacks:
        parser.error("at least one lookback is required")
    if not 0 <= args.location_filter_rate <= 1:
        parser.error("location-filter-rate must be between 0.0 and 1.0")
    return args


def main() -> int:
    args = parse_args()
    random.seed(args.seed)
    results: Counter[str] = Counter()
    stopped = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    started = time.monotonic()
    next_query = started
    next_summary = started + 10
    print(
        f"Querying signal slices at {args.rate:g} req/s for "
        f"{'unlimited time' if args.duration == 0 else f'{args.duration:g}s'}, "
        f"using lookbacks {','.join(args.lookbacks)}. Press Ctrl-C to stop."
    )

    while not stopped and (args.duration == 0 or time.monotonic() - started < args.duration):
        lookback = random.choice(args.lookbacks)
        selected_location = None
        if random.random() < args.location_filter_rate:
            selected_location = location_id(random.randint(1, args.locations))

        result = send(args.url, args.api_key, lookback, selected_location, args.timeout)
        matched = result.status == 200 and not result.detail
        results["requests"] += 1
        results["rows"] += result.row_count
        results["latency_micros"] += round(result.latency_ms * 1000)
        results["matched" if matched else "unexpected"] += 1
        results[f"status:{result.status}"] += 1

        if not args.quiet:
            marker = "OK" if matched else "UNEXPECTED"
            scope = selected_location or "all"
            suffix = "" if matched else f" response={result.detail!r}"
            print(
                f"{marker:10} lookback={lookback:<4} location={scope:<36} "
                f"status={result.status:<3} rows={result.row_count:<5} "
                f"latency={result.latency_ms:7.1f}ms{suffix}"
            )

        now = time.monotonic()
        if now >= next_summary:
            average_ms = results["latency_micros"] / results["requests"] / 1000
            print(
                f"SUMMARY requests={results['requests']} rows={results['rows']} "
                f"unexpected={results['unexpected']} avg_latency={average_ms:.1f}ms"
            )
            next_summary = now + 10

        next_query += 1 / args.rate
        time.sleep(max(0, next_query - time.monotonic()))

    average_ms = (
        results["latency_micros"] / results["requests"] / 1000
        if results["requests"]
        else 0
    )
    print(
        f"Stopped after {time.monotonic() - started:.1f}s: requests={results['requests']} "
        f"rows={results['rows']} unexpected={results['unexpected']} "
        f"avg_latency={average_ms:.1f}ms."
    )
    return 1 if results["unexpected"] else 0


if __name__ == "__main__":
    sys.exit(main())
