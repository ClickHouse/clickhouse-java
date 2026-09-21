#!/usr/bin/env python3
"""Generate variable-size reconciliation batches with occasional invalid requests."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


SIGNAL_VALUES = {
    "TEMPERATURE": (lambda: round(random.uniform(-10, 45), 2), "C"),
    "HUMIDITY": (lambda: round(random.uniform(10, 95), 2), "%"),
    "PRESSURE": (lambda: round(random.uniform(970, 1040), 2), "hPa"),
    "MOTION": (lambda: random.choice([0.0, 1.0]), ""),
    "GAS": (lambda: round(random.uniform(0, 500), 2), "ppm"),
    "BATTERY": (lambda: round(random.uniform(0, 100), 2), "%"),
    "LIGHT": (lambda: round(random.uniform(0, 10000), 2), "lux"),
}

LOCATION_NAMESPACE = uuid.UUID("7c3f6c4e-3cb5-4a57-a93e-8a98cc81a84c")

ERROR_SCENARIOS = (
    "missing_key",
    "invalid_key",
    "malformed_json",
    "missing_value",
    "unknown_type",
    "blank_device",
    "wrong_content_type",
)

EXPECTED_STATUS = {
    "valid": 202,
    "missing_key": 401,
    "invalid_key": 401,
    "malformed_json": 400,
    "missing_value": 400,
    "unknown_type": 400,
    "blank_device": 400,
    "wrong_content_type": 415,
}


@dataclass
class RequestSpec:
    scenario: str
    batch_size: int
    body: bytes
    headers: dict[str, str]


def valid_signal(device_count: int, location_count: int) -> dict[str, object]:
    signal_type = random.choice(list(SIGNAL_VALUES))
    value_factory, unit = SIGNAL_VALUES[signal_type]
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    device_number = random.randint(1, device_count)
    location_number = (device_number - 1) % location_count + 1
    return {
        "signalId": str(uuid.uuid4()),
        "deviceId": f"sensor-{device_number:03d}",
        "locationId": str(uuid.uuid5(LOCATION_NAMESPACE, f"location-{location_number:03d}")),
        "signalType": signal_type,
        "value": value_factory(),
        "unit": unit,
        "eventTime": now,
    }


def request_for(
    api_key: str,
    device_count: int,
    location_count: int,
    min_batch: int,
    max_batch: int,
    error_rate: float,
) -> RequestSpec:
    batch_size = random.randint(min_batch, max_batch)
    batch = [valid_signal(device_count, location_count) for _ in range(batch_size)]
    scenario = random.choice(ERROR_SCENARIOS) if random.random() < error_rate else "valid"
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}

    if scenario == "missing_key":
        headers.pop("X-API-Key")
    elif scenario == "invalid_key":
        headers["X-API-Key"] = "definitely-invalid-token"
    elif scenario == "malformed_json":
        return RequestSpec(scenario, batch_size, b'[{"deviceId":"broken"}', headers)
    elif scenario == "missing_value":
        random.choice(batch).pop("value")
    elif scenario == "unknown_type":
        random.choice(batch)["signalType"] = "PLASMA"
    elif scenario == "blank_device":
        random.choice(batch)["deviceId"] = " "
    elif scenario == "wrong_content_type":
        headers["Content-Type"] = "text/plain"

    return RequestSpec(scenario, batch_size, json.dumps(batch).encode(), headers)


def send(base_url: str, spec: RequestSpec, timeout: float) -> tuple[int, float, str]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/reconciliation",
        data=spec.body,
        headers=spec.headers,
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(200).decode(errors="replace")
            return response.status, (time.monotonic() - started) * 1000, body
    except urllib.error.HTTPError as error:
        body = error.read(200).decode(errors="replace")
        return error.code, (time.monotonic() - started) * 1000, body
    except urllib.error.URLError as error:
        return 0, (time.monotonic() - started) * 1000, str(error.reason)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080", help="API base URL")
    parser.add_argument("--api-key", default="dev-key-1", help="valid API key")
    parser.add_argument("--rate", type=float, default=1.0, help="batches per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds; 0 runs until Ctrl-C")
    parser.add_argument("--min-batch", type=int, default=1, help="minimum signals per batch")
    parser.add_argument("--max-batch", type=int, default=25, help="maximum signals per batch")
    parser.add_argument(
        "--error-rate",
        type=float,
        default=0.08,
        help="fraction of requests made deliberately invalid (0.0-1.0)",
    )
    parser.add_argument("--devices", type=int, default=50, help="number of emulated devices")
    parser.add_argument("--locations", type=int, default=5, help="number of sensor locations")
    parser.add_argument("--timeout", type=float, default=3.0, help="request timeout in seconds")
    parser.add_argument("--seed", type=int, help="random seed for repeatable traffic")
    parser.add_argument("--quiet", action="store_true", help="only print periodic summaries")
    args = parser.parse_args()

    if args.rate <= 0 or args.devices <= 0 or args.locations <= 0 or args.duration < 0:
        parser.error("rate, devices, and locations must be positive; duration must be non-negative")
    if args.min_batch <= 0 or args.max_batch < args.min_batch:
        parser.error("min-batch must be positive and no greater than max-batch")
    if not 0 <= args.error_rate <= 1:
        parser.error("error-rate must be between 0.0 and 1.0")
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
    next_request = started
    next_summary = started + 10
    print(
        f"Generating reconciliation batches to {args.url} at {args.rate:g} batch/s, "
        f"size {args.min_batch}-{args.max_batch}, error rate {args.error_rate:.1%}. "
        f"Using {args.devices} devices across {args.locations} locations. Press Ctrl-C to stop."
    )

    while not stopped and (args.duration == 0 or time.monotonic() - started < args.duration):
        spec = request_for(
            args.api_key,
            args.devices,
            args.locations,
            args.min_batch,
            args.max_batch,
            args.error_rate,
        )
        status, latency_ms, detail = send(args.url, spec, args.timeout)
        expected = EXPECTED_STATUS[spec.scenario]
        matched = status == expected
        results[f"status:{status}"] += 1
        results[f"scenario:{spec.scenario}"] += 1
        results["signals"] += spec.batch_size
        results["matched" if matched else "unexpected"] += 1

        if not args.quiet:
            marker = "OK" if matched else "UNEXPECTED"
            suffix = "" if matched else f" response={detail!r}"
            print(
                f"{marker:10} {spec.scenario:20} batch={spec.batch_size:<4} "
                f"status={status:<3} expected={expected:<3} "
                f"latency={latency_ms:7.1f}ms{suffix}"
            )

        now = time.monotonic()
        if now >= next_summary:
            requests = results["matched"] + results["unexpected"]
            statuses = " ".join(
                f"{key.removeprefix('status:')}={value}"
                for key, value in sorted(results.items())
                if key.startswith("status:")
            )
            print(
                f"SUMMARY requests={requests} signals={results['signals']} "
                f"unexpected={results['unexpected']} statuses[{statuses}]"
            )
            next_summary = now + 10

        next_request += 1 / args.rate
        time.sleep(max(0, next_request - time.monotonic()))

    requests = results["matched"] + results["unexpected"]
    print(
        f"Stopped after {time.monotonic() - started:.1f}s, "
        f"{requests} requests, and {results['signals']} generated signals."
    )
    return 1 if results["unexpected"] else 0


if __name__ == "__main__":
    sys.exit(main())
