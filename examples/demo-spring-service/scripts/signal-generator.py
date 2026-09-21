#!/usr/bin/env python3
"""Generate realistic and deliberately broken traffic for the IoT ingest API."""

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

PROFILES = {
    "mixed": {
        "valid": 55,
        "count": 5,
        "missing_key": 8,
        "invalid_key": 8,
        "malformed_json": 5,
        "missing_value": 5,
        "unknown_type": 5,
        "blank_device": 3,
        "wrong_content_type": 3,
        "wrong_method": 3,
    },
    "valid": {"valid": 100},
    "errors": {
        "missing_key": 15,
        "invalid_key": 15,
        "malformed_json": 15,
        "missing_value": 15,
        "unknown_type": 15,
        "blank_device": 10,
        "wrong_content_type": 8,
        "wrong_method": 7,
    },
    "auth": {"missing_key": 50, "invalid_key": 50},
    "payload": {
        "malformed_json": 25,
        "missing_value": 25,
        "unknown_type": 25,
        "blank_device": 25,
    },
    "storage": {"valid": 85, "count": 15},
}

EXPECTED_STATUS = {
    "valid": 202,
    "count": 200,
    "missing_key": 401,
    "invalid_key": 401,
    "malformed_json": 400,
    "missing_value": 400,
    "unknown_type": 400,
    "blank_device": 400,
    "wrong_content_type": 415,
    "wrong_method": 405,
}


@dataclass
class RequestSpec:
    method: str
    path: str
    body: Optional[bytes]
    headers: dict[str, str]


def valid_payload(device_count: int, location_count: int) -> dict[str, object]:
    signal_type = random.choice(list(SIGNAL_VALUES))
    value_factory, unit = SIGNAL_VALUES[signal_type]
    device_number = random.randint(1, device_count)
    location_number = (device_number - 1) % location_count + 1
    return {
        "deviceId": f"sensor-{device_number:03d}",
        "locationId": str(uuid.uuid5(LOCATION_NAMESPACE, f"location-{location_number:03d}")),
        "type": signal_type,
        "value": value_factory(),
        "unit": unit,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def request_for(
    scenario: str, api_key: str, device_count: int, location_count: int
) -> RequestSpec:
    payload = valid_payload(device_count, location_count)
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    method = "POST"
    path = "/api/v1/signals"

    if scenario == "count":
        return RequestSpec("GET", f"{path}/count", None, {"X-API-Key": api_key})
    if scenario == "missing_key":
        headers.pop("X-API-Key")
    elif scenario == "invalid_key":
        headers["X-API-Key"] = "definitely-invalid-token"
    elif scenario == "malformed_json":
        return RequestSpec(method, path, b'{"deviceId": "broken"', headers)
    elif scenario == "missing_value":
        payload.pop("value")
    elif scenario == "unknown_type":
        payload["type"] = "PLASMA"
    elif scenario == "blank_device":
        payload["deviceId"] = " "
    elif scenario == "wrong_content_type":
        headers["Content-Type"] = "text/plain"
    elif scenario == "wrong_method":
        method = "PUT"

    return RequestSpec(method, path, json.dumps(payload).encode(), headers)


def send(base_url: str, spec: RequestSpec, timeout: float) -> tuple[int, float, str]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{spec.path}",
        data=spec.body,
        headers=spec.headers,
        method=spec.method,
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
    parser.add_argument("--profile", choices=PROFILES, default="mixed")
    parser.add_argument("--rate", type=float, default=2.0, help="requests per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds; 0 runs until Ctrl-C")
    parser.add_argument("--devices", type=int, default=20, help="number of emulated devices")
    parser.add_argument("--locations", type=int, default=5, help="number of sensor locations")
    parser.add_argument("--timeout", type=float, default=3.0, help="request timeout in seconds")
    parser.add_argument("--seed", type=int, help="random seed for repeatable traffic")
    parser.add_argument("--quiet", action="store_true", help="only print periodic summaries")
    args = parser.parse_args()
    if args.rate <= 0 or args.devices <= 0 or args.locations <= 0 or args.duration < 0:
        parser.error("rate, devices, and locations must be positive; duration must be non-negative")
    return args


def main() -> int:
    args = parse_args()
    random.seed(args.seed)
    scenarios = list(PROFILES[args.profile])
    weights = list(PROFILES[args.profile].values())
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
        f"Generating {args.profile} traffic to {args.url} at {args.rate:g} req/s "
        f"({args.devices} devices across {args.locations} locations). Press Ctrl-C to stop."
    )

    while not stopped and (args.duration == 0 or time.monotonic() - started < args.duration):
        scenario = random.choices(scenarios, weights=weights, k=1)[0]
        status, latency_ms, detail = send(
            args.url,
            request_for(scenario, args.api_key, args.devices, args.locations),
            args.timeout,
        )
        expected = EXPECTED_STATUS[scenario]
        matched = status == expected
        results[f"status:{status}"] += 1
        results[f"scenario:{scenario}"] += 1
        results["matched" if matched else "unexpected"] += 1

        if not args.quiet:
            marker = "OK" if matched else "UNEXPECTED"
            suffix = "" if matched else f" response={detail!r}"
            print(
                f"{marker:10} {scenario:20} status={status:<3} "
                f"expected={expected:<3} latency={latency_ms:7.1f}ms{suffix}"
            )

        now = time.monotonic()
        if now >= next_summary:
            total = results["matched"] + results["unexpected"]
            statuses = " ".join(
                f"{key.removeprefix('status:')}={value}"
                for key, value in sorted(results.items())
                if key.startswith("status:")
            )
            print(f"SUMMARY total={total} unexpected={results['unexpected']} statuses[{statuses}]")
            next_summary = now + 10

        next_request += 1 / args.rate
        time.sleep(max(0, next_request - time.monotonic()))

    total = results["matched"] + results["unexpected"]
    print(f"Stopped after {time.monotonic() - started:.1f}s and {total} requests.")
    return 1 if results["unexpected"] else 0


if __name__ == "__main__":
    sys.exit(main())
