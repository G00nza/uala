#!/usr/bin/env python3
"""Load test script for the Uala microblogging API. Requires Python 3.8+. No external deps."""
import argparse
import csv
import http.client
import json
import os
import queue
import random
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import List, Optional, Tuple


@dataclass
class Config:
    target: str
    host: str
    port: int
    users: int = 1000
    tweets_per_user: int = 100
    follows_per_user: int = 200
    max_vus: int = 500
    ramp_step: int = 50
    ramp_interval: int = 10
    duration: int = 600
    seed_only: bool = False
    skip_seed: bool = False
    seed_workers: int = 50

    @classmethod
    def from_args(cls, args) -> "Config":
        parsed = urllib.parse.urlparse(args.target)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return cls(
            target=args.target,
            host=parsed.hostname,
            port=port,
            users=args.users,
            tweets_per_user=args.tweets_per_user,
            follows_per_user=args.follows_per_user,
            max_vus=args.max_vus,
            ramp_step=args.ramp_step,
            ramp_interval=args.ramp_interval,
            duration=args.duration,
            seed_only=args.seed_only,
            skip_seed=args.skip_seed,
            seed_workers=args.seed_workers,
        )


@dataclass
class SeedState:
    celebrity_ids: List[str] = field(default_factory=list)
    user_ids: List[str] = field(default_factory=list)

    def save(self, path: str = "seed_state.json") -> None:
        with open(path, "w") as f:
            json.dump(asdict(self), f)

    @classmethod
    def load(cls, path: str = "seed_state.json") -> "SeedState":
        with open(path) as f:
            data = json.load(f)
        return cls(**data)


@dataclass
class RequestResult:
    timestamp: float
    endpoint: str      # "timeline" | "tweets" | "follow"
    latency_ms: int
    status_code: int
    vu_profile: str    # "always_on" | "cycler" | "one_shot" | "seed"


def _new_conn(config: "Config") -> http.client.HTTPConnection:
    return http.client.HTTPConnection(config.host, config.port, timeout=10)


def do_request(
    conn: http.client.HTTPConnection,
    method: str,
    path: str,
    body: Optional[dict] = None,
    user_id: Optional[str] = None,
) -> Tuple[int, int, bytes]:
    """Returns (latency_ms, status_code, response_body)."""
    headers = {"Content-Type": "application/json"}
    if user_id:
        headers["X-User-ID"] = user_id
    encoded = json.dumps(body).encode() if body else None
    t0 = time.monotonic()
    conn.request(method, path, body=encoded, headers=headers)
    resp = conn.getresponse()
    resp_body = resp.read()
    latency_ms = int((time.monotonic() - t0) * 1000)
    return latency_ms, resp.status, resp_body


def _percentile(data: List[int], p: int) -> int:
    if not data:
        return 0
    sorted_data = sorted(data)
    idx = max(0, int(len(sorted_data) * p / 100) - 1)
    return sorted_data[idx]


class MetricsCollector:
    def __init__(self) -> None:
        self._q: "queue.Queue[RequestResult]" = queue.Queue()
        self._results: List[RequestResult] = []
        self._lock = threading.Lock()
        self._active_vus = 0

    def add(self, result: RequestResult) -> None:
        self._q.put_nowait(result)

    def drain(self) -> None:
        while True:
            try:
                r = self._q.get_nowait()
                with self._lock:
                    self._results.append(r)
            except queue.Empty:
                break

    def set_active_vus(self, n: int) -> None:
        with self._lock:
            self._active_vus = n

    def snapshot(self, since: float = 0.0) -> dict:
        with self._lock:
            results = [r for r in self._results if r.timestamp >= since]
            active = self._active_vus
        if not results:
            return {}
        latencies = [r.latency_ms for r in results]
        errors = sum(1 for r in results if r.status_code >= 400)
        by_endpoint: dict = {}
        for ep in ("timeline", "tweets", "follow"):
            ep_lats = [r.latency_ms for r in results if r.endpoint == ep]
            if ep_lats:
                by_endpoint[ep] = {"p95": _percentile(ep_lats, 95)}
        return {
            "count": len(results),
            "latencies": latencies,
            "error_rate": errors / len(results),
            "by_endpoint": by_endpoint,
            "active_vus": active,
        }

    def all_results(self) -> List[RequestResult]:
        with self._lock:
            return list(self._results)
