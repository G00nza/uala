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


class Reporter:
    """Imprime métricas en tiempo real cada `interval` segundos."""

    def __init__(self, collector: MetricsCollector, interval: int = 5) -> None:
        self._collector = collector
        self._interval = interval
        self._start = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 1)

    def _run(self) -> None:
        window_start = time.time()
        while not self._stop.wait(self._interval):
            self._collector.drain()
            now_mono = time.monotonic()
            now_wall = time.time()
            snap = self._collector.snapshot(since=window_start)
            window_start = now_wall
            if not snap:
                continue
            elapsed = int(now_mono - self._start)
            lats = snap["latencies"]
            rps = len(lats) / self._interval
            p50 = _percentile(lats, 50)
            p95 = _percentile(lats, 95)
            p99 = _percentile(lats, 99)
            err_pct = snap["error_rate"] * 100
            active = snap["active_vus"]
            total = len(self._collector.all_results())
            print(
                f"\n[t={elapsed}s] VUs: {active} | RPS: {rps:.0f} | "
                f"p50: {p50}ms | p95: {p95}ms | p99: {p99}ms | "
                f"errors: {err_pct:.1f}% | total: {total}",
                flush=True,
            )
            ep_parts = []
            for ep, stats in snap["by_endpoint"].items():
                ep_parts.append(f"{ep}: p95={stats['p95']}ms")
            if ep_parts:
                print("  " + "  ".join(ep_parts), flush=True)


def run_seed(config: "Config", state_path: str = "seed_state.json") -> "SeedState":
    state = SeedState()

    # 1. Crear 2 celebrities
    for i in range(2):
        conn = _new_conn(config)
        _, _, body = do_request(conn, "POST", "/users", {"username": f"celebrity_{i}_{int(time.time())}"})
        conn.close()
        state.celebrity_ids.append(json.loads(body)["id"])
    print(f"[seed] 2 celebrities creados: {state.celebrity_ids}")

    # 2. Crear N usuarios normales en paralelo
    created: List[Optional[str]] = [None] * config.users

    def _create_user(idx: int) -> None:
        conn = _new_conn(config)
        try:
            _, _, body = do_request(conn, "POST", "/users", {"username": f"user_{idx}_{int(time.time())}"})
            created[idx] = json.loads(body)["id"]
        finally:
            conn.close()

    print(f"[seed] Creando {config.users} usuarios...")
    with ThreadPoolExecutor(max_workers=config.seed_workers) as ex:
        list(ex.map(_create_user, range(config.users)))

    state.user_ids = [uid for uid in created if uid]
    print(f"[seed] {len(state.user_ids)} usuarios creados.")

    # 3. Crear follows: cada usuario sigue celebrities + subconjunto aleatorio
    def _create_follows(user_id: str) -> None:
        conn = _new_conn(config)
        try:
            for celeb_id in state.celebrity_ids:
                do_request(conn, "POST", "/follow", {"followee_id": celeb_id}, user_id=user_id)
            peers = random.sample(
                [uid for uid in state.user_ids if uid != user_id],
                min(config.follows_per_user, len(state.user_ids) - 1),
            )
            for peer_id in peers:
                do_request(conn, "POST", "/follow", {"followee_id": peer_id}, user_id=user_id)
        finally:
            conn.close()

    print(f"[seed] Creando follows ({config.follows_per_user} por usuario)...")
    with ThreadPoolExecutor(max_workers=config.seed_workers) as ex:
        list(ex.map(_create_follows, state.user_ids))

    # 4. Publicar tweets iniciales
    def _create_tweets(user_id: str) -> None:
        conn = _new_conn(config)
        try:
            for i in range(config.tweets_per_user):
                do_request(
                    conn, "POST", "/tweets",
                    {"content": f"seed tweet {i} by {user_id[:8]}"},
                    user_id=user_id,
                )
        finally:
            conn.close()

    print(f"[seed] Publicando {config.tweets_per_user} tweets por usuario...")
    with ThreadPoolExecutor(max_workers=config.seed_workers) as ex:
        list(ex.map(_create_tweets, state.user_ids))

    state.save(state_path)
    print(f"[seed] Completo. Estado guardado en {state_path}")
    return state


def write_results(
    collector: MetricsCollector,
    timestamp: str,
    output_dir: str = ".",
) -> None:
    collector.drain()
    results = collector.all_results()
    if not results:
        print("No results to write.")
        return

    csv_path = os.path.join(output_dir, f"results_{timestamp}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "endpoint", "latency_ms", "status_code", "vu_profile"],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    summary_path = os.path.join(output_dir, f"summary_{timestamp}.txt")
    total = len(results)
    errors = sum(1 for r in results if r.status_code >= 400)
    with open(summary_path, "w") as f:
        f.write(f"Total requests: {total}\n")
        f.write(f"Error rate:     {errors / total * 100:.1f}%\n\n")
        for ep in ("timeline", "tweets", "follow"):
            ep_results = [r for r in results if r.endpoint == ep]
            if not ep_results:
                continue
            lats = [r.latency_ms for r in ep_results]
            ep_errors = sum(1 for r in ep_results if r.status_code >= 400)
            timestamps = [r.timestamp for r in ep_results]
            duration_s = max(timestamps) - min(timestamps)
            rps = len(ep_results) / duration_s if duration_s > 0 else 0
            f.write(f"[{ep}]\n")
            f.write(f"  requests:   {len(ep_results)}\n")
            f.write(f"  p50:        {_percentile(lats, 50)}ms\n")
            f.write(f"  p95:        {_percentile(lats, 95)}ms\n")
            f.write(f"  p99:        {_percentile(lats, 99)}ms\n")
            f.write(f"  rps:        {rps:.1f}\n")
            f.write(f"  error_rate: {ep_errors / len(ep_results) * 100:.1f}%\n\n")

    print(f"\nResultados guardados en:\n  {csv_path}\n  {summary_path}")
