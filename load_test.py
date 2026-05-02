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
