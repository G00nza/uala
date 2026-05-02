import json
import os
import sys
import tempfile
import time
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import load_test
from load_test import Config, SeedState, RequestResult


class FakeArgs:
    target = "http://localhost:8080"
    users = 100
    tweets_per_user = 10
    follows_per_user = 20
    max_vus = 50
    ramp_step = 10
    ramp_interval = 5
    duration = 60
    seed_only = False
    skip_seed = False
    seed_workers = 10


class TestConfig(unittest.TestCase):
    def test_from_args_parses_host_and_port(self):
        cfg = Config.from_args(FakeArgs())
        self.assertEqual(cfg.host, "localhost")
        self.assertEqual(cfg.port, 8080)
        self.assertEqual(cfg.users, 100)
        self.assertEqual(cfg.duration, 60)

    def test_from_args_default_port_80_when_missing(self):
        args = FakeArgs()
        args.target = "http://192.168.1.10"
        cfg = Config.from_args(args)
        self.assertEqual(cfg.port, 80)


class TestSeedState(unittest.TestCase):
    def test_save_and_load_roundtrip(self):
        state = SeedState(celebrity_ids=["c1", "c2"], user_ids=["u1", "u2", "u3"])
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            state.save(path)
            loaded = SeedState.load(path)
            self.assertEqual(loaded.celebrity_ids, ["c1", "c2"])
            self.assertEqual(loaded.user_ids, ["u1", "u2", "u3"])
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
