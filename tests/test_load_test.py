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


from unittest.mock import MagicMock, patch
from load_test import do_request, _percentile


class TestDoRequest(unittest.TestCase):
    def _make_conn(self, status: int, body: bytes) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.status = status
        mock_resp.read.return_value = body
        conn = MagicMock()
        conn.getresponse.return_value = mock_resp
        return conn

    def test_get_request_no_body(self):
        conn = self._make_conn(200, b'{"tweets":[]}')
        latency, status, body = do_request(conn, "GET", "/timeline", user_id="abc-123")
        conn.request.assert_called_once_with(
            "GET", "/timeline", body=None,
            headers={"Content-Type": "application/json", "X-User-ID": "abc-123"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"tweets":[]}')
        self.assertGreaterEqual(latency, 0)

    def test_post_request_with_body(self):
        conn = self._make_conn(201, b'{"id":"xyz"}')
        latency, status, body = do_request(
            conn, "POST", "/tweets",
            body={"content": "hello"},
            user_id="abc-123",
        )
        args = conn.request.call_args
        self.assertEqual(args[0][0], "POST")
        self.assertEqual(args[0][1], "/tweets")
        self.assertIn(b"hello", args[1]["body"])
        self.assertEqual(status, 201)

    def test_no_user_id_omits_header(self):
        conn = self._make_conn(201, b'{"id":"u1"}')
        do_request(conn, "POST", "/users", body={"username": "alice"})
        headers = conn.request.call_args[1]["headers"]
        self.assertNotIn("X-User-ID", headers)


class TestPercentile(unittest.TestCase):
    def test_p50_of_sorted_list(self):
        data = list(range(1, 101))
        self.assertEqual(_percentile(data, 50), 50)

    def test_p95_of_sorted_list(self):
        data = list(range(1, 101))
        self.assertEqual(_percentile(data, 95), 95)

    def test_empty_returns_zero(self):
        self.assertEqual(_percentile([], 95), 0)

    def test_single_element(self):
        self.assertEqual(_percentile([42], 99), 42)


if __name__ == "__main__":
    unittest.main()
