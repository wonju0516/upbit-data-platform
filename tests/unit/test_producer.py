"""ingest/producer.py의 순수 함수 테스트. 네트워크·Kafka를 쓰지 않는다."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))

import producer as pr  # noqa: E402

DAY = 86_400_000
# 2026-11-25 03:04:05 UTC
T = 1795575845000


class EnvelopeTest(unittest.TestCase):
    def test_payload_kept_byte_for_byte(self):
        raw = b'{"code":"KRW-BTC","sequential_id":17954866689740000,"trade_volume":6.681e-05}'
        out = pr.envelope("upbit_ws", "trade", "KRW-BTC", "producer-a", "c1", 123, raw)
        self.assertTrue(out.endswith(b',"payload":' + raw + b"}"))
        d = json.loads(out)
        self.assertEqual(d["payload"]["sequential_id"], 17954866689740000)
        self.assertEqual(
            {k: d[k] for k in ("source", "msg_type", "market", "producer_id", "connection_id")},
            {
                "source": "upbit_ws",
                "msg_type": "trade",
                "market": "KRW-BTC",
                "producer_id": "producer-a",
                "connection_id": "c1",
            },
        )
        self.assertEqual(d["received_at_ms"], 123)


class ClassifyFaultTest(unittest.TestCase):
    def c(self, reason, ext=True, up=True, pong=None, others=0):
        return pr.classify_fault(reason, ext, up, pong, others)

    def test_local_reasons(self):
        for r in ("PROCESS_RESTART", "PLANNED_STOP", "ERROR", "FORCED"):
            self.assertEqual(self.c(r), "LOCAL", r)

    def test_external_down_is_local(self):
        self.assertEqual(self.c("SILENCE", ext=False, up=False), "LOCAL")
        self.assertEqual(self.c("SERVER_CLOSE", ext=False), "LOCAL")

    def test_upbit_side(self):
        self.assertEqual(self.c("SERVER_CLOSE"), "UPBIT")
        self.assertEqual(self.c("SILENCE", pong=True), "UPBIT")
        self.assertEqual(self.c("CONN_LOST", up=False), "UPBIT")
        self.assertEqual(self.c("SILENCE", pong=False, others=2), "UPBIT")

    def test_unknown(self):
        self.assertEqual(self.c("SILENCE", pong=False), "UNKNOWN")
        self.assertEqual(self.c("CONN_LOST", ext=None, up=None), "UNKNOWN")


class TicksTest(unittest.TestCase):
    def test_params_today(self):
        p = pr.ticks_params("KRW-BTC", T, T + 60_000, None)
        self.assertEqual(p, {"market": "KRW-BTC", "count": 500, "to": "03:04:05"})

    def test_params_days_ago_and_cursor(self):
        p = pr.ticks_params("KRW-BTC", T - DAY, T, 42)
        self.assertEqual(p["days_ago"], 1)
        self.assertEqual(p["cursor"], 42)

    def test_in_window_uses_margin(self):
        rows = [{"timestamp": t} for t in (997, 1000, 1500, 2000, 2003)] + [{"x": 1}]
        got = pr.in_window(rows, 1000, 2000, margin_ms=2)
        self.assertEqual([r["timestamp"] for r in got], [1000, 1500, 2000])

    def test_need_more(self):
        full = [{"timestamp": 5000 + i} for i in range(500)]
        self.assertTrue(pr.need_more(full, 1000, margin_ms=0))
        self.assertFalse(pr.need_more(full, 6000, margin_ms=0))
        self.assertFalse(pr.need_more(full[:10], 1000, margin_ms=0))


class GapRecordTest(unittest.TestCase):
    def test_trade_recoverable_and_fault(self):
        r = pr.gap_record("p", "c", "trade", 1000, 31000, "SERVER_CLOSE", probe_external_ok=True)
        self.assertEqual(r["gap_ms"], 30000)
        self.assertTrue(r["recoverable"])
        self.assertEqual(r["fault_side"], "UPBIT")

    def test_orderbook_not_recoverable(self):
        r = pr.gap_record("p", "c", "orderbook", 0, 1, "PROCESS_RESTART")
        self.assertFalse(r["recoverable"])
        self.assertEqual(r["fault_side"], "LOCAL")


class IntervalTrackerTest(unittest.TestCase):
    def test_window_max_and_reset(self):
        it = pr.IntervalTracker(window_ms=1000)
        self.assertIsNone(it.add("trade", 0))
        it.add("trade", 300)
        it.reset("trade")  # 재연결: 그 사이 간격은 세지 않음
        it.add("trade", 900)
        done = it.add("trade", 1100)
        self.assertEqual(done, (0, "trade", 3, 300))


class SpoolTest(unittest.TestCase):
    def test_append_and_take(self):
        with tempfile.TemporaryDirectory() as d:
            s = pr.Spool(Path(d))
            s.append("t", b"KRW-BTC", b'{"a":"\xea\xb0\x80"}')
            s.append("t", None, b"{}")
            files = s.take()
            self.assertEqual(len(files), 1)
            rows = [json.loads(x) for x in files[0].read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0], {"topic": "t", "key": "KRW-BTC", "value": '{"a":"가"}'})
            self.assertIsNone(rows[1]["key"])
            self.assertEqual(list(Path(d).glob("*.jsonl")), [])
            self.assertEqual(s.take(), files)  # 못 보낸 .sending은 다음에 다시 가져감


if __name__ == "__main__":
    unittest.main()
