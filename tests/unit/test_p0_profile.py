"""ingest/p0_profile.py의 순수 함수 테스트. 네트워크를 쓰지 않는다."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))

import p0_profile as p0  # noqa: E402

MIN = 60_000
START = 100 * MIN + 30_000  # 100분 30초


def _raw(**kw) -> bytes:
    return json.dumps(kw).encode()


class PureFunctionTest(unittest.TestCase):
    def test_krw_markets_filters_and_sorts(self):
        markets = [{"market": "KRW-ETH"}, {"market": "BTC-XRP"}, {"market": "KRW-BTC"}, {}]
        self.assertEqual(p0.krw_markets(markets), ["KRW-BTC", "KRW-ETH"])

    def test_subscribe_message(self):
        msg = json.loads(p0.subscribe_message("trade", ["KRW-BTC"], "t1"))
        self.assertEqual(
            msg, [{"ticket": "t1"}, {"type": "trade", "codes": ["KRW-BTC"]}, {"format": "DEFAULT"}]
        )

    def test_percentile(self):
        self.assertIsNone(p0.percentile([], 50))
        vals = list(range(1, 101))
        self.assertEqual(p0.percentile(vals, 50), 50)
        self.assertEqual(p0.percentile(vals, 99), 99)
        self.assertEqual(p0.percentile([7], 95), 7)


class StatsTest(unittest.TestCase):
    def test_counts_latency_snapshot_duplicates(self):
        st = p0.Stats(START)
        t = _raw(
            code="KRW-BTC",
            timestamp=START,
            trade_timestamp=START + MIN,
            sequential_id=1,
            stream_type="SNAPSHOT",
        )
        st.add("trade", START + 40, t)
        st.add("trade", START + 50, t)  # 재연결로 같은 체결이 다시 옴
        rt = _raw(code="KRW-BTC", timestamp=START, sequential_id=2, stream_type="REALTIME")
        st.add("trade", START + 70, rt)
        st.add("trade", START + 60, b"not json")
        ob = _raw(
            code="KRW-BTC", timestamp=START, orderbook_units=[{}] * 30, stream_type="REALTIME"
        )
        st.add("orderbook", START + 10, ob)
        self.assertEqual(st.count["trade"], 4)
        self.assertEqual(st.trade_dups, 1)
        self.assertEqual(st.bad, 1)
        self.assertEqual(st.snapshots["trade"], 2)
        self.assertEqual(st.latency["trade"], [70])  # SNAPSHOT은 지연에서 뺌
        self.assertEqual(st.levels, {30: 1})
        self.assertEqual(st.codes["trade"], {"KRW-BTC"})

    def test_no_trade_minutes_uses_complete_minutes_only(self):
        st = p0.Stats(START)
        # 꽉 찬 분: 101, 102, 103 (끝 104분 10초)
        st.add("trade", 0, _raw(code="KRW-BTC", trade_timestamp=101 * MIN + 5, sequential_id=1))
        st.add("trade", 0, _raw(code="KRW-BTC", trade_timestamp=103 * MIN, sequential_id=2))
        r = st.no_trade_minutes(["KRW-BTC", "KRW-XYZ"], 104 * MIN + 10_000)
        self.assertEqual(r["complete_minutes"], 3)
        self.assertEqual(r["markets_all_empty"], 1)  # KRW-XYZ
        self.assertEqual(r["markets_none_empty"], 0)  # KRW-BTC는 102분이 빔
        self.assertEqual(r["median_per_market"], 1)

    def test_metrics_shape(self):
        st = p0.Stats(START)
        st.add("ticker", START + 5, _raw(code="KRW-BTC", timestamp=START))
        m = st.metrics(["KRW-BTC"], START + 10_000, {"ticker": 50, "trade": 0, "orderbook": 0})
        self.assertEqual(m["markets_subscribed"], 1)
        self.assertEqual(m["ticker_msgs"], 1)
        self.assertEqual(m["trade_msgs"], 0)
        self.assertIsNone(m["trade_bytes_per_msg"])
        self.assertEqual(m["ticker_latency_ms_p50"], 5)
        self.assertEqual(m["no_trade_minutes"], {"complete_minutes": 0})
        json.dumps(m)  # 원장에 JSON으로 들어가야 함


def _trade(st, seq, ts, recv=None):
    st.add("trade", recv or ts, _raw(code="KRW-BTC", sequential_id=seq, trade_timestamp=ts))


class TwoCollectorTest(unittest.TestCase):
    def test_disconnect_gap_filled_by_next_message(self):
        st = p0.Stats(START)
        st.disconnected("trade", START + 1000, "ConnectionClosedError:1006")
        _trade(st, 1, START + 2000, recv=START + 2500)
        self.assertEqual(st.events[0]["gap_ms"], 1500)
        self.assertEqual(st.events[0]["reason"], "ConnectionClosedError:1006")
        self.assertEqual(st.pending, {})

    def test_compare_only_one_side_and_coverage(self):
        a, b = p0.Stats(START), p0.Stats(START)
        end = START + 120_000
        t = START + 60_000
        for st in (a, b):
            _trade(st, 1, t)  # 둘 다 받음
        a.disconnected("trade", t + 1000, "forced")  # A만 일부러 끊김
        _trade(b, 2, t + 1500)  # A가 끊긴 동안 B만 받음
        _trade(a, 3, t + 4000)  # A 다시 붙음
        _trade(b, 3, t + 4000)
        _trade(a, 9, START + 1000)  # 앞 10초 여유 구간이라 세지 않음
        b.disconnected("orderbook", t + 30_000, "ConnectionClosedError")
        a.disconnected("orderbook", t + 35_000, "ConnectionClosedError")
        r = p0.compare(a, b, START, end)
        self.assertEqual(r["trades_both"], 2)
        self.assertEqual(r["trades_only_a"], 0)
        self.assertEqual(r["trades_only_b"], 1)
        self.assertEqual(r["trades_covered_by_b_during_a_gaps"], 1)
        self.assertEqual(r["spontaneous_a"], 1)  # forced는 빼고 셈
        self.assertEqual(r["spontaneous_a_overlapping_b_10s"], 1)  # 5초 차이로 겹침


if __name__ == "__main__":
    unittest.main()
