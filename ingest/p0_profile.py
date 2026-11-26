"""0단계 데이터 프로파일링: KRW 전체 마켓을 종류별 연결 3개로 받아 원본을 저장하고 지표를 잰다.

Kafka·Spark 없이 Python만 쓴다 (docs/07_단계별실행계획.md 0단계, 이슈 #5).

    .venv/bin/python ingest/p0_profile.py --seconds 60
    .venv/bin/python ingest/p0_profile.py --seconds 1800 --drop-at 900

- 원본: data/p0/<UTC 시각>/<종류>.tsv.gz  (한 줄 = 받은 시각 ms, 탭, 받은 JSON 그대로). git 밖.
- 지표: data/p0/<UTC 시각>/manifest.json, 그리고 원장 experiments/LEDGER.tsv 한 행.
- 업비트 요청 규칙(docs/09_결정현황.md 9-1절): Origin 헤더 없음, 재연결은 대기 시간을 늘려 가며.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import email.utils
import gzip
import json
import math
import sys
import time
import urllib.request
import uuid
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

import ledger  # noqa: E402

WS_URL = "wss://api.upbit.com/websocket/v1"
MARKETS_URL = "https://api.upbit.com/v1/market/all?is_details=true"
TYPES = ("trade", "orderbook", "ticker")
SCRIPT = "ingest/p0_profile.py"


# ---------- 순수 함수 (tests/unit/test_p0_profile.py) ----------


def krw_markets(markets: list[dict]) -> list[str]:
    """REST 마켓 목록에서 KRW 마켓 코드만 정렬해서 돌려준다."""
    return sorted(m["market"] for m in markets if m.get("market", "").startswith("KRW-"))


def subscribe_message(kind: str, codes: list[str], ticket: str) -> str:
    """업비트 WebSocket 구독 메시지 (종류 하나, 코드 전체)."""
    return json.dumps(
        [{"ticket": ticket}, {"type": kind, "codes": codes}, {"format": "DEFAULT"}],
        separators=(",", ":"),
    )


def percentile(values: list[float], q: float) -> float | None:
    """가장 가까운 순위 방식 백분위수. 값이 없으면 None."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(q / 100 * len(s)) - 1))
    return s[k]


class Stats:
    """종류별 메시지를 받으면서 지표를 모은다. 원본은 다시 읽지 않는다."""

    def __init__(self, start_ms: int):
        self.start_ms = start_ms
        self.count: Counter[str] = Counter()
        self.raw_bytes: Counter[str] = Counter()
        self.latency: dict[str, list[int]] = defaultdict(list)
        self.snapshots: Counter[str] = Counter()
        self.disconnects: Counter[str] = Counter()
        self.codes: dict[str, set[str]] = defaultdict(set)
        self.levels: Counter[int] = Counter()
        self.trade_keys: set[tuple[str, int]] = set()
        self.trade_dups = 0
        self.trade_minutes: dict[str, set[int]] = defaultdict(set)
        self.bad = 0

    def add(self, kind: str, received_ms: int, raw: bytes) -> None:
        self.count[kind] += 1
        self.raw_bytes[kind] += len(raw)
        try:
            msg = json.loads(raw)
        except ValueError:
            self.bad += 1
            return
        code = msg.get("code")
        if code:
            self.codes[kind].add(code)
        if msg.get("stream_type") == "SNAPSHOT":
            # 연결 직후 받는 '마지막 상태'라 시각이 오래됨. 지연에서 뺀다.
            self.snapshots[kind] += 1
        elif isinstance(msg.get("timestamp"), int):
            self.latency[kind].append(received_ms - msg["timestamp"])
        if kind == "orderbook":
            self.levels[len(msg.get("orderbook_units") or [])] += 1
        if kind == "trade" and code:
            key = (code, msg.get("sequential_id"))
            if key in self.trade_keys:
                self.trade_dups += 1
            self.trade_keys.add(key)
            if isinstance(msg.get("trade_timestamp"), int):
                self.trade_minutes[code].add(msg["trade_timestamp"] // 60000)

    def no_trade_minutes(self, codes: list[str], end_ms: int) -> dict:
        """측정 구간 안의 '꽉 찬 분' 중 체결이 없던 분 수 (마켓별 분포)."""
        first = -(-self.start_ms // 60000)  # 시작 뒤 첫 정각 분
        last = end_ms // 60000  # 끝나기 전 마지막 정각 분 (미포함)
        total = max(0, last - first)
        if total == 0:
            return {"complete_minutes": 0}
        empty = sorted(
            sum(1 for m in range(first, last) if m not in self.trade_minutes.get(c, ()))
            for c in codes
        )
        return {
            "complete_minutes": total,
            "median_per_market": percentile(empty, 50),
            "p90_per_market": percentile(empty, 90),
            "markets_all_empty": sum(1 for e in empty if e == total),
            "markets_none_empty": sum(1 for e in empty if e == 0),
        }

    def metrics(self, codes: list[str], end_ms: int, gz_bytes: dict[str, int]) -> dict:
        secs = max(1e-9, (end_ms - self.start_ms) / 1000)
        out: dict = {"markets_subscribed": len(codes), "seconds": round(secs, 1)}
        daily_gz = 0.0
        for k in TYPES:
            n = self.count[k]
            lat = self.latency[k]
            out[f"{k}_msgs"] = n
            out[f"{k}_msgs_per_s"] = round(n / secs, 2)
            out[f"{k}_bytes_per_msg"] = round(self.raw_bytes[k] / n, 1) if n else None
            out[f"{k}_gzip_ratio"] = (
                round(gz_bytes.get(k, 0) / self.raw_bytes[k], 4) if self.raw_bytes[k] else None
            )
            for q in (50, 95, 99):
                out[f"{k}_latency_ms_p{q}"] = percentile(lat, q)
            out[f"{k}_latency_ms_min"] = min(lat) if lat else None
            out[f"{k}_snapshots"] = self.snapshots[k]
            out[f"{k}_disconnects"] = self.disconnects[k]
            out[f"{k}_markets_seen"] = len(self.codes[k])
            daily_gz += gz_bytes.get(k, 0) / secs * 86400
        out["est_daily_gb_gzip"] = round(daily_gz / 1e9, 2)
        out["orderbook_levels"] = {str(k): v for k, v in sorted(self.levels.items())}
        out["trade_duplicates"] = self.trade_dups
        out["no_trade_minutes"] = self.no_trade_minutes(codes, end_ms)
        out["bad_json"] = self.bad
        return out


# ---------- 입출력 ----------


def fetch_markets() -> tuple[list[str], float | None]:
    """KRW 마켓 목록과 (서버 Date 헤더 − 내 시계) 초 단위 대략값."""
    req = urllib.request.Request(MARKETS_URL, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        body = json.load(r)
        date = r.headers.get("Date")
    offset = None
    if date:
        offset = round(email.utils.parsedate_to_datetime(date).timestamp() - time.time(), 1)
    return krw_markets(body), offset


class _Drop(Exception):
    """--drop-at으로 일부러 끊을 때."""


async def collect(kind, codes, stats, deadline, drop_at, gz_files):
    from websockets.asyncio.client import connect
    from websockets.exceptions import WebSocketException

    f = gz_files[kind]
    backoff = 1.0
    dropped = drop_at is None
    while time.monotonic() < deadline:
        try:
            async with connect(WS_URL, max_size=2**22, open_timeout=10, close_timeout=2) as ws:
                await ws.send(subscribe_message(kind, codes, str(uuid.uuid4())))
                backoff = 1.0
                while True:
                    now = time.monotonic()
                    if now >= deadline:
                        return
                    if not dropped and now >= drop_at:
                        dropped = True
                        await ws.close()
                        raise _Drop()
                    try:
                        raw = await asyncio.wait_for(ws.recv(decode=False), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    ms = time.time_ns() // 1_000_000
                    f.write(str(ms).encode() + b"\t" + raw + b"\n")
                    stats.add(kind, ms, raw)
        except (_Drop, WebSocketException, OSError, asyncio.TimeoutError) as e:
            stats.disconnects[kind] += 1
            print(f"[{kind}] 끊김: {type(e).__name__}, {backoff:.0f}초 뒤 재연결", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def run(seconds: int, drop_at_s: int | None, out_dir: Path) -> tuple[dict, dict]:
    codes, clock_offset = fetch_markets()
    print(f"KRW 마켓 {len(codes)}개, 시계 차이 약 {clock_offset}초", flush=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    gz_files = {k: gzip.open(out_dir / f"{k}.tsv.gz", "wb") for k in TYPES}
    start_ms = time.time_ns() // 1_000_000
    stats = Stats(start_ms)
    t0 = time.monotonic()
    deadline = t0 + seconds
    drop_at = t0 + drop_at_s if drop_at_s is not None else None
    try:
        await asyncio.gather(
            *(collect(k, codes, stats, deadline, drop_at, gz_files) for k in TYPES)
        )
    finally:
        for f in gz_files.values():
            f.close()
    # 받는 것은 deadline까지만이므로 비율은 설정한 길이로 나눈다 (닫는 대기 시간 제외).
    end_ms = start_ms + seconds * 1000
    gz_bytes = {k: (out_dir / f"{k}.tsv.gz").stat().st_size for k in TYPES}
    metrics = stats.metrics(codes, end_ms, gz_bytes)
    metrics["clock_offset_s_approx"] = clock_offset
    info = {"codes": codes, "start_ms": start_ms, "end_ms": end_ms}
    return metrics, info


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--drop-at", type=int, default=None, help="이 초에 연결을 한 번 일부러 끊음")
    ap.add_argument("--no-ledger", action="store_true", help="원장에 쓰지 않음 (동작 확인용)")
    a = ap.parse_args(argv)
    if a.drop_at is not None and not 0 < a.drop_at < a.seconds:
        ap.error("--drop-at은 0과 --seconds 사이")

    started = dt.datetime.now(dt.timezone.utc)
    out_dir = REPO / "data" / "p0" / started.strftime("%Y%m%d-%H%M%S")
    metrics, info = asyncio.run(run(a.seconds, a.drop_at, out_dir))

    ok = all(metrics[f"{k}_msgs"] > 0 for k in TYPES)
    config = {
        "seconds": a.seconds,
        "drop_at": a.drop_at,
        "types": list(TYPES),
        "markets": "KRW-ALL",
        "format": "DEFAULT",
    }
    manifest = out_dir / "manifest.json"
    manifest.write_text(
        json.dumps({"config": config, "metrics": metrics, **info}, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=1))
    if a.no_ledger:
        print("원장에 쓰지 않음 (--no-ledger)")
        return 0 if ok else 1
    sample = (
        f"KRW 전체 {metrics['markets_subscribed']}개, "
        f"{started:%Y-%m-%dT%H:%MZ}부터 {a.seconds}초"
        + (f", {a.drop_at}초에 끊기" if a.drop_at is not None else "")
    )
    rid = ledger.append_run(
        stage="P0",
        script=SCRIPT,
        config=config,
        sample=sample,
        status="ok" if ok else "fail",
        metrics=metrics,
        artifact=str(manifest.relative_to(REPO)),
        repo=REPO,
    )
    print(f"원장 기록: {rid} ({'ok' if ok else 'fail'})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
