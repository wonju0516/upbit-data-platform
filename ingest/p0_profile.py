"""0단계 데이터 프로파일링: KRW 전체 마켓을 종류별 연결 3개로 받아 원본을 저장하고 지표를 잰다.

Kafka·Spark 없이 Python만 쓴다 (docs/07_단계별실행계획.md 0단계, 이슈 #5).

    .venv/bin/python ingest/p0_profile.py --seconds 60
    .venv/bin/python ingest/p0_profile.py --seconds 1800 --drop-at 900
    .venv/bin/python ingest/p0_profile.py --seconds 7200 --collectors 2 --drop-at 3600
    .venv/bin/python ingest/p0_profile.py --seconds 3600 --collectors 2 --silence-sec 5
    .venv/bin/python ingest/p0_profile.py --seconds 21600 --collectors 2 --silence-sec 5 \
        --stop-after-event-min 10

- 원본: data/p0/<UTC 시각>/<종류>.tsv.gz  (한 줄 = 받은 시각 ms, 탭, 받은 JSON 그대로). git 밖.
  --collectors 2면 수집기 A·B가 각자 연결 3개로 같은 것을 받고 a/, b/ 아래에 따로 저장한다.
  --drop-at은 수집기 A만 끊는다 (B가 그 빈틈을 메우는지 보려고).
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
SILENCE_MS = 3000  # 메시지 사이가 이보다 길면 '침묵'으로 기록 (KRW 전체 연결은 평소 초당 수십 건)
CONNECT_GAP_S = 0.25  # 연결 시도 사이 최소 간격 (업비트 IP당 초당 5회 한도, 09 M9)
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
        self.trade_keys: dict[tuple[str, int], int | None] = {}  # (코드, 체결 번호) → 체결 시각
        self.trade_dups = 0
        self.events: list[dict] = []  # 끊김 기록
        self.pending: dict[str, dict] = {}  # 다시 붙은 뒤 첫 메시지를 기다리는 끊김
        self.last_ms: dict[str, int] = {}
        self.silences: dict[str, list[tuple[int, int]]] = defaultdict(list)  # (시작, 끝) ms
        self.trade_minutes: dict[str, set[int]] = defaultdict(set)
        self.bad = 0

    def disconnected(self, kind: str, t_ms: int, reason: str, ip: str | None = None) -> None:
        """끊긴 시각과 이유를 남긴다. 빈틈(gap_ms)은 다시 붙은 뒤 첫 메시지에서 채운다."""
        self.disconnects[kind] += 1
        ev = {"kind": kind, "t_ms": t_ms, "reason": reason, "gap_ms": None, "ip": ip}
        self.events.append(ev)
        self.pending[kind] = ev

    def add(self, kind: str, received_ms: int, raw: bytes) -> None:
        prev = self.last_ms.get(kind)
        if prev is not None and received_ms - prev > SILENCE_MS:
            self.silences[kind].append((prev, received_ms))
        self.last_ms[kind] = received_ms
        ev = self.pending.pop(kind, None)
        if ev is not None:
            ev["gap_ms"] = received_ms - ev["t_ms"]
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
            ts = msg.get("trade_timestamp")
            self.trade_keys[key] = ts if isinstance(ts, int) else None
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
            out[f"{k}_silences"] = len(self.silences[k])
            out[f"{k}_silent_s"] = round(sum(e - s for s, e in self.silences[k]) / 1000, 1)
            out[f"{k}_disconnects_spontaneous"] = sum(
                1 for e in self.events if e["kind"] == k and e["reason"] != "forced"
            )
            out[f"{k}_markets_seen"] = len(self.codes[k])
            daily_gz += gz_bytes.get(k, 0) / secs * 86400
        out["est_daily_gb_gzip"] = round(daily_gz / 1e9, 2)
        out["orderbook_levels"] = {str(k): v for k, v in sorted(self.levels.items())}
        out["trade_duplicates"] = self.trade_dups
        out["no_trade_minutes"] = self.no_trade_minutes(codes, end_ms)
        out["bad_json"] = self.bad
        out["disconnect_events"] = self.events
        out["disconnect_reasons"] = dict(Counter(e["reason"] for e in self.events))
        return out


def overlap_ms(x: list[tuple[int, int]], y: list[tuple[int, int]]) -> int:
    """두 구간 목록이 겹치는 시간의 합 (ms)."""
    return sum(max(0, min(e1, e2) - max(s1, s2)) for s1, e1 in x for s2, e2 in y)


def compare(a: Stats, b: Stats, start_ms: int, end_ms: int, margin_ms: int = 10_000) -> dict:
    """수집기 A·B 비교: 한쪽만 받은 체결, 끊김이 겹쳤나, 한쪽 빈틈을 다른 쪽이 메웠나.

    체결은 처음·끝 margin_ms를 뺀 구간(둘 다 확실히 받고 있던 때)의 체결 시각으로 센다.
    """
    lo, hi = start_ms + margin_ms, end_ms - margin_ms

    def keys(st: Stats) -> dict:
        return {k: ts for k, ts in st.trade_keys.items() if ts is not None and lo <= ts < hi}

    ka, kb = keys(a), keys(b)
    only_a = {k: ka[k] for k in ka.keys() - kb.keys()}
    only_b = {k: kb[k] for k in kb.keys() - ka.keys()}

    def spont(st: Stats) -> list[dict]:
        return [e for e in st.events if e["reason"] != "forced"]

    def overlapping(x: list[dict], y: list[dict], within_ms: int = 10_000) -> int:
        return sum(
            1
            for e in x
            if any(f["kind"] == e["kind"] and abs(f["t_ms"] - e["t_ms"]) <= within_ms for f in y)
        )

    def covered(gaps: Stats, other_only: dict) -> int:
        # gaps 쪽 체결 연결이 끊겨 있던 동안 다른 쪽만 받은 체결 수 (앞뒤 2초 여유)
        n = 0
        for e in gaps.events:
            if e["kind"] != "trade" or e["gap_ms"] is None:
                continue
            w0, w1 = e["t_ms"] - 2000, e["t_ms"] + e["gap_ms"] + 2000
            n += sum(1 for ts in other_only.values() if w0 <= ts <= w1)
        return n

    sa, sb = spont(a), spont(b)
    return {
        "trades_both": len(ka.keys() & kb.keys()),
        "trades_only_a": len(only_a),
        "trades_only_b": len(only_b),
        "spontaneous_a": len(sa),
        "spontaneous_b": len(sb),
        "spontaneous_a_overlapping_b_10s": overlapping(sa, sb),
        "spontaneous_b_overlapping_a_10s": overlapping(sb, sa),
        "trades_covered_by_b_during_a_gaps": covered(a, only_b),
        "trades_covered_by_a_during_b_gaps": covered(b, only_a),
        **{
            f"{k}_silent_s_{tag}": v
            for k in TYPES
            for tag, v in (
                ("a", round(sum(e - s for s, e in a.silences[k]) / 1000, 1)),
                ("b", round(sum(e - s for s, e in b.silences[k]) / 1000, 1)),
                ("both", round(overlap_ms(a.silences[k], b.silences[k]) / 1000, 1)),
            )
        },
    }


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


class _Silence(Exception):
    """--silence-sec 동안 메시지가 없어 우리가 끊을 때."""


class _ConnectLimiter:
    """모든 연결 시도 사이에 일정 간격 이상을 둔다."""

    def __init__(self, gap_s: float):
        self.gap_s = gap_s
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            if self.next_at > now:
                await asyncio.sleep(self.next_at - now)
            self.next_at = max(now, self.next_at) + self.gap_s


def ntp_offset_ms(t1: float, t2: float, t3: float, t4: float) -> float:
    """SNTP 시계 차이 (서버 − 내 시계, ms). t1·t4는 내 시계로 보낸·받은 시각, t2·t3은 서버 시각.

    음수면 내 시계가 빠르다. 실제 지연 = 잰 지연 + 이 값.
    """
    return ((t2 - t1) + (t3 - t4)) / 2 * 1000


def sntp_offset_ms(host: str = "time.google.com", timeout: float = 2.0) -> float | None:
    """time.google.com에 SNTP 요청 한 번으로 시계 차이를 잰다. 실패하면 None."""
    import socket
    import struct

    def ts(b: bytes) -> float:
        sec, frac = struct.unpack("!II", b)
        return sec - 2208988800 + frac / 2**32

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(timeout)
            t1 = time.time()
            s.sendto(b"\x1b" + b"\0" * 47, (host, 123))
            data, _ = s.recvfrom(48)
            t4 = time.time()
    except OSError:
        return None
    return round(ntp_offset_ms(t1, ts(data[32:40]), ts(data[40:48]), t4), 1)


class _RunState:
    """모든 연결이 함께 보는 종료 시각. --stop-after-event-min이면 첫 끊김 뒤로 당겨진다."""

    def __init__(self, deadline: float, stop_after_s: float | None):
        self.deadline = deadline
        self.stop_after_s = stop_after_s
        self.first_event_at: float | None = None

    def on_event(self, reason: str) -> None:
        if reason == "forced" or self.stop_after_s is None or self.first_event_at is not None:
            return
        self.first_event_at = time.monotonic()
        self.deadline = min(self.deadline, self.first_event_at + self.stop_after_s)


def _kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).strftime("%H:%M:%S")


async def clock_sampler(state: _RunState, out: list, every_s: float = 600) -> None:
    """every_s마다 시계 차이를 재서 out에 (내 시각 ms, 차이 ms)로 남긴다."""
    while time.monotonic() < state.deadline:
        off = await asyncio.to_thread(sntp_offset_ms)
        out.append((time.time_ns() // 1_000_000, off))
        until = time.monotonic() + every_s
        while time.monotonic() < min(until, state.deadline):
            await asyncio.sleep(1)


async def collect(name, kind, codes, stats, state, drop_at, f, limiter, silence_s=None):
    from websockets.asyncio.client import connect
    from websockets.exceptions import WebSocketException

    backoff = 1.0
    dropped = drop_at is None
    ip = None
    while time.monotonic() < state.deadline:
        await limiter.wait()
        try:
            async with connect(WS_URL, max_size=2**22, open_timeout=10, close_timeout=2) as ws:
                ip = (ws.remote_address or [None])[0]
                await ws.send(subscribe_message(kind, codes, str(uuid.uuid4())))
                backoff = 1.0
                last = time.monotonic()
                while True:
                    now = time.monotonic()
                    if now >= state.deadline:
                        return
                    if silence_s is not None and now - last > silence_s:
                        raise _Silence()
                    if not dropped and now >= drop_at:
                        dropped = True
                        await ws.close()
                        raise _Drop()
                    try:
                        raw = await asyncio.wait_for(ws.recv(decode=False), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    last = time.monotonic()
                    ms = time.time_ns() // 1_000_000
                    f.write(str(ms).encode() + b"\t" + raw + b"\n")
                    stats.add(kind, ms, raw)
        except (_Drop, _Silence, WebSocketException, OSError, asyncio.TimeoutError) as e:
            if isinstance(e, _Drop):
                reason = "forced"
            elif isinstance(e, _Silence):
                reason = "silence"
            else:
                code = getattr(getattr(e, "rcvd", None), "code", None)
                reason = type(e).__name__ + (f":{code}" if code is not None else "")
            stats.disconnected(kind, time.time_ns() // 1_000_000, reason, ip)
            state.on_event(reason)
            print(f"[{_kst()} {name}/{kind}] 끊김: {reason} ip={ip}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def run(
    seconds: int,
    drop_at_s: int | None,
    out_dir: Path,
    collectors: int = 1,
    silence_s: float | None = None,
    stop_after_s: float | None = None,
) -> tuple[dict, dict]:
    codes, clock_offset = fetch_markets()
    print(
        f"KRW 마켓 {len(codes)}개, 수집기 {collectors}대, 시계 차이 약 {clock_offset}초", flush=True
    )
    names = ["a", "b"][:collectors]
    dirs = {"a": out_dir} if collectors == 1 else {n: out_dir / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    gz = {(n, k): gzip.open(dirs[n] / f"{k}.tsv.gz", "wb") for n in names for k in TYPES}
    start_ms = time.time_ns() // 1_000_000
    stats = {n: Stats(start_ms) for n in names}
    t0 = time.monotonic()
    state = _RunState(t0 + seconds, stop_after_s)
    drop_at = t0 + drop_at_s if drop_at_s is not None else None
    limiter = _ConnectLimiter(CONNECT_GAP_S)
    offsets: list[tuple[int, float | None]] = []
    try:
        await asyncio.gather(
            clock_sampler(state, offsets),
            *(
                collect(
                    n,
                    k,
                    codes,
                    stats[n],
                    state,
                    drop_at if n == "a" else None,
                    gz[n, k],
                    limiter,
                    silence_s,
                )
                for n in names
                for k in TYPES
            ),
        )
    finally:
        for f in gz.values():
            f.close()
    # 받는 것은 deadline까지만이므로 비율은 실제 측정 길이로 나눈다 (닫는 대기 시간 제외).
    end_ms = start_ms + int((state.deadline - t0) * 1000)

    def one(n: str) -> dict:
        gz_bytes = {k: (dirs[n] / f"{k}.tsv.gz").stat().st_size for k in TYPES}
        return stats[n].metrics(codes, end_ms, gz_bytes)

    if collectors == 1:
        metrics = one("a")
    else:
        metrics = {n: one(n) for n in names}
        metrics["compare"] = compare(stats["a"], stats["b"], start_ms, end_ms)
        metrics["markets_subscribed"] = len(codes)
    metrics["clock_offset_s_approx"] = clock_offset
    good = [o for _, o in offsets if o is not None]
    metrics["clock_offsets_ms"] = offsets
    if good:
        corr = percentile(good, 50)
        metrics["latency_correction_ms"] = corr
        for m in [metrics] if collectors == 1 else [metrics[n] for n in names]:
            for k in TYPES:
                for q in (50, 99):
                    v = m.get(f"{k}_latency_ms_p{q}")
                    m[f"{k}_latency_ms_p{q}_corrected"] = None if v is None else round(v + corr)
    if state.first_event_at is not None:
        metrics["stopped_after_first_event_s"] = round(state.first_event_at - t0)
    info = {"codes": codes, "start_ms": start_ms, "end_ms": end_ms}
    return metrics, info


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--drop-at", type=int, default=None, help="이 초에 연결을 한 번 일부러 끊음")
    ap.add_argument(
        "--collectors", type=int, choices=(1, 2), default=1, help="동시에 받을 수집기 수"
    )
    ap.add_argument(
        "--silence-sec", type=float, default=None, help="이 초 동안 메시지가 없으면 끊고 다시 연결"
    )
    ap.add_argument(
        "--stop-after-event-min",
        type=float,
        default=None,
        help="업비트 쪽 첫 끊김(또는 침묵) 뒤 이 분이 지나면 끝냄 (--seconds는 최대 길이)",
    )
    ap.add_argument("--no-ledger", action="store_true", help="원장에 쓰지 않음 (동작 확인용)")
    a = ap.parse_args(argv)
    if a.drop_at is not None and not 0 < a.drop_at < a.seconds:
        ap.error("--drop-at은 0과 --seconds 사이")

    started = dt.datetime.now(dt.timezone.utc)
    out_dir = REPO / "data" / "p0" / started.strftime("%Y%m%d-%H%M%S")
    metrics, info = asyncio.run(
        run(
            a.seconds,
            a.drop_at,
            out_dir,
            a.collectors,
            a.silence_sec,
            a.stop_after_event_min * 60 if a.stop_after_event_min else None,
        )
    )

    per = [metrics] if a.collectors == 1 else [metrics["a"], metrics["b"]]
    ok = all(m[f"{k}_msgs"] > 0 for m in per for k in TYPES)
    config = {
        "seconds": a.seconds,
        "drop_at": a.drop_at,
        "types": list(TYPES),
        "markets": "KRW-ALL",
        "format": "DEFAULT",
        "collectors": a.collectors,
        "silence_sec": a.silence_sec,
        "stop_after_event_min": a.stop_after_event_min,
        "connect_gap_s": CONNECT_GAP_S,
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
        + (f", 수집기 {a.collectors}대 동시" if a.collectors > 1 else "")
        + (f", {a.silence_sec:g}초 침묵이면 재연결" if a.silence_sec else "")
        + (
            f", 첫 끊김 {a.stop_after_event_min:g}분 뒤 종료(실제 {metrics['seconds']}초)"
            if a.stop_after_event_min
            else ""
        )
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
