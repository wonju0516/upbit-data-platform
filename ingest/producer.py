"""1단계 수집기: 업비트 KRW 전체 체결·호가·현재가를 받아 Kafka에 넣는다 (이슈 #7).

    .venv/bin/python ingest/producer.py
    .venv/bin/python ingest/producer.py --seconds 180 --drop-at 60   # 동작 확인용

- 종류별 연결 3개. 받은 메시지는 봉투(docs/04_데이터명세서.md 2절)에 원본 그대로 담아 보낸다.
- 침묵 감지(기본 10초, 임시값 #8) → 다시 연결. 끊긴 구간은 ops.ingest_gap에 남기고
  업비트 쪽인지 우리 쪽인지 판정한다 (04 4.5절). 체결은 REST 체결 이력으로 보충한다.
- Kafka에 못 쓰면 data/ingest/spool/에 쌓았다가 다시 보낸다 (09 9-2절 B).
- 5분마다 마켓 목록을 발행하고, KRW 마켓이 바뀌면 같은 연결에 구독을 다시 보낸다.
- 업비트 요청 규칙(09 9-1절): Origin 헤더 없음, 재연결은 대기 시간을 늘려 가며.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import signal
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ingest"))

from p0_profile import (  # noqa: E402
    CONNECT_GAP_S,
    MARKETS_URL,
    TYPES,
    WS_URL,
    _ConnectLimiter,
    krw_markets,
    sntp_offset_ms,
    subscribe_message,
)

BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092"
TOPICS = {
    "trade": "upbit.trade.raw",
    "orderbook": "upbit.orderbook.raw",
    "ticker": "upbit.ticker.raw",
}
MARKET_TOPIC = "upbit.market.raw"
GAP_TOPIC = "ops.ingest_gap"
TICKS_URL = "https://api.upbit.com/v1/trades/ticks"
PROBE_UPBIT_URL = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
STALL_MS = 3000  # 다른 연결이 이만큼 조용하면 '함께 멈춤'으로 셈
REST_GAP_S = 0.125  # REST 요청 사이 간격 (초당 8회, 업비트 시세 조회 한도 10회 아래)
TICKS_PAGE = 500
MARKET_POLL_S = 300
INTERVAL_WINDOW_MS = 600_000  # 수신 간격 최댓값을 10분 단위로 기록 (#8)


# ---------- 순수 함수 (tests/unit/test_producer.py) ----------


def envelope(
    source: str,
    msg_type: str,
    market: str | None,
    producer_id: str,
    connection_id: str,
    received_at_ms: int,
    payload: bytes,
) -> bytes:
    """봉투 JSON. payload는 받은 바이트를 고치지 않고 그대로 끼워 넣는다."""
    head = json.dumps(
        {
            "source": source,
            "msg_type": msg_type,
            "market": market,
            "producer_id": producer_id,
            "connection_id": connection_id,
            "received_at_ms": received_at_ms,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return head[:-1].encode() + b',"payload":' + payload + b"}"


def classify_fault(
    reason: str,
    probe_external_ok: bool | None,
    probe_upbit_ok: bool | None,
    pong_ok: bool | None,
    others_stalled: int,
) -> str:
    """끊김이 업비트 쪽(UPBIT)인지 우리 쪽(LOCAL)인지 판정한다 (04 4.5절 판정표)."""
    if reason in ("PROCESS_RESTART", "PLANNED_STOP", "ERROR", "FORCED"):
        return "LOCAL"
    if probe_external_ok is False:
        return "LOCAL"
    if probe_external_ok is True:
        if reason == "SERVER_CLOSE" or pong_ok is True or probe_upbit_ok is False:
            return "UPBIT"
        if others_stalled > 0:
            return "UPBIT"
    return "UNKNOWN"


def ticks_params(market: str, end_ms: int, now_ms: int, cursor: int | None) -> dict:
    """REST 체결 이력 요청 값. to는 UTC 시각, 오늘(UTC)이 아니면 days_ago를 붙인다."""
    end = dt.datetime.fromtimestamp(end_ms / 1000, dt.timezone.utc)
    today = dt.datetime.fromtimestamp(now_ms / 1000, dt.timezone.utc).date()
    p: dict = {"market": market, "count": TICKS_PAGE, "to": end.strftime("%H:%M:%S")}
    days = (today - end.date()).days
    if days > 0:
        p["days_ago"] = days
    if cursor is not None:
        p["cursor"] = cursor
    return p


def in_window(rows: list[dict], start_ms: int, end_ms: int, margin_ms: int = 2000) -> list[dict]:
    """공백 구간(앞뒤 여유 포함)에 체결된 행만. REST의 timestamp는 체결 시각(ms)."""
    lo, hi = start_ms - margin_ms, end_ms + margin_ms
    return [r for r in rows if isinstance(r.get("timestamp"), int) and lo <= r["timestamp"] <= hi]


def need_more(rows: list[dict], start_ms: int, margin_ms: int = 2000) -> bool:
    """한 쪽을 꽉 채워 받았고 가장 오래된 체결이 아직 공백 시작보다 뒤면 더 받아야 한다."""
    ts = [r["timestamp"] for r in rows if isinstance(r.get("timestamp"), int)]
    return len(rows) >= TICKS_PAGE and bool(ts) and min(ts) > start_ms - margin_ms


def gap_record(
    producer_id: str,
    connection_id: str | None,
    msg_type: str,
    gap_start_ms: int,
    gap_end_ms: int,
    reason: str,
    close_rcvd: str | None = None,
    close_sent: str | None = None,
    pong_ok: bool | None = None,
    server_ip: str | None = None,
    others_stalled: int = 0,
    probe_upbit_ok: bool | None = None,
    probe_external_ok: bool | None = None,
    reconnect_attempts: int = 1,
    restart_cause: str | None = None,
    backfilled_trades: int | None = None,
) -> dict:
    """ops.ingest_gap 한 건 (04 4.5절 컬럼)."""
    return {
        "producer_id": producer_id,
        "connection_id": connection_id,
        "msg_type": msg_type,
        "gap_start_ms": gap_start_ms,
        "gap_end_ms": gap_end_ms,
        "gap_ms": gap_end_ms - gap_start_ms,
        "reason": reason,
        "close_code_rcvd": close_rcvd,
        "close_code_sent": close_sent,
        "pong_ok": pong_ok,
        "server_ip": server_ip,
        "others_stalled": others_stalled,
        "probe_upbit_ok": probe_upbit_ok,
        "probe_external_ok": probe_external_ok,
        "fault_side": classify_fault(
            reason, probe_external_ok, probe_upbit_ok, pong_ok, others_stalled
        ),
        "recoverable": msg_type == "trade",
        "backfilled_trades": backfilled_trades,
        "reconnect_attempts": reconnect_attempts,
        "restart_cause": restart_cause,
    }


def close_text(frame) -> str | None:
    """websockets Close 프레임 → '1011/keepalive ping timeout'."""
    if frame is None:
        return None
    return f"{frame.code}/{frame.reason}"


class IntervalTracker:
    """연결마다 메시지 사이 간격의 10분 최댓값 (#8 침묵 기준 확정용). 재연결 사이는 빼고 잰다."""

    def __init__(self, window_ms: int = INTERVAL_WINDOW_MS):
        self.window_ms = window_ms
        self.prev: dict[str, int] = {}
        self.cur: dict[str, tuple[int, int, int]] = {}  # kind → (창 시작, 메시지 수, 최대 간격)

    def reset(self, kind: str) -> None:
        self.prev.pop(kind, None)

    def add(self, kind: str, t_ms: int) -> tuple[int, str, int, int] | None:
        """창이 바뀌면 끝난 창 (창 시작, 종류, 메시지 수, 최대 간격)을 돌려준다."""
        win = t_ms - t_ms % self.window_ms
        done = None
        w, n, mx = self.cur.get(kind, (win, 0, 0))
        if w != win:
            done = (w, kind, n, mx)
            w, n, mx = win, 0, 0
        p = self.prev.get(kind)
        if p is not None:
            mx = max(mx, t_ms - p)
        self.prev[kind] = t_ms
        self.cur[kind] = (w, n + 1, mx)
        return done


# ---------- 입출력 ----------


def _kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).strftime("%H:%M:%S")


def log(*a) -> None:
    print(f"[{_kst()}]", *a, flush=True)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def http_get(url: str, params: dict | None = None, timeout: float = 5.0) -> bytes:
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def probe_upbit() -> bool:
    try:
        http_get(PROBE_UPBIT_URL, timeout=3)
        return True
    except (OSError, urllib.error.URLError):
        return False


def probe_external() -> bool:
    return sntp_offset_ms(timeout=2.0) is not None


class Spool:
    """Kafka에 못 쓴 메시지를 파일에 쌓는다. 한 줄 = JSON {topic, key, value}."""

    def __init__(self, folder: Path):
        self.folder = folder
        folder.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.count = 0

    def _current(self) -> Path:
        return self.folder / f"spool-{dt.datetime.now(dt.timezone.utc):%Y%m%d-%H}.jsonl"

    def append(self, topic: str, key: bytes | None, value: bytes) -> None:
        line = json.dumps(
            {
                "topic": topic,
                "key": key.decode() if key else None,
                "value": value.decode("utf-8"),
            },
            ensure_ascii=False,
        )
        with self.lock, self._current().open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            self.count += 1

    def take(self) -> list[Path]:
        """보낼 파일을 .sending으로 이름 바꿔 가져간다 (새로 쌓이는 것과 섞이지 않게)."""
        out = []
        with self.lock:
            for p in sorted(self.folder.glob("spool-*.jsonl")):
                q = p.with_suffix(".sending")
                p.rename(q)
                out.append(q)
            out += [p for p in sorted(self.folder.glob("spool-*.sending")) if p not in out]
        return out


class Sink:
    """Kafka 발행. 실패한 메시지는 Spool로, Kafka가 다시 받기 시작하면 Spool을 비운다."""

    def __init__(self, bootstrap: str, producer_id: str, spool: Spool):
        self.conf = {
            "bootstrap.servers": bootstrap,
            "client.id": producer_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
            "message.timeout.ms": 30000,
            "queue.buffering.max.kbytes": 262144,
            "error_cb": self._err,
        }
        self.lock = threading.Lock()
        self.p = self._make()
        self.fatal = False
        self.resets = 0
        self.spool = spool
        self.last_ok = 0.0
        self.sent = 0
        self.failed = 0
        self.stop = threading.Event()
        threading.Thread(target=self._poll, daemon=True).start()
        threading.Thread(target=self._replay, daemon=True).start()

    def _make(self):
        from confluent_kafka import Producer

        return Producer(self.conf)

    def _err(self, err) -> None:
        if err.fatal():
            self.fatal = True
        log(f"Kafka 오류{' (fatal)' if err.fatal() else ''}: {err}")

    def _reset(self) -> None:
        """fatal 상태의 발행기는 더 못 쓰므로 새로 만든다. 남은 메시지는 콜백으로 버퍼에 간다."""
        with self.lock:
            if not self.fatal:
                return
            old, self.p = self.p, self._make()
            self.fatal = False
            self.resets += 1
        from confluent_kafka import KafkaException

        try:  # 남은 메시지를 지우면 콜백이 _PURGE_QUEUE로 와서 버퍼에 쌓인다
            old.purge()
            old.flush(5)
        except KafkaException as e:
            log(f"이전 발행기 정리 중 오류: {e}")
        log(f"Kafka 발행기를 새로 만듦 ({self.resets}번째)")

    def _cb(self, err, msg) -> None:
        if err is None:
            self.last_ok = time.monotonic()
            self.sent += 1
            return
        self.failed += 1
        self.spool.append(msg.topic(), msg.key(), msg.value())
        if self.failed % 1000 == 1:
            log(f"Kafka 쓰기 실패 → 버퍼에 쌓음 (누적 {self.failed}건): {err}")

    def send(self, topic: str, key: str | None, value: bytes) -> None:
        """절대 예외를 내지 않는다. 못 보내면 버퍼로 (Kafka 문제가 업비트 연결을 끊지 않게)."""
        from confluent_kafka import KafkaException

        if self.fatal:
            self._reset()
        k = key.encode() if key else None
        try:
            self.p.produce(topic, value=value, key=k, on_delivery=self._cb)
        except (BufferError, KafkaException) as e:
            if isinstance(e, KafkaException) and e.args[0].fatal():
                self.fatal = True
            self.failed += 1
            self.spool.append(topic, k, value)

    def _poll(self) -> None:
        from confluent_kafka import KafkaException

        while not self.stop.is_set():
            try:
                self.p.poll(0.1)
            except KafkaException as e:  # fatal은 poll에서도 터진다: 스레드는 살려 두고 새로 만든다
                log(f"Kafka poll 오류: {e}")
                if e.args[0].fatal():
                    self.fatal = True
            if self.fatal:
                self._reset()

    def _replay(self) -> None:
        while not self.stop.wait(5):
            if time.monotonic() - self.last_ok > 10:
                continue  # Kafka가 아직 못 받는 중
            for path in self.spool.take():
                n = 0
                with path.open(encoding="utf-8") as f:
                    for line in f:
                        r = json.loads(line)
                        self.send(r["topic"], r["key"], r["value"].encode("utf-8"))
                        n += 1
                        if n % 10000 == 0:
                            self.p.flush(30)
                self.p.flush(30)
                path.unlink()
                log(f"버퍼 {path.name}의 {n}건을 Kafka로 다시 보냄")

    def close(self) -> None:
        left = self.p.flush(30)
        self.stop.set()
        if left:
            log(f"종료 때 못 보낸 {left}건")


class _Silence(Exception):
    def __init__(self, pong_ok: bool | None):
        self.pong_ok = pong_ok


class _Drop(Exception):
    """--drop-at으로 일부러 끊을 때 (동작 확인용)."""


class Collector:
    def __init__(self, a: argparse.Namespace, sink: Sink, codes: list[str]):
        self.a = a
        self.sink = sink
        self.codes = codes
        self.codes_version = 0
        self.stop = asyncio.Event()
        self.last_ms: dict[str, int] = {}
        self.limiter = _ConnectLimiter(CONNECT_GAP_S)
        self.rest_limiter = _ConnectLimiter(REST_GAP_S)
        self.intervals = IntervalTracker()
        self.data = Path(a.data_dir)
        self.data.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data / f"{a.producer_id}-state.json"
        self.pending: dict[str, dict] = {}  # kind → 아직 다시 받기 전인 공백
        self.dropped = a.drop_at is None
        self.mute_done = a.mute_at is None  # T18: 체결 연결의 데이터를 한 번 버림
        self.t0 = time.monotonic()
        self.tasks: set[asyncio.Task] = set()
        self.received = 0  # WebSocket으로 받은 메시지 수 (유실 확인용)
        self.beat = time.monotonic()  # 상태 저장이 돌 때마다 갱신 (감시용)
        self._restart_gaps()

    # --- 공백 ---

    def _restart_gaps(self) -> None:
        """지난 실행의 마지막 수신 시각이 남아 있으면 그 뒤를 공백으로 잡는다."""
        try:
            st = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        reason = "PLANNED_STOP" if st.get("planned") else "PROCESS_RESTART"
        # 끝나기 전에 이유를 남기지 못했으면(오류·강제 종료·정전) CRASH
        cause = "PLANNED" if st.get("planned") else (st.get("cause") or "CRASH")
        for kind, t in (st.get("last_ms") or {}).items():
            if kind in TYPES and isinstance(t, int):
                self.pending[kind] = {"reason": reason, "cause": cause, "start": t, "attempts": 1}
        log(f"지난 실행 기록 있음 → {reason}({cause}) 공백 {sorted(self.pending)}")

    def save_state(self, planned: bool = False, cause: str | None = None) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        st = {"last_ms": self.last_ms, "planned": planned, "cause": cause}
        tmp.write_text(json.dumps(st), encoding="utf-8")
        tmp.replace(self.state_path)

    async def on_disconnect(self, kind: str, cid: str, ip: str | None, reason: str, **kw) -> None:
        t = now_ms()
        others = sum(1 for k, v in self.last_ms.items() if k != kind and t - v > STALL_MS)
        up, ext = await asyncio.gather(
            asyncio.to_thread(probe_upbit), asyncio.to_thread(probe_external)
        )
        g = self.pending.get(kind)
        if g is not None:  # 다시 받기 전에 또 끊김: 처음 공백을 이어 간다
            g["attempts"] += 1
            return
        self.pending[kind] = {
            "reason": reason,
            "start": self.last_ms.get(kind, t),
            "cid": cid,
            "ip": ip,
            "others": others,
            "up": up,
            "ext": ext,
            "attempts": 1,
            **kw,
        }
        log(
            f"{kind} 끊김 {reason} ip={ip} pong={kw.get('pong')} 함께멈춤={others} "
            f"업비트REST={up} 외부={ext}"
        )

    def on_recovered(self, kind: str, t: int) -> None:
        g = self.pending.pop(kind, None)
        if g is None:
            return
        rec = gap_record(
            self.a.producer_id,
            g.get("cid"),
            kind,
            g["start"],
            t,
            g["reason"],
            close_rcvd=g.get("rcvd"),
            close_sent=g.get("sent"),
            pong_ok=g.get("pong"),
            server_ip=g.get("ip"),
            others_stalled=g.get("others", 0),
            probe_upbit_ok=g.get("up"),
            probe_external_ok=g.get("ext"),
            reconnect_attempts=g["attempts"],
            restart_cause=g.get("cause"),
        )
        if kind == "trade":
            self._spawn(self.backfill(rec))
        else:
            self.publish_gap(rec)

    def publish_gap(self, rec: dict) -> None:
        self.sink.send(
            GAP_TOPIC, rec["producer_id"], json.dumps(rec, separators=(",", ":")).encode()
        )
        log(
            f"공백 기록 {rec['msg_type']} {rec['gap_ms'] / 1000:.1f}초 {rec['reason']} "
            f"→ {rec['fault_side']} (보충 {rec['backfilled_trades']})"
        )

    async def backfill(self, rec: dict) -> None:
        """공백 구간의 체결을 마켓마다 REST로 다시 받아 source=upbit_rest로 보낸다."""
        start, end = rec["gap_start_ms"], rec["gap_end_ms"]
        total = 0
        for code in list(self.codes):
            cursor = None
            while True:
                await self.rest_limiter.wait()
                rows = await asyncio.to_thread(self._ticks, code, end, cursor)
                if rows is None:
                    break
                t = now_ms()
                for r in in_window(rows, start, end):
                    raw = json.dumps(r, separators=(",", ":"), ensure_ascii=False).encode()
                    self.sink.send(
                        TOPICS["trade"],
                        code,
                        envelope(
                            "upbit_rest", "trade", code, self.a.producer_id, "rest-backfill", t, raw
                        ),
                    )
                    total += 1
                if not need_more(rows, start):
                    break
                cursor = rows[-1].get("sequential_id")
        rec["backfilled_trades"] = total
        self.publish_gap(rec)

    def _ticks(self, code: str, end_ms: int, cursor: int | None) -> list[dict] | None:
        params = ticks_params(code, end_ms + 2000, now_ms(), cursor)
        for i in range(3):
            try:
                return json.loads(http_get(TICKS_URL, params))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(1 + i)
                    continue
                log(f"REST 체결 {code} 실패: {e.code}")
                return None
            except (OSError, ValueError) as e:
                log(f"REST 체결 {code} 실패: {e!r}")
                time.sleep(1 + i)
        return None

    # --- 연결 ---

    async def connection(self, kind: str) -> None:
        from websockets.asyncio.client import connect
        from websockets.exceptions import ConnectionClosed, WebSocketException

        backoff = 1.0
        n = 0
        while not self.stop.is_set():
            await self.limiter.wait()
            n += 1
            cid = (
                f"{self.a.producer_id}-{kind}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}-{n}"
            )
            ip = None
            try:
                async with connect(WS_URL, max_size=2**22, open_timeout=10, close_timeout=2) as ws:
                    ip = (ws.remote_address or [None])[0]
                    sent_ver = self.codes_version
                    await ws.send(subscribe_message(kind, self.codes, str(uuid.uuid4())))
                    self.intervals.reset(kind)
                    last = time.monotonic()
                    while not self.stop.is_set():
                        mono = time.monotonic()
                        if sent_ver != self.codes_version:
                            sent_ver = self.codes_version
                            await ws.send(subscribe_message(kind, self.codes, str(uuid.uuid4())))
                            log(f"{kind} 구독 다시 보냄: 마켓 {len(self.codes)}개")
                        if mono - last > self.a.silence_sec:
                            raise _Silence(await self._pong(ws))
                        if (
                            not self.dropped
                            and kind == "trade"
                            and mono - self.t0 >= self.a.drop_at
                        ):
                            self.dropped = True
                            raise _Drop()
                        try:
                            raw = await asyncio.wait_for(ws.recv(decode=False), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if self._muted(kind):
                            continue  # T18: 연결은 그대로 두고 받은 것을 버려 침묵을 만든다
                        last = time.monotonic()
                        backoff = 1.0
                        self.on_message(kind, cid, raw)
                    return
            except _Silence as e:
                if self._muted(kind):
                    self.mute_done = True
                await self.on_disconnect(kind, cid, ip, "SILENCE", pong=e.pong_ok)
            except _Drop:
                await self.on_disconnect(kind, cid, ip, "FORCED")
            except ConnectionClosed as e:
                reason = "SERVER_CLOSE" if e.rcvd is not None else "CONN_LOST"
                await self.on_disconnect(
                    kind, cid, ip, reason, rcvd=close_text(e.rcvd), sent=close_text(e.sent)
                )
            except (WebSocketException, OSError, asyncio.TimeoutError) as e:
                await self.on_disconnect(kind, cid, ip, "CONN_LOST", sent=repr(e)[:200])
            except Exception as e:  # noqa: BLE001 - 우리 코드 오류도 공백으로 남기고 계속 돈다
                traceback.print_exc()
                await self.on_disconnect(kind, cid, ip, "ERROR", sent=repr(e)[:200])
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _muted(self, kind: str) -> bool:
        return (
            not self.mute_done and kind == "trade" and time.monotonic() - self.t0 >= self.a.mute_at
        )

    async def _pong(self, ws) -> bool | None:
        """침묵 중 WebSocket ping을 보내 3초 안에 응답이 오는지 본다."""
        try:
            waiter = await ws.ping()
            await asyncio.wait_for(waiter, timeout=3)
            return True
        except asyncio.TimeoutError:
            return False
        except Exception:  # noqa: BLE001 - 이미 닫힌 연결
            return None

    def on_message(self, kind: str, cid: str, raw: bytes) -> None:
        t = now_ms()
        self.received += 1
        self.last_ms[kind] = t
        self.on_recovered(kind, t)
        try:
            code = json.loads(raw).get("code")
        except ValueError:
            code = None
        self.sink.send(
            TOPICS[kind], code, envelope("upbit_ws", kind, code, self.a.producer_id, cid, t, raw)
        )
        done = self.intervals.add(kind, t)
        if done:
            w, k, cnt, mx = done
            line = f"{w}\t{k}\t{cnt}\t{mx}\n"
            with (self.data / f"{self.a.producer_id}-intervals.tsv").open("a") as f:
                f.write(line)

    # --- 마켓 목록, 상태 ---

    async def market_poller(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=MARKET_POLL_S)
                return
            except asyncio.TimeoutError:
                pass
            try:
                body = json.loads(await asyncio.to_thread(http_get, MARKETS_URL))
            except (OSError, ValueError) as e:
                log(f"마켓 목록 실패: {e!r}")
                continue
            self.publish_markets(body)
            new = krw_markets(body)
            if new != self.codes:
                log(f"KRW 마켓 바뀜 {len(self.codes)} → {len(new)}개")
                self.codes = new
                self.codes_version += 1

    def publish_markets(self, body: list[dict]) -> None:
        t = now_ms()
        for m in body:
            code = m.get("market", "")
            if code.startswith("KRW-"):
                raw = json.dumps(m, separators=(",", ":"), ensure_ascii=False).encode()
                self.sink.send(
                    MARKET_TOPIC,
                    code,
                    envelope(
                        "upbit_rest", "market", code, self.a.producer_id, "market-poll", t, raw
                    ),
                )

    async def state_saver(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=5)
                return
            except asyncio.TimeoutError:
                self.save_state()
                self.beat = time.monotonic()
                log_every = int(time.monotonic() - self.t0) % 600 < 5
                if log_every:
                    log(
                        f"보냄 {self.sink.sent} 실패 {self.sink.failed} "
                        f"버퍼 누적 {self.sink.spool.count}"
                    )

    def _watchdog(self, limit_s: float = 60.0) -> None:
        """멈춤(먹통) 감시. Docker 재시작 정책은 프로세스가 끝나야 동작하므로 스스로 끝낸다."""
        while not self.stop.is_set():
            time.sleep(5)
            if not self.stop.is_set() and time.monotonic() - self.beat > limit_s:
                log(f"감시: {limit_s:.0f}초 넘게 멈춤 → 스스로 종료 (Docker가 다시 띄움)")
                try:  # 다음 실행이 HANG으로 기록하게 남긴다
                    self.save_state(cause="HANG")
                except OSError:
                    pass
                os._exit(3)

    def _spawn(self, coro) -> None:
        t = asyncio.get_running_loop().create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, self.stop.set)
        if self.a.seconds:
            loop.call_later(self.a.seconds, self.stop.set)
        threading.Thread(target=self._watchdog, daemon=True).start()
        log(
            f"수집 시작: {self.a.producer_id}, KRW 마켓 {len(self.codes)}개, "
            f"침묵 {self.a.silence_sec}초"
        )
        await asyncio.gather(
            self.market_poller(),
            self.state_saver(),
            *(self.connection(k) for k in TYPES),
        )
        if self.tasks:
            log(f"진행 중인 REST 보충 {len(self.tasks)}개를 기다림")
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.save_state(planned=True)
        log(f"정상 종료 (계획 정지로 기록). WebSocket 수신 {self.received}건")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--producer-id", default="producer-a")
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--silence-sec", type=float, default=10.0, help="임시값 (이슈 #8)")
    ap.add_argument("--seconds", type=int, default=None, help="이 초 뒤 끝냄 (동작 확인용)")
    ap.add_argument("--drop-at", type=int, default=None, help="이 초에 체결 연결을 한 번 끊음")
    ap.add_argument(
        "--mute-at",
        type=int,
        default=None,
        help="이 초부터 체결 연결로 오는 것을 버려 침묵을 만듦 (T18 시험용, 한 번)",
    )
    ap.add_argument("--data-dir", default=str(REPO / "data" / "ingest"))
    a = ap.parse_args(argv)

    body = json.loads(http_get(MARKETS_URL))
    spool = Spool(Path(a.data_dir) / "spool")
    sink = Sink(a.bootstrap, a.producer_id, spool)
    c = Collector(a, sink, krw_markets(body))
    c.publish_markets(body)
    try:
        asyncio.run(c.run())
    finally:
        sink.close()
        log(f"보냄 {sink.sent} 실패 {sink.failed} 버퍼 누적 {spool.count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
