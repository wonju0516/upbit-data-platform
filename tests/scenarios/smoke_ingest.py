"""수집기 스모크 테스트 (docs/06_신뢰성검증계획.md 0절). 고친 뒤마다 이것 하나만 돌린다.

    .venv/bin/python tests/scenarios/smoke_ingest.py            # 3분, 60초에 체결 연결을 끊음
    .venv/bin/python tests/scenarios/smoke_ingest.py --check-only <producer_id> --data-dir <폴더>
        # 수집기를 컨테이너 등 다른 곳에서 이미 돌렸을 때 Kafka 확인만

1. 시험 전용 이름(producer-smoke-<시각>)과 폴더(data/ingest-smoke/)로 수집기를 돌린다.
   본 수집의 상태 파일·버퍼와 섞이지 않는다.
2. Kafka에서 그 이름의 메시지만 읽어 통과 기준을 확인하고 PASS/FAIL을 출력한다.
원장에는 쓰지 않는다 (측정이 아니라 동작 확인).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TOPICS = ("upbit.trade.raw", "upbit.orderbook.raw", "upbit.ticker.raw", "upbit.market.raw")


def read_topic(c, topic: str, producer_id: str) -> list[dict]:
    from confluent_kafka import TopicPartition

    out = []
    for p in c.list_topics(topic, timeout=10).topics[topic].partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if hi <= lo:
            continue
        c.assign([TopicPartition(topic, p, lo)])
        got = 0
        while got < hi - lo:
            m = c.poll(5)
            if m is None:
                break
            if m.error():
                continue
            got += 1
            if f'"producer_id":"{producer_id}"'.encode() in m.value()[:300]:
                out.append(json.loads(m.value()))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=180)
    ap.add_argument("--drop-at", type=int, default=60)
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--check-only", metavar="PRODUCER_ID", help="실행 없이 이 이름의 결과만 확인")
    ap.add_argument("--data-dir", default=str(REPO / "data" / "ingest-smoke"))
    a = ap.parse_args()

    data = Path(a.data_dir)
    if a.check_only:
        pid, returncode = a.check_only, 0
    else:
        pid = f"producer-smoke-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}"
        shutil.rmtree(data, ignore_errors=True)
        cmd = [
            sys.executable,
            str(REPO / "ingest" / "producer.py"),
            "--producer-id",
            pid,
            "--seconds",
            str(a.seconds),
            "--drop-at",
            str(a.drop_at),
            "--data-dir",
            str(data),
        ]
        print("실행:", " ".join(cmd[1:]), flush=True)
        returncode = subprocess.run(cmd, timeout=a.seconds + 120).returncode

    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": pid, "enable.auto.commit": False})
    msgs = {t: read_topic(c, t, pid) for t in TOPICS}
    gaps = [g for g in read_topic(c, "ops.ingest_gap", pid) if g["producer_id"] == pid]
    c.close()
    spool = list((data / "spool").glob("*")) if (data / "spool").exists() else []

    trade_src = Counter(m["source"] for m in msgs["upbit.trade.raw"])
    forced = [g for g in gaps if g["reason"] == "FORCED"]
    checks = [
        ("수집기 정상 종료", returncode == 0),
        ("체결·호가·현재가 모두 받음", all(len(msgs[t]) > 0 for t in TOPICS[:3])),
        ("마켓 목록 발행", len(msgs["upbit.market.raw"]) > 0),
        ("일부러 끊은 공백 1건 기록", len(forced) == 1),
        ("그 공백 판정 LOCAL", bool(forced) and forced[0]["fault_side"] == "LOCAL"),
        ("체결 REST 보충 끝남", bool(forced) and forced[0]["backfilled_trades"] is not None),
        (
            "REST 보충분이 체결 토픽에 있음",
            trade_src["upbit_rest"] == (forced[0]["backfilled_trades"] if forced else -1),
        ),
        ("남은 버퍼 없음", not spool),
    ]
    print(json.dumps({t: len(v) for t, v in msgs.items()}, ensure_ascii=False))
    print("체결 출처:", dict(trade_src), "공백:", json.dumps(gaps, ensure_ascii=False))
    for name, ok in checks:
        print(("  PASS " if ok else "  FAIL ") + name)
    ok = all(v for _, v in checks)
    print("스모크 테스트:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
