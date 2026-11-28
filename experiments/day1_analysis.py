"""하루 연속 수집 분석 (notes/01_day1_plan.md A·C·E·F, D의 우리 쪽 1분 캔들).

Bronze Delta 표를 읽어 숫자를 JSON 한 파일로, D용 1분 캔들을 CSV로 남긴다.
공식 캔들과의 비교는 experiments/day1_candles.py가 한다.

    docker run --rm -v <repo>/data/lake:/data -v <repo>/experiments:/exp upbit-spark:local \
      /opt/spark/bin/spark-submit --driver-memory=2g --conf=... /exp/day1_analysis.py
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def ms(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=KST).timestamp() * 1000)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="/data")
    ap.add_argument("--out", default="/data/analysis/day1")
    ap.add_argument("--producer-id", default="producer-a")
    ap.add_argument("--start", default="2026-11-27 09:25")  # T06 시험 구간(09:19~09:25) 제외
    ap.add_argument("--end", default="2026-11-28 09:00")
    ap.add_argument("--kinds", default="trade,ticker,orderbook")
    a = ap.parse_args()
    t0, t1 = ms(a.start), ms(a.end)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = SparkSession.builder.appName("upbit-day1-analysis").getOrCreate()
    res: dict = {"window": [a.start, a.end], "producer_id": a.producer_id}
    j = F.get_json_object

    def load(table: str):
        """한 번만 JSON을 풀어 필요한 컬럼만 남긴다 (원문 통째로 메모리에 올리면 OOM)."""
        df = spark.read.format("delta").load(f"{a.lake}/delta/bronze/{table}")
        t = df.select(
            "value_json",
            F.json_tuple("value_json", "producer_id", "source", "market", "received_at_ms", "payload")
            .alias("producer_id", "source", "market", "recv", "payload"),
        ).withColumn("recv", F.col("recv").cast("long"))
        return t.where((F.col("producer_id") == a.producer_id) & F.col("recv").between(t0, t1 - 1))

    def dump() -> None:
        with open(f"{a.out}/summary.json", "w") as f:
            json.dump(res, f, ensure_ascii=False, indent=1, default=str)

    import os
    os.makedirs(a.out, exist_ok=True)
    from pyspark import StorageLevel

    # A·E·F: 종류별 건수·바이트·지연·SNAPSHOT
    res["types"] = {}
    for kind in a.kinds.split(","):
        fields = ["timestamp", "stream_type"] + (
            ["sequential_id", "trade_timestamp", "trade_price", "trade_volume"] if kind == "trade" else []
        )
        df = load(f"bronze_{kind}_raw").select(
            "source", "market", "recv", F.length("value_json").alias("len"),
            F.json_tuple("payload", *fields).alias(*fields),
        ).withColumn("ts", F.col("timestamp").cast("long"))
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        by_src = {r.source: r.n for r in df.groupBy("source").agg(F.count("*").alias("n")).collect()}
        size = df.agg(F.sum("len").alias("b")).first().b
        snap = df.where(F.col("stream_type") == "SNAPSHOT").count()
        lat = (
            df.where((F.col("source") == "upbit_ws") & (F.col("stream_type") == "REALTIME"))
            .select((F.col("recv") - F.col("ts")).alias("lat"))
            .agg(F.percentile_approx("lat", [0.5, 0.95, 0.99], 10000).alias("p"), F.max("lat").alias("mx"))
            .first()
        )
        res["types"][kind] = {
            "messages_by_source": by_src,
            "json_bytes": size,
            "snapshot": snap,
            "latency_ms_p50_p95_p99": lat.p,
            "latency_ms_max": lat.mx,
        }
        if kind == "trade":
            seq = F.col("sequential_id")
            per_seq = df.groupBy("market", seq.alias("seq")).agg(
                F.countDistinct("source").alias("srcs"), F.count("*").alias("n")
            )
            res["types"][kind]["unique_trades"] = per_seq.count()
            res["types"][kind]["ws_rest_overlap"] = per_seq.where("srcs > 1").count()
            res["types"][kind]["duplicate_rows"] = per_seq.agg(F.sum(F.col("n") - 1)).first()[0]
            trades = (
                df.where(F.col("stream_type").isNull() | (F.col("stream_type") != "SNAPSHOT"))
                .select(
                    "market",
                    seq.alias("seq"),
                    # WebSocket은 trade_timestamp, REST 보충은 timestamp가 체결 시각
                    F.coalesce(F.col("trade_timestamp"), F.col("timestamp")).cast("long").alias("tts"),
                    F.col("trade_price").cast("double").alias("p"),
                    F.col("trade_volume").cast("double").alias("v"),
                )
                .dropDuplicates(["market", "seq"])
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            trades.count()
        df.unpersist()
        dump()
        print(kind, json.dumps(res["types"][kind], default=str), flush=True)

    # Bronze 디스크 (Parquet, 전체 기간)
    res["bronze_bytes"] = {}
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    for t in ("trade", "orderbook", "ticker", "market", "ingest_gap"):
        name = f"bronze_{t}_raw" if t not in ("ingest_gap",) else "bronze_ingest_gap"
        p = jvm.org.apache.hadoop.fs.Path(f"{a.lake}/delta/bronze/{name}")
        res["bronze_bytes"][name] = fs.getContentSummary(p).getLength()

    # C: 공백 기록
    g = spark.read.format("delta").load(f"{a.lake}/delta/bronze/bronze_ingest_gap")  # 공백 기록엔 received_at_ms가 없음
    cols = ["msg_type", "reason", "fault_side", "restart_cause", "pong_ok", "gap_ms", "gap_start_ms",
            "connection_id", "others_stalled", "backfilled_trades"]
    for c in cols:
        g = g.withColumn(c, j("value_json", f"$.{c}"))
    g = g.where((j("value_json", "$.producer_id") == a.producer_id)
                & F.col("gap_start_ms").cast("long").between(t0, t1 - 1))
    gaps = [r.asDict() for r in g.select(*cols).orderBy("gap_start_ms").collect()]
    res["gaps"] = gaps

    # D: 거래대금 상위 10 + 하위 5 마켓의 1분 캔들 (KRW만)
    trades = trades.where(F.col("tts").between(t0, t1 - 1) & F.col("market").startswith("KRW-"))
    val = trades.groupBy("market").agg(F.sum(F.col("p") * F.col("v")).alias("krw")).orderBy(F.desc("krw"))
    ranked = [r.market for r in val.collect()]
    pick = ranked[:10] + ranked[-5:]
    res["candle_markets"] = pick
    w = Window.partitionBy("market", "minute").orderBy("tts", "seq")
    w_all = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    c = (
        trades.where(F.col("market").isin(pick))
        .withColumn("minute", (F.col("tts") / 60000).cast("long") * 60000)
        .withColumn("o", F.first("p").over(w_all))
        .withColumn("c", F.last("p").over(w_all))
        .groupBy("market", "minute")
        .agg(F.first("o").alias("open"), F.max("p").alias("high"), F.min("p").alias("low"),
             F.first("c").alias("close"), F.sum("v").alias("volume"), F.count("*").alias("trades"))
    )
    c.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{a.out}/our_candles")

    dump()
    print(json.dumps(res, ensure_ascii=False, default=str)[:4000])


if __name__ == "__main__":
    main()
