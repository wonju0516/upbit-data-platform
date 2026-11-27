"""Bronze 검사: 토픽·칸마다 Kafka 오프셋이 빠짐·중복 없이 이어져 있는지 본다 (이슈 #9).

Kafka 오프셋은 칸마다 0부터 1씩 늘어나므로 "건수 = 서로 다른 오프셋 수 = 최대 − 최소 + 1"이면
그 범위에 빠진 것도 겹친 것도 없다.

    docker exec upbit-bronze /opt/spark/bin/spark-submit --conf ... /app/pipelines/bronze_check.py
"""

from __future__ import annotations

import argparse
import sys

from bronze_stream import TABLES


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="/data")
    a = ap.parse_args()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("upbit-bronze-check").getOrCreate()
    ok = True
    for table in TABLES.values():
        path = f"{a.lake}/delta/bronze/{table}"
        try:
            df = spark.read.format("delta").load(path)
        except Exception as e:  # noqa: BLE001 - 아직 테이블이 없을 수 있음
            print(f"FAIL {table}: 읽기 실패 {str(e)[:120]}")
            ok = False
            continue
        rows = (
            df.groupBy("kafka_topic", "kafka_partition")
            .agg(
                F.count("*").alias("n"),
                F.countDistinct("kafka_offset").alias("distinct"),
                F.min("kafka_offset").alias("lo"),
                F.max("kafka_offset").alias("hi"),
            )
            .orderBy("kafka_partition")
            .collect()
        )
        for r in rows:
            span = r.hi - r.lo + 1
            good = r.n == r.distinct == span
            ok &= good
            print(
                f"{'PASS' if good else 'FAIL'} {table} p{r.kafka_partition}: 건수 {r.n}, "
                f"중복 {r.n - r.distinct}, 빠짐 {span - r.distinct}, 오프셋 {r.lo}~{r.hi}"
            )
    print("Bronze 검사:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
