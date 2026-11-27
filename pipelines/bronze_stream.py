"""1단계 Bronze 잡: Kafka 토픽 5개를 원본 그대로 Delta 테이블에 쌓는다 (이슈 #9, docs/04 3절).

컨테이너(docker/compose.yml의 bronze)에서 spark-submit으로 돈다.
- 토픽마다 스트리밍 쿼리 하나, 체크포인트로 죽었다 살아도 빠짐·중복 없이 이어서 쓴다.
- 처음 뜰 때는 Kafka에 남은 가장 오래된 것부터(earliest) 읽는다.
- ingest_date·ingest_hour는 한국 시각(spark.sql.session.timeZone=Asia/Seoul) 기준 파티션.
"""

from __future__ import annotations

import argparse

TABLES = {
    "upbit.trade.raw": "bronze_trade_raw",
    "upbit.orderbook.raw": "bronze_orderbook_raw",
    "upbit.ticker.raw": "bronze_ticker_raw",
    "upbit.market.raw": "bronze_market_raw",
    "ops.ingest_gap": "bronze_ingest_gap",
}


def bronze_columns(df):
    """Kafka 레코드 → Bronze 컬럼 (04 3절). 값은 문자열로 바꿀 뿐 고치지 않는다."""
    from pyspark.sql import functions as F

    return (
        df.select(
            F.col("value").cast("string").alias("value_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at"),
        )
        .withColumn("ingest_date", F.to_date("ingested_at"))
        .withColumn("ingest_hour", F.hour("ingested_at"))
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--lake", default="/data", help="delta/·checkpoints/ 아래에 씀")
    ap.add_argument("--trigger", default="1 minute", help="마이크로배치 간격")
    ap.add_argument(
        "--max-offsets", type=int, default=500_000, help="한 배치에 토픽당 최대 메시지 수"
    )
    a = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("upbit-bronze").getOrCreate()
    for topic, table in TABLES.items():
        src = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", a.bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            # Kafka 보관(3일)이 지나 지워진 구간이 있어도 멈추지 않는다. 빠진 구간은
            # 오프셋 연속성 검사(bronze_check.py)로 드러난다.
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", a.max_offsets)
            .load()
        )
        (
            bronze_columns(src)
            .writeStream.format("delta")
            .outputMode("append")
            .partitionBy("ingest_date", "ingest_hour")
            .option("checkpointLocation", f"{a.lake}/checkpoints/bronze/{table}")
            .trigger(processingTime=a.trigger)
            .queryName(table)
            .start(f"{a.lake}/delta/bronze/{table}")
        )
    # 쿼리 하나라도 실패하면 예외로 끝나고 → 컨테이너 재시작 정책이 다시 띄운다
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
