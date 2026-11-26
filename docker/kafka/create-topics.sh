#!/bin/bash
# 토픽 5개를 미리 만든다 (docs/04_데이터명세서.md 2절, docs/09_결정현황.md 2절).
# 이미 있으면 건너뛰고, 마지막에 칸 수·복제 수를 출력한다.
set -euo pipefail

BS=kafka-1:19092,kafka-2:19092,kafka-3:19092
KT=/opt/kafka/bin/kafka-topics.sh
RETENTION_MS=259200000 # 3일

create() {
  local name=$1 partitions=$2
  "$KT" --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 3 \
    --config retention.ms="$RETENTION_MS" \
    --config min.insync.replicas=2 \
    --config compression.type=zstd
}

create upbit.trade.raw 3
create upbit.orderbook.raw 3
create upbit.ticker.raw 3
create upbit.market.raw 1
create ops.ingest_gap 1

"$KT" --bootstrap-server "$BS" --describe \
  --topic 'upbit\..*|ops\..*' | grep -E '^Topic:'
