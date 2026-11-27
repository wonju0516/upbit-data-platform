# 1단계 수집기 이미지 (ingest/producer.py). 근거: docs/09_결정현황.md 9-1절(이미지 기준)
# Python 3.13 계열 최신판 중 나온 지 2주 넘은 3.13.17 (2026-10-22), digest로 고정
FROM python:3.13.17-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY env/requirements.txt env/requirements.txt
# 수집기에 필요한 두 개만 설치 (버전은 env/requirements.txt 한 곳에서 관리)
RUN grep -E '^(websockets|confluent-kafka)==' env/requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -r /tmp/req.txt \
 && useradd -u 1000 -m collector
COPY tools/ tools/
COPY ingest/ ingest/
USER collector
ENTRYPOINT ["python", "ingest/producer.py"]
