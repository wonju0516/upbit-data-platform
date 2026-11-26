# STRUCTURE.md — 저장소 지도

## 읽는 순서

| 누가 | 순서 |
|---|---|
| 사람 | `README.md` → `docs/01_초기기획서.md` → `STATE.md` → 필요한 단계의 기준 문서 |
| AI 에이전트 | `AGENTS.md` → `STATE.md` → `docs/09_결정현황.md` → `docs/RULES.md` → 작업 단계의 기준 문서 |

## 폴더

`(예정)`은 아직 비어 있고, `git 밖`은 커밋하지 않습니다.

```
upbit-data-platform/
├── AGENTS.md              에이전트·사람 공통 진입점
├── CLAUDE.md              @AGENTS.md 한 줄
├── README.md              프로젝트 소개
├── STATE.md               진행 기록 (지금, 다음 할 일, 결정, 열린 질문)
├── STRUCTURE.md           이 문서
├── verify.sh              커밋 전 검사 한 번에
├── .env.example           비밀값 이름만 (실제 값은 .env, git 밖)
├── docs/                  설계 문서 01~10, RULES, LEDGER_SCHEMA, 아키텍처 그림(.svg·.excalidraw)
├── experiments/LEDGER.tsv 측정 원장 (append-only, 코드가 씀)
├── env/                   Python 환경 선언(예정), 하드웨어 실측 hw-YYYYMMDD.md
├── tools/                 훅 본체·원장 도구 (Python 표준 라이브러리만)
├── .githooks/             commit-msg, pre-commit (sh → tools/)
├── .github/               CI, PR·이슈 템플릿
├── tests/unit/            도구·코드 단위 테스트
├── tests/scenarios/       장애 시험 T01~T20 재현 스크립트 (예정)
├── ingest/                수집기, Market Poller (예정)
├── pipelines/             Bronze·Silver·Gold·대사 Spark 잡 (예정)
├── dags/                  Airflow 일일 배치 (예정)
├── dashboards/            Grafana 대시보드 JSON (예정)
├── docker/                Docker Compose (예정)
├── api/ web/             FastAPI, React (예정, 07 문서 5단계)
├── features/ ml/ serving/ ML 단계 (예정)
├── notes/                 사람이 읽는 측정 요약 (원장 run_id를 적음)
├── data/                  수집 원본·산출물 (git 밖)
└── _private/              개인 메모, 이 PC의 환경 정보 LOCAL.md (git 밖)
```

## 단계별 기준 문서

| 단계 | 이 순서로 따름 | 만드는 곳 | 커밋 scope |
|---|---|---|---|
| 0 프로파일링 | 09 → 07(0단계) → 04(원천 필드) | `ingest/`, `notes/`, 원장 | `ingest` |
| 1 수집 → Kafka → Bronze | 09 2·3절 → 04(2·3절) → 06(T06·T07·T11~T13·T18·T19) | `ingest/`, `docker/`, `pipelines/` | `ingest` `kafka` `bronze` |
| 2 모니터링 | 09 1절 → 06(3절) | `dashboards/`, `docker/` | `monitor` |
| 3 Silver | 09 4절 → 04(4절) → 06(T01~T03) | `pipelines/` | `silver` |
| 4 Gold + 대사 | 09 5·9-2절 → 04(5절) → 06(1절, T04·T05·T20) | `pipelines/`, `dags/` | `gold` `recon` `infra` |
| 5 서빙 → 화면 | 09 → 04(7절) → 05(7절) | `api/`, `web/` | `serve` `web` |
| 6 2주 운영 | 06(3-0·3-1·4·5절) | `notes/` | `docs` |

## 큰 실수를 막는 체크리스트

| 확인 | 어기면 생기는 일 |
|---|---|
| 커밋 전에 `./verify.sh` | CI에서 실패하고 이슈가 생김 |
| `data/`·데이터 파일이 스테이징되지 않았나 | 업비트 약관 위반 (데이터 배포) |
| `.env`·웹훅 주소·비밀번호가 파일에 없나 | 비밀값 노출 |
| 측정을 원장(`tools/ledger.py`)으로 남겼나 | 결과의 출처를 증명할 수 없음 |
| 09와 다른 이름·테이블을 만들지 않았나 | 문서와 코드가 어긋남 |
| 측정 결과를 `record` 커밋으로 따로 남겼나 | 결과와 코드 계보가 섞임 |
| 커밋 메시지·파일에 IP·계정·이메일·PC 경로가 없나 | 개인 정보가 기록에 남음 |

## 이름 규칙

| 대상 | 규칙 | 예 |
|---|---|---|
| 커밋 | `<type>(<scope>): <제목>` | `feat(ingest): 0단계 호가 수집 스크립트를 추가한다` |
| 측정 ID | `R<YYYYMMDD>-<HHMMSS>-<hex6>` | `R20261126-031500-a1b2c3` |
| 설계 문서 | `docs/<두 자리 번호>_<이름>.md` | `docs/09_결정현황.md` |
| 하드웨어 기록 | `env/hw-YYYYMMDD.md` | `env/hw-20261125.md` |
| 테이블 | `<계층>_<대상>` (04 문서 0절) | `silver_trade`, `fact_candle_1m` |
