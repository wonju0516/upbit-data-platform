# LEDGER_SCHEMA — 측정 원장 열 정의

`experiments/LEDGER.tsv`는 탭으로 구분한 append-only 표입니다. 한 행이 측정 한 번(run)입니다. `tools/ledger.py`의 `append_run()`만 행을 씁니다. 규칙은 [RULES.md](RULES.md) 4절에 있습니다.

| 열 | 형식 | 뜻 |
|---|---|---|
| `run_id` | `R<YYYYMMDD>-<HHMMSS>-<6자리 hex>` | 측정 하나의 고유 이름. 커밋 트레일러 `Run-Id`가 이 값을 가리킴 |
| `ts_utc` | ISO 8601 UTC (`2026-11-25T12:00:00Z`) | 행을 쓴 시각 |
| `git_sha` | 40자리 hex 또는 `dirty-<40자리>` | 측정한 코드의 커밋. 작업 트리가 깨끗하지 않으면 `dirty-` |
| `stage` | `P0`~`P8` | 단계 ID (`STATE.md` 진행표) |
| `script` | 저장소 기준 경로 | 측정을 실행한 스크립트 |
| `config_sha256` | 64자리 hex | 측정 설정(인자, 대상 코인, 기간)을 JSON으로 만든 뒤의 SHA-256. 트레일러 `Config-SHA256`과 대조 |
| `sample` | 글 | 표본 범위. 예: `KRW 전체 301개, 2026-11-26T00:00Z부터 600초` |
| `status` | `ok` · `fail` · `correction` | `correction`은 이전 행을 정정하는 행 (`note`에 대상 `run_id`) |
| `metrics_json` | JSON 한 줄 | 지표 이름과 값. 예: `{"orderbook_msgs_per_s": 512.3}` |
| `artifact` | 경로 또는 `-` | 산출물 위치(git 밖 `data/` 아래). 없으면 `-` |
| `artifact_sha256` | 64자리 hex 또는 `-` | 산출물 파일의 SHA-256 |
| `note` | 글 또는 `-` | 메모. 탭·줄바꿈 금지 |

## 검사 (`tools/validate_ledger.py`)

- 첫 줄이 위 열 이름과 정확히 같음
- 모든 행의 열 수가 같고, `run_id`가 형식에 맞고 겹치지 않음
- `status`가 세 값 중 하나, `metrics_json`이 JSON 객체, 해시 열이 형식에 맞음
- **append-only**: 커밋될 원장이 직전 커밋의 원장으로 시작함 (앞 행을 고치거나 지우면 거부)
