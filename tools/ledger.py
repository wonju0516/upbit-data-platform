"""측정 원장(experiments/LEDGER.tsv) 읽기·검사·추가. 표준 라이브러리만 쓴다.

열 정의는 docs/LEDGER_SCHEMA.md. 행은 append_run()으로만 추가한다.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
import secrets
import subprocess
from pathlib import Path

LEDGER_PATH = "experiments/LEDGER.tsv"
COLUMNS = [
    "run_id",
    "ts_utc",
    "git_sha",
    "stage",
    "script",
    "config_sha256",
    "sample",
    "status",
    "metrics_json",
    "artifact",
    "artifact_sha256",
    "note",
]
HEADER = "\t".join(COLUMNS) + "\n"
STATUSES = {"ok", "fail", "correction"}
RUN_ID_RE = re.compile(r"^R\d{8}-\d{6}-[0-9a-f]{6}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^(dirty-)?[0-9a-f]{40}$")
STAGE_RE = re.compile(r"^P[0-8]$")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def parse(text: str) -> list[dict[str, str]]:
    """원장 문자열을 행 목록으로 바꾼다. 형식 검사는 validate()가 한다."""
    lines = text.splitlines()
    rows = []
    for line in lines[1:]:
        if line == "":
            continue
        cells = line.split("\t")
        rows.append(dict(zip(COLUMNS, cells)))
    return rows


def validate(text: str) -> list[str]:
    """원장 내용의 형식 오류 목록을 돌려준다. 비어 있으면 통과."""
    errors: list[str] = []
    if not text.startswith(HEADER):
        return ["원장 첫 줄이 열 정의와 다릅니다 (docs/LEDGER_SCHEMA.md)"]
    if not text.endswith("\n"):
        errors.append("원장 마지막 줄 끝에 줄바꿈이 없습니다")
    seen: set[str] = set()
    for n, line in enumerate(text.splitlines()[1:], start=2):
        if line == "":
            errors.append(f"{n}행: 빈 줄")
            continue
        cells = line.split("\t")
        if len(cells) != len(COLUMNS):
            errors.append(f"{n}행: 열 수 {len(cells)}개 (기대 {len(COLUMNS)}개)")
            continue
        row = dict(zip(COLUMNS, cells))
        rid = row["run_id"]
        if not RUN_ID_RE.match(rid):
            errors.append(f"{n}행: run_id 형식 오류 {rid!r}")
        if rid in seen:
            errors.append(f"{n}행: run_id 중복 {rid}")
        seen.add(rid)
        if not TS_RE.match(row["ts_utc"]):
            errors.append(f"{n}행: ts_utc 형식 오류")
        if not GIT_SHA_RE.match(row["git_sha"]):
            errors.append(f"{n}행: git_sha 형식 오류")
        if not STAGE_RE.match(row["stage"]):
            errors.append(f"{n}행: stage 형식 오류")
        if not SHA256_RE.match(row["config_sha256"]):
            errors.append(f"{n}행: config_sha256 형식 오류")
        if row["status"] not in STATUSES:
            errors.append(f"{n}행: status는 {sorted(STATUSES)} 중 하나")
        try:
            if not isinstance(json.loads(row["metrics_json"]), dict):
                errors.append(f"{n}행: metrics_json은 JSON 객체여야 함")
        except json.JSONDecodeError:
            errors.append(f"{n}행: metrics_json이 JSON이 아님")
        if row["artifact_sha256"] != "-" and not SHA256_RE.match(row["artifact_sha256"]):
            errors.append(f"{n}행: artifact_sha256 형식 오류")
    return errors


def check_append_only(old: str | None, new: str) -> list[str]:
    """new가 old로 시작하는지(앞 행을 고치거나 지우지 않았는지) 본다."""
    if old is None or old == "":
        return []
    if not new.startswith(old):
        return ["원장은 append-only입니다. 이미 있는 행을 고치거나 지웠습니다 (정정은 새 행)"]
    return []


def config_sha256(config: dict) -> str:
    """측정 설정을 정렬된 JSON으로 만든 뒤의 SHA-256."""
    blob = json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _clean(value: str) -> str:
    if "\t" in value or "\n" in value or "\r" in value:
        raise ValueError("원장 값에 탭이나 줄바꿈을 넣을 수 없습니다")
    return value


def _git_sha(repo: Path) -> str:
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    return f"dirty-{sha}" if dirty else sha


def append_run(
    *,
    stage: str,
    script: str,
    config: dict,
    sample: str,
    status: str,
    metrics: dict,
    artifact: str | None = None,
    note: str | None = None,
    repo: str | Path = ".",
    now: _dt.datetime | None = None,
    git_sha: str | None = None,
) -> str:
    """원장에 측정 한 행을 추가하고 run_id를 돌려준다."""
    repo = Path(repo)
    now = now or _dt.datetime.now(_dt.timezone.utc)
    run_id = f"R{now:%Y%m%d-%H%M%S}-{secrets.token_hex(3)}"
    art_sha = file_sha256(repo / artifact) if artifact else "-"
    row = [
        run_id,
        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        git_sha or _git_sha(repo),
        stage,
        _clean(script),
        config_sha256(config),
        _clean(sample),
        status,
        json.dumps(metrics, sort_keys=True, ensure_ascii=False),
        _clean(artifact or "-"),
        art_sha,
        _clean(note or "-"),
    ]
    path = repo / LEDGER_PATH
    old = path.read_text(encoding="utf-8") if path.exists() else HEADER
    new = old + "\t".join(row) + "\n"
    errors = validate(new)
    if errors:
        raise ValueError("; ".join(errors))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        if not path.exists() or path.stat().st_size == 0:
            f.write(HEADER)
        f.write("\t".join(row) + "\n")
    return run_id
