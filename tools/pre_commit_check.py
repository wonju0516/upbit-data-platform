"""pre-commit 검사 (docs/RULES.md 3.4절). 인덱스(커밋될 내용)를 본다.

막는 것: 5MB 초과, 데이터·모델 바이너리, 금지 경로, 비밀값 모양,
.ipynb 출력 셀, 원장 append-only 위반.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gitutil  # noqa: E402
import ledger  # noqa: E402

MAX_BYTES = 5 * 1024 * 1024
FORBIDDEN_PREFIXES = (
    "data/",
    "_private/",
    "checkpoints/",
    "spark-warehouse/",
    "mlruns/",
    "mlartifacts/",
    "lockbox/",
)
DATA_EXTENSIONS = {
    ".parquet",
    ".zip",
    ".gz",
    ".tgz",
    ".tar",
    ".7z",
    ".zst",
    ".csv",
    ".jsonl",
    ".ndjson",
    ".pkl",
    ".pickle",
    ".joblib",
    ".pt",
    ".pth",
    ".onnx",
    ".h5",
    ".npy",
    ".npz",
    ".db",
    ".sqlite",
    ".duckdb",
    ".safetensors",
    ".lgb",
    ".avro",
    ".orc",
}
# 비밀값 모양. 문자열을 나눠 적어 이 파일 자체가 걸리지 않게 한다.
SECRET_PATTERNS = [
    ("GitHub 토큰", re.compile(r"\b(gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})")),
    ("AWS 키", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("Slack 토큰", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    ("개인 키", re.compile("-----BEGIN [A-Z ]*" + "PRIVATE KEY-----")),
    ("Discord 웹훅", re.compile(r"discord(app)?\.com/api/" + r"webhooks/\d+/[A-Za-z0-9_-]+")),
    ("Slack 웹훅", re.compile(r"hooks\.slack\.com/" + r"services/[A-Za-z0-9/]+")),
]


def check_path(path: str) -> list[str]:
    p = PurePosixPath(path)
    errors = []
    if path.startswith(FORBIDDEN_PREFIXES):
        errors.append(f"{path}: 커밋 금지 경로입니다")
    name = p.name
    if name == ".env" or (name.startswith(".env.") and name != ".env.example"):
        errors.append(f"{path}: .env 파일은 커밋하지 않습니다 (이름만 .env.example에)")
    if p.suffix.lower() in DATA_EXTENSIONS:
        errors.append(f"{path}: 데이터·모델 파일({p.suffix})은 커밋하지 않습니다")
    return errors


def check_content(path: str, data: bytes) -> list[str]:
    errors = []
    if len(data) > MAX_BYTES:
        errors.append(f"{path}: {len(data) / 1024 / 1024:.1f}MB (최대 5MB)")
    if b"\0" in data[:8000]:
        errors.append(f"{path}: 바이너리 파일입니다")
        return errors
    text = data.decode("utf-8", errors="replace")
    for label, pat in SECRET_PATTERNS:
        if pat.search(text):
            errors.append(f"{path}: {label}처럼 보이는 값이 있습니다")
    if path.endswith(".ipynb"):
        try:
            nb = json.loads(text)
            if any(c.get("outputs") for c in nb.get("cells", [])):
                errors.append(f"{path}: 노트북 출력 셀을 지우고 커밋합니다")
        except json.JSONDecodeError:
            errors.append(f"{path}: 노트북 JSON을 읽을 수 없습니다")
    return errors


def check_ledger(index_text: str | None, head_text: str | None) -> list[str]:
    if index_text is None:
        return []
    return ledger.validate(index_text) + ledger.check_append_only(head_text, index_text)


def main() -> int:
    errors: list[str] = []
    files = gitutil.staged_files()
    for path in files:
        errors += check_path(path)
        if gitutil.staged_size(path) > MAX_BYTES:
            errors.append(f"{path}: 5MB를 넘습니다")
            continue
        errors += check_content(path, gitutil.staged_bytes(path))
    if ledger.LEDGER_PATH in files:
        errors += check_ledger(
            gitutil.show(f":{ledger.LEDGER_PATH}"), gitutil.show(f"HEAD:{ledger.LEDGER_PATH}")
        )
    if errors:
        print("[pre-commit] 커밋 거부", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        print("  규칙: docs/RULES.md 2·3.4절", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
