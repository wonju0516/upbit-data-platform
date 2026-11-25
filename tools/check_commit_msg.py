"""커밋 메시지 검사 (docs/RULES.md 3.3절). commit-msg 훅과 verify.sh가 쓴다.

사용:
  python tools/check_commit_msg.py <메시지 파일>     # commit-msg 훅 (인덱스 기준)
  python tools/check_commit_msg.py --range <범위>    # 이미 만든 커밋들 (예: origin/main..HEAD)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gitutil  # noqa: E402
import ledger  # noqa: E402

# scope 목록은 docs/RULES.md 3.3절, .githooks/commit-msg와 같은 커밋에서 함께 고친다.
TYPES = ["record", "fix", "upgrade", "feat", "docs", "chore", "revert"]
SCOPES = [
    "ingest",
    "kafka",
    "bronze",
    "silver",
    "gold",
    "recon",
    "serve",
    "web",
    "monitor",
    "infra",
    "env",
    "docs",
]
REQUIRED_TRAILERS = {
    "record": ["Run-Id", "Ledger", "Config-SHA256"],
    "fix": ["Invalidates"],
}
# record 커밋에 들어가도 되는 경로 (코드가 섞이면 안 됨)
RECORD_ALLOWED_PREFIXES = ("experiments/", "notes/", "STATE.md")
TITLE_RE = re.compile(r"^(?P<type>[a-z]+)\((?P<scope>[a-z]+)\): (?P<subject>.+)$")
TRAILER_RE = re.compile(r"^(?P<key>[A-Za-z][A-Za-z0-9-]*): (?P<value>.+)$")
MAX_TITLE = 72


def _strip_comments(message: str) -> list[str]:
    return [ln.rstrip("\r") for ln in message.splitlines() if not ln.startswith("#")]


def parse_trailers(lines: list[str]) -> dict[str, list[str]]:
    """마지막 문단에서 `Key: value` 줄을 모은다."""
    body = list(lines)
    while body and body[-1].strip() == "":
        body.pop()
    block: list[str] = []
    for ln in reversed(body):
        if ln.strip() == "":
            break
        block.append(ln)
    trailers: dict[str, list[str]] = {}
    for ln in reversed(block):
        m = TRAILER_RE.match(ln)
        if m:
            trailers.setdefault(m["key"], []).append(m["value"].strip())
    return trailers


def check(message: str, ledger_text: str | None, files: list[str]) -> list[str]:
    """오류 목록을 돌려준다. 비어 있으면 통과.

    ledger_text: 이 커밋 시점의 원장 내용(인덱스 또는 그 커밋). files: 이 커밋에 들어가는 파일.
    """
    lines = _strip_comments(message)
    while lines and lines[0].strip() == "":
        lines.pop(0)
    if not lines:
        return ["커밋 메시지가 비어 있습니다"]
    title = lines[0]
    if title.startswith(("Merge ", "fixup! ", "squash! ")):
        return []
    errors: list[str] = []
    m = TITLE_RE.match(title)
    if not m:
        return [f"제목 형식은 '<type>(<scope>): <제목>'입니다: {title!r}"]
    ctype, scope, subject = m["type"], m["scope"], m["subject"]
    if ctype not in TYPES:
        errors.append(f"type {ctype!r}는 목록에 없습니다: {', '.join(TYPES)}")
    if scope not in SCOPES:
        errors.append(f"scope {scope!r}는 목록에 없습니다: {', '.join(SCOPES)}")
    if len(title) > MAX_TITLE:
        errors.append(f"제목이 {len(title)}자입니다 (최대 {MAX_TITLE}자)")
    if subject.endswith("."):
        errors.append("제목 끝에 마침표를 쓰지 않습니다")
    if ctype != "revert" and not subject.endswith("다"):
        errors.append("제목은 한국어 평서문('~한다')으로 끝냅니다")
    if len(lines) > 1 and lines[1].strip() != "":
        errors.append("제목과 본문 사이에 빈 줄이 필요합니다")

    trailers = parse_trailers(lines[1:])
    for key in REQUIRED_TRAILERS.get(ctype, []):
        if key not in trailers:
            errors.append(f"{ctype} 커밋에는 '{key}:' 트레일러가 필요합니다")

    rows = {r["run_id"]: r for r in ledger.parse(ledger_text or ledger.HEADER)}
    if ctype == "record":
        for rid in trailers.get("Run-Id", []):
            if rid not in rows:
                errors.append(f"Run-Id {rid}가 커밋될 원장에 없습니다")
        for path in trailers.get("Ledger", []):
            if path != ledger.LEDGER_PATH:
                errors.append(f"Ledger는 {ledger.LEDGER_PATH}이어야 합니다")
        for sha in trailers.get("Config-SHA256", []):
            if not ledger.SHA256_RE.match(sha):
                errors.append("Config-SHA256은 64자리 소문자 16진수입니다")
            elif not any(
                r["config_sha256"] == sha
                for rid, r in rows.items()
                if rid in trailers.get("Run-Id", [])
            ):
                errors.append("Config-SHA256이 Run-Id 행의 config_sha256과 다릅니다")
        mixed = [f for f in files if not f.startswith(RECORD_ALLOWED_PREFIXES)]
        if mixed:
            errors.append("record 커밋에 코드·문서가 섞였습니다: " + ", ".join(mixed))
    if ctype == "fix":
        for value in trailers.get("Invalidates", []):
            if value == "none":
                continue
            for rid in [v.strip() for v in value.split(",")]:
                if rid not in rows:
                    errors.append(f"Invalidates의 {rid}가 원장에 없습니다")
    return errors


def _report(errors: list[str], label: str) -> None:
    print(f"[commit-msg] {label}", file=sys.stderr)
    for e in errors:
        print(f"  - {e}", file=sys.stderr)
    print("  규칙: docs/RULES.md 3.3절", file=sys.stderr)


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "--range":
        failed = 0
        for sha in gitutil.rev_list(argv[1]):
            errs = check(
                gitutil.commit_message(sha),
                gitutil.show(f"{sha}:{ledger.LEDGER_PATH}"),
                gitutil.commit_files(sha),
            )
            if errs:
                failed += 1
                _report(errs, f"{sha[:7]} 거부")
        return 1 if failed else 0
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    message = Path(argv[0]).read_text(encoding="utf-8", errors="replace")
    errs = check(message, gitutil.show(f":{ledger.LEDGER_PATH}"), gitutil.staged_files())
    if errs:
        _report(errs, "커밋 메시지 거부")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
