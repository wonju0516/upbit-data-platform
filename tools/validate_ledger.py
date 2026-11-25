"""원장 검사. 작업 트리의 원장 형식과, 직전 커밋 대비 append-only 여부를 본다."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gitutil  # noqa: E402
import ledger  # noqa: E402


def main() -> int:
    path = Path(ledger.LEDGER_PATH)
    if not path.exists():
        print(f"[ledger] {path} 없음", file=sys.stderr)
        return 1
    text = path.read_text(encoding="utf-8")
    errors = ledger.validate(text) + ledger.check_append_only(
        gitutil.show(f"HEAD:{ledger.LEDGER_PATH}"), text
    )
    if errors:
        print("[ledger] 오류", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(f"[ledger] 통과: {len(ledger.parse(text))}행")
    return 0


if __name__ == "__main__":
    sys.exit(main())
