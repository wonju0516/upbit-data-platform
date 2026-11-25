#!/usr/bin/env bash
# 커밋 전 검사를 한 번에 (docs/RULES.md 3.4절)
#   ./verify.sh            push 안 한 커밋까지 검사
#   VERIFY_RANGE=a..b ./verify.sh   검사할 커밋 범위를 직접 지정 (CI)
set -u
cd "$(dirname "$0")"
. .githooks/_python.sh
if [ -z "$PY" ]; then
  echo "[verify] Python 3.9 이상이 없습니다" >&2
  exit 1
fi
fail=0
step() { echo "== $1"; }

step "1/5 린트 (ruff check)"
if command -v ruff >/dev/null 2>&1; then
  ruff check tools tests || fail=1
else
  echo "   ruff 없음: 건너뜀 (환경 준비 전). CI에서는 실행됨"
fi

step "2/5 포맷 (ruff format --check)"
if command -v ruff >/dev/null 2>&1; then
  ruff format --check tools tests || fail=1
else
  echo "   ruff 없음: 건너뜀"
fi

step "3/5 테스트"
"$PY" -m unittest discover -s tests/unit || fail=1

step "4/5 원장"
"$PY" tools/validate_ledger.py || fail=1

step "5/5 커밋 메시지"
if [ -n "${VERIFY_RANGE:-}" ]; then
  RANGE="$VERIFY_RANGE"
elif git rev-parse --abbrev-ref '@{u}' >/dev/null 2>&1; then
  RANGE="@{u}..HEAD"
else
  RANGE="HEAD --not --remotes"
fi
echo "   범위: $RANGE"
"$PY" tools/check_commit_msg.py --range "$RANGE" || fail=1

if [ "$fail" -ne 0 ]; then
  echo "[verify] 실패" >&2
  exit 1
fi
echo "[verify] 통과"
