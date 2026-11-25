#!/bin/sh
# 쓸 수 있는 Python(3.9 이상)을 찾아 PY에 넣는다. 없으면 PY는 빈 값.
PY=""
# Windows에서도 한글 메시지가 깨지지 않게 UTF-8로 출력한다.
export PYTHONUTF8=1
for cand in python3 python; do
  if command -v "$cand" >/dev/null 2>&1 &&
     "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)' >/dev/null 2>&1; then
    PY="$cand"
    break
  fi
done
