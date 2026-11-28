"""하루 분석 D: 우리 체결로 만든 1분 캔들 vs 업비트 공식 1분 캔들 (notes/01_day1_plan.md).

day1_analysis.py가 만든 our_candles CSV를 읽고, 같은 마켓의 공식 캔들을 REST로 받아
분 단위로 OHLCV가 같은지 센다. 초당 5회 이하로 호출한다.

    python experiments/day1_candles.py --dir data/lake/analysis/day1
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
URL = "https://api.upbit.com/v1/candles/minutes/1?market={m}&to={to}&count=200"


def fetch(market: str, t0: int, t1: int) -> dict[int, tuple]:
    out: dict[int, tuple] = {}
    to = t1
    while to > t0:
        iso = datetime.fromtimestamp(to / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        req = urllib.request.Request(URL.format(m=market, to=iso), headers={"Accept": "application/json"})
        rows = json.load(urllib.request.urlopen(req, timeout=10))
        time.sleep(0.2)
        if not rows:
            break
        for r in rows:
            t = int(datetime.strptime(r["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                    .replace(tzinfo=timezone.utc).timestamp() * 1000)
            if t0 <= t < t1:
                out[t] = (r["opening_price"], r["high_price"], r["low_price"], r["trade_price"],
                          r["candle_acc_trade_volume"])
        to = min(int(datetime.strptime(r["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                     .replace(tzinfo=timezone.utc).timestamp() * 1000) for r in rows)
    return out


def same(a: tuple, b: tuple) -> list[str]:
    bad = [n for n, x, y in zip("OHLC", a[:4], b[:4]) if abs(float(x) - float(y)) > 1e-12 * max(1, abs(float(y)))]
    if abs(float(a[4]) - float(b[4])) > 1e-6 * max(1e-8, abs(float(b[4]))):
        bad.append("V")
    return bad


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="data/lake/analysis/day1")
    ap.add_argument("--out", default="data/analysis-day1-candles.json")  # lake 폴더는 Spark 컨테이너 사용자 소유
    a = ap.parse_args()
    summary = json.load(open(f"{a.dir}/summary.json"))
    s, e = summary["window"]
    t0 = int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=KST).timestamp() * 1000)
    t1 = int(datetime.strptime(e, "%Y-%m-%d %H:%M").replace(tzinfo=KST).timestamp() * 1000)

    ours: dict[str, dict[int, tuple]] = defaultdict(dict)
    for path in glob.glob(f"{a.dir}/our_candles/*.csv"):
        for r in csv.DictReader(open(path)):
            ours[r["market"]][int(r["minute"])] = (r["open"], r["high"], r["low"], r["close"], r["volume"])

    total = {"minutes": 0, "match": 0, "only_official": 0, "only_ours": 0, "diff": 0}
    report = {}
    for m in summary["candle_markets"]:
        off = fetch(m, t0, t1)
        o = ours.get(m, {})
        stat = {"official": len(off), "ours": len(o), "match": 0, "only_official": [], "only_ours": [], "diff": []}
        for t in sorted(set(off) | set(o)):
            ts = datetime.fromtimestamp(t / 1000, KST).strftime("%m-%d %H:%M")
            if t not in o:
                stat["only_official"].append(ts)
            elif t not in off:
                stat["only_ours"].append(ts)
            else:
                bad = same(o[t], off[t])
                if bad:
                    stat["diff"].append([ts, "".join(bad)])
                else:
                    stat["match"] += 1
        n = len(set(off) | set(o))
        total["minutes"] += n
        total["match"] += stat["match"]
        for k in ("only_official", "only_ours", "diff"):
            total[k] += len(stat[k])
        report[m] = stat
        print(f"{m}: 분 {n}, 일치 {stat['match']} ({stat['match'] / max(n, 1):.4%}), 공식에만 "
              f"{len(stat['only_official'])}, 우리에만 {len(stat['only_ours'])}, 값 다름 {len(stat['diff'])} {stat['diff']}")
    total["match_rate"] = total["match"] / max(total["minutes"], 1)
    print("합계", total)
    json.dump({"total": total, "markets": report}, open(a.out, "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
