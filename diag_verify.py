# -*- coding: utf-8 -*-
"""不依赖候选池，对当日通过条件3/5 的全部股票重算条件4/5/6。

用途：验证「候选池」是否漏股。走的是和早上无池模式完全相同的重算路径，
日K全部命中 output/cache/kline_<day>.json.gz，零网络。

用法：
    SCREENER_HOME=<交付目录> python diag_verify.py [YYYY-MM-DD]
"""
import sys

sys.path.insert(0, ".")

import backfill as bf
import collector as col
from screener import Funnel, load_cfg, stage_spot


def main() -> int:
    day = sys.argv[1] if len(sys.argv) > 1 else "2026-10-21"
    cfg = load_cfg()

    snaps, meta, problems = col.pick_samples(day)
    if "0925" not in snaps:
        print("[X] 没有 9:25 快照")
        return 1

    f = Funnel()
    d = stage_spot(snaps["0925"], cfg, f, source_label="全市场快照(9:25)")
    if d.index.name != "code" and "code" in d.columns:
        d = d.set_index("code", drop=False)

    p924 = snaps["0924"]["price"].reindex(d.index) if "0924" in snaps else None
    if p924 is not None:
        d = d[p924.notna() & (d["price"] >= p924)]
        f.add("9:25价 >= 9:24价", len(d))

    print(f.render())
    print("-> 待重算条件4/5/6：%d 只" % len(d))

    bf.load_kline_cache(day)
    cfg2 = dict(cfg)
    cfg2["max_candidates"] = 10 ** 6
    f2 = Funnel()
    kept = bf.stage_history_par(d, cfg2, day, f2, workers=4)
    print(f2.render())

    cols = ["code", "name", "price", "auc_chg", "amount", "prev_turnover",
            "ma", "ma_bias_pct", "last_zt_date", "t2_chg_pct", "t2_over_t1"]
    cols = [c for c in cols if c in kept.columns]
    print("\n=== 不依赖候选池的结果：%d 只 ===" % len(kept))
    if len(kept):
        k = kept.copy()
        k["amount"] = (k["amount"] / 1e4).round(0).astype("int64")
        for c in ("price", "auc_chg", "prev_turnover", "ma", "ma_bias_pct",
                  "t2_chg_pct", "t2_over_t1"):
            if c in k.columns:
                k[c] = k[c].round(2)
        print(k[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
