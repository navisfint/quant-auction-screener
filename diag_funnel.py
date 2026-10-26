# -*- coding: utf-8 -*-
"""离线复现「预筛候选池」的完整漏斗 —— 回答「全市场 5574 只怎么只剩 100」。

只读 + 零日K请求：日K全部命中 output/cache/kline_<for_date>.json.gz。
唯一网络请求是全市场快照（1 次）。

用法：
    python diag_funnel.py [for_date]
"""
import sys
from datetime import datetime

sys.path.insert(0, ".")

import backfill as bf
import datasource as ds
import prep
from screener import load_cfg


def main() -> int:
    for_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    cfg = load_cfg()
    relax = float(cfg.get("prefilter_relax", 1.25))

    src = ds.build_source(cfg)
    spot = src.spot()
    if spot is None or spot.empty:
        print("[X] 快照为空")
        return 1

    rows = [("① 全市场快照", len(spot))]

    d = spot.copy()
    if cfg["main_board_only"]:
        d = d[d["code"].map(prep._is_main_board)]
    if cfg["exclude_st"]:
        d = d[~d["name"].map(prep._is_st)]
    rows.append(("② 主板 & 非ST", len(d)))

    d = d.dropna(subset=["price", "float_mv"])
    d = d[d["price"] > 0]
    d = d[d["price"] < cfg["max_price"] * relax]
    cap = cfg.get("market_cap_field", "float_mv")
    if cap not in d.columns:
        cap = "float_mv"
    d = d[d[cap].notna() & (d[cap] < cfg["max_market_cap"] * relax)]
    rows.append(("③ 静态放宽 %g 倍（价<%.1f, 市值<%.0f亿）"
                 % (relax, cfg["max_price"] * relax,
                    cfg["max_market_cap"] * relax / 1e8), len(d)))

    bf.load_kline_cache(for_date)
    print("    日K缓存就绪 %d 只（零网络）" % len(bf._KLINE_CACHE))

    stages = {i: 0 for i in range(6)}
    kept = []
    for code, row in d.iterrows():
        try:
            st, rec = bf._check_history_one(row, cfg, for_date, "tx")
        except Exception:
            st, rec = 0, None
        stages[st] = stages.get(st, 0) + 1
        if rec is not None:
            kept.append(rec)

    n4 = stages.get(1, 0) + stages.get(2, 0) + stages.get(3, 0) + stages.get(4, 0) + stages.get(5, 0)
    n5 = stages.get(2, 0) + stages.get(3, 0) + stages.get(4, 0) + stages.get(5, 0)
    n6 = stages.get(3, 0) + stages.get(4, 0) + stages.get(5, 0)
    n7 = stages.get(4, 0) + stages.get(5, 0)
    rows.append(("④ 条4a 昨日换手率 (%.1f%%, %.1f%%)"
                 % (cfg["min_prev_turnover"], cfg["max_prev_turnover"]), n4))
    rows.append(("⑤ 条4b 昨收 > MA%d" % int(cfg["ma_period"]), n5))
    rows.append(("⑥ 条5b 前%d日有涨停" % int(cfg["zt_lookback_days"]), n6))
    rows.append(("⑦ 条6a T-2涨跌幅 < %.1f%%" % cfg["t2_chg_max"], n7))
    rows.append(("⑧ 条6b T-2额/T-1额 > %.2f"
                 % cfg["min_t2_over_t1_amount_ratio"], stages.get(5, 0)))
    rows.append(("= 候选池最终", len(kept)))

    print()
    print("=" * 78)
    print("  候选池完整漏斗  for_date=%s  T-1 由日K最后一根决定" % for_date)
    print("=" * 78)
    w = max(len(r[0]) for r in rows)
    prev = None
    for i, (k, n) in enumerate(rows):
        mark = "  " if i == 0 else "→ "
        extra = ""
        if prev is not None and prev > 0:
            extra = "   (剔除 %d)" % (prev - n) if prev - n else "   (无变化)"
        print("  %s%-*s %6d%s" % (mark, w, k, n, extra))
        prev = n
    print("=" * 78)
    # stage 0 同时代表「日K无效」和「昨日换手率不达标」，两者在
    # _check_history_one 里共用同一个返回码，这里只能合并表述，
    # 不能写成「数据缺失」——那会让人误以为接口挂了。
    print("  未过条4a（含日K无效/停牌）: %d 只" % stages.get(0, 0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
