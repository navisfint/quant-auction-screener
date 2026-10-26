"""收盘后预筛 —— 把条件 4/5/6 提前到 T-1 晚上算完。

【为什么要这一步】
条件 1（9:20 的量）和条件 2（9:24 的价）只能靠盘中多时点快照取样，
9:19–9:26 那几分钟要反复采样，能省的条件必须省掉。
而条件 4（昨日换手率 / 昨收>MA）、条件 5（前30日涨停）、条件 6（T-2 涨跌幅 /
T-2÷T-1 成交额比）用的**全是「昨日及之前」的数据**，收盘后就定型了，
跟次日盘中毫无关系。所以挪到晚上算，早上窗口里只剩条件 1/2/3。

【产出】
output/pool/pool_YYYYMMDD.json    （YYYYMMDD = 这个池子服务的交易日）
    for_date    适用交易日
    built_at    生成时刻
    prev_date   依据的 T-1 日期
    items       [{code, name, market, ref_close, shares, ...历史字段}]

【为什么要放宽静态项】
股价 < 50 元、流通市值 < 120 亿 这两项按 T-1 收盘值判定，但次日的竞价价
可能跳空，导致「昨天刚好 49.8 元、今天竞价 51 元」。所以池子按 relax 系数
放大（默认 1.25 倍），次日采样后用**竞价实时价**精确复核。
宁可池子大一点，也不能因为放宽不足漏掉真正的候选。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import datasource as ds
from datasource import OUT_DIR
from screener import Funnel, _is_main_board, _is_st, load_cfg, log_line

POOL_DIR = OUT_DIR / "pool"

# 日K并发硬上限 —— 见 build_pool 文档字符串里的实测依据。
# 任何调用方（含 runner 的 --kworkers）传更大的值都会被夹到这里。
HIST_WORKERS_MAX = 4


def next_trade_date(now: datetime | None = None) -> str:
    """次日（自动跳过周末）。A 股节假日无法推断，需要时用 --for-date 显式指定。"""
    now = now or datetime.now()
    d = now.date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def pool_file(for_date: str) -> Path:
    return POOL_DIR / ("pool_%s.json" % for_date.replace("-", ""))


def load_pool(for_date: str) -> dict | None:
    fp = pool_file(for_date)
    if not fp.exists():
        return None
    try:
        return json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_pool(cfg: dict | None = None, for_date: str | None = None,
               workers: int = 4, source: str = "tencent",
               relax: float | None = None, quiet: bool = False) -> dict | None:
    """跑一次预筛，返回池子 dict（同时落盘）。失败返回 None。

    ★ 并发上限 4（HIST_WORKERS_MAX）—— 2026-10-19 实测出来的硬约束：
      腾讯 web.ifzq.gtimg.cn 的 WAF 在约 90 只/秒 时必然返回 HTTP 501。
      10 线程跑 2272 只 = 91 只/秒 → 立刻被拦 → 自动降级到新浪「不复权」
      日K → MA 偏高 → 「昨收 > MA」被低估 → 临界股被漏掉。
      同一份数据实测对比：前复权 233→193→100 只，不复权 232→192→99 只
      （漏掉 002972，MA 差仅 0.13%）。
      4 线程约 31 只/秒，2272 只耗时 74 秒，全程不被拦。
      盘后跑，多花 50 秒换「口径正确 + 不漏股」，不要为了快把口径搞坏。
    """
    workers = max(1, min(int(workers), HIST_WORKERS_MAX))
    cfg = cfg or load_cfg()
    now = datetime.now()
    for_date = for_date or next_trade_date(now)
    relax = float(cfg.get("prefilter_relax", 1.25) if relax is None else relax)

    def say(*a):
        if not quiet:
            print(*a)

    say(">>> 预筛（为 %s 准备候选池）  %s" % (for_date, now.strftime("%H:%M:%S")))

    try:
        src = ds.build_source(cfg, prefer=source)
        spot = src.spot()
    except Exception as e:
        msg = "全市场快照获取失败：%s: %s" % (type(e).__name__, e)
        say("[X] " + msg)
        log_line("!! 预筛失败 " + msg)
        return None

    if spot is None or spot.empty:
        say("[X] 快照为空，预筛中止")
        return None
    say("    全市场 %d 只（来源 %s）" % (len(spot), src.name))

    d = spot.copy()
    if cfg["main_board_only"]:
        d = d[d["code"].map(_is_main_board)]
    if cfg["exclude_st"]:
        d = d[~d["name"].map(_is_st)]
    n_board = len(d)

    # 静态项按放宽系数先粗筛：真正的复核留给次日采样（用竞价实时价）
    d = d.dropna(subset=["price", "float_mv"])
    d = d[d["price"] > 0]
    d = d[d["price"] < cfg["max_price"] * relax]
    cap_col = cfg.get("market_cap_field", "float_mv")
    if cap_col not in d.columns:
        cap_col = "float_mv"
    d = d[d[cap_col].notna() & (d[cap_col] < cfg["max_market_cap"] * relax)]
    say("    主板非ST %d 只 → 静态项放宽 %g 倍后 %d 只" % (n_board, relax, len(d)))

    funnel = Funnel()
    items: list[dict] = []
    if d.empty:
        say("[!] 静态项筛完为空，池子为空（这不正常，请检查阈值）")
    else:
        # 条件 4/5/6 并行校验；today_str 传 for_date，让最后一根K线 = T-1
        import backfill as bf
        # max_candidates 封顶在盘中是为防跑飞，盘后预筛必须解除
        cfg2 = dict(cfg)
        cfg2["max_candidates"] = 10 ** 6
        try:
            kept = bf.stage_history_par(d, cfg2, for_date, funnel, workers=workers)
        except RuntimeError as e:
            say("[X] 日K校验失败：%s" % e)
            log_line("!! 预筛日K失败 %s" % e)
            return None
        say(funnel.render())
        # 口径提示：降级到不复权会漏临界股，必须让用户看得见，不能静默
        _adj = ds.kline_adj_summary()
        if _adj:
            say("    " + _adj)
            if ds.KLINE_ADJ.get("none"):
                log_line("!! 预筛日K口径降级 " + _adj)
        for _, r in kept.iterrows():
            ref_close = float(r["price"])
            fv = float(r[cap_col]) if pd.notna(r.get(cap_col)) else None
            items.append({
                "code": str(r["code"]), "name": str(r["name"]),
                "market": int(r["market"]),
                "ref_close": ref_close,
                "shares": (fv / ref_close) if (fv and ref_close > 0) else None,
                "prev_date": str(r["prev_date"]),
                "prev_close_d": float(r["prev_close_d"]),
                "prev_turnover": float(r["prev_turnover"]),
                "ma": float(r["ma"]), "ma_bias_pct": float(r["ma_bias_pct"]),
                "last_zt_date": str(r["last_zt_date"]),
                "t2_date": str(r["t2_date"]),
                "t2_chg_pct": float(r["t2_chg_pct"]),
                "t2_over_t1": float(r["t2_over_t1"]),
                "t2_amount": float(r["t2_amount"]),
                "t1_amount": float(r["t1_amount"]),
            })

    payload = {
        "for_date": for_date,
        "built_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "source": src.name,
        "prev_date": items[0]["prev_date"] if items else None,
        "relax": relax,
        "count": len(items),
        "kline_adj": dict(ds.KLINE_ADJ),
        "funnel": [[str(a), int(b)] for a, b in funnel.steps],
        "items": items,
    }
    POOL_DIR.mkdir(parents=True, exist_ok=True)
    fp = pool_file(for_date)
    fp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    say("")
    say(">>> 候选池 %d 只  →  %s" % (len(items), fp))
    if items:
        say("    依据 T-1 = %s；采样时每轮只需 1 个请求（%d 只 ≈ 1 批）"
            % (payload["prev_date"], len(items)))
    log_line("预筛 %s 候选池 %d 只 → %s" % (for_date, len(items), fp.name))
    return payload


def main(argv=None):
    ds.setup_std_encoding()
    ap = argparse.ArgumentParser(description="收盘后预筛：为下一交易日准备候选池")
    ap.add_argument("--for-date", default=None,
                    help="池子服务的交易日 YYYY-MM-DD（默认自动取次日，跳周末）")
    ap.add_argument("--source", default="tencent",
                    choices=["auto", "eastmoney", "tencent"])
    ap.add_argument("--workers", type=int, default=HIST_WORKERS_MAX,
                    help="日K并发数，上限 %d（超过会被夹到上限，原因见 build_pool）"
                         % HIST_WORKERS_MAX)
    ap.add_argument("--relax", type=float, default=None,
                    help="静态项放宽系数，默认读 config.prefilter_relax")
    args = ap.parse_args(argv)

    cfg = load_cfg()
    res = build_pool(cfg, for_date=args.for_date, workers=args.workers,
                     source=args.source, relax=args.relax)
    return 0 if res is not None else 1


if __name__ == "__main__":
    sys.exit(main())
