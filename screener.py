#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
==============================================================================
集合竞价选股引擎（A股主板）
==============================================================================
数据源：免费公开行情接口，无需 token / 无需开户
       主源 东方财富 · 备源 腾讯+新浪（不可达时自动降级）

运行时机：交易日 09:19 之前启动（脚本自动等到 09:20 / 09:24 / 09:25 抓快照）

【六个条件组】
  1) 9:25竞价金额 / 9:20竞价金额  > 12 倍
  2) 9:25竞价价格 >= 9:24竞价价格
  3) -1.5% < 今日竞价涨跌幅 < 6%，且 9:25竞价金额 > 350万
  4) 3.5% < 昨日换手率 < 20%，且 昨日收盘价 > 60日均线
  5) 前30日出现过涨停；股价 < 50元；市值 < 120亿；主板；非ST
  6) 上上个交易日涨跌幅 < 6%，且 上上日成交额 / 昨日成交额 > 110%

【用法】
  python screener.py live                      # 盘中实时（9:20 / 9:24 / 9:25 三次快照）
  python screener.py live --now                # 不等时间，立即抓三次（间隔12秒，盘前演练）
  python screener.py now                       # 立即抓一次快照跑全流程（验证代码用）
  python screener.py replay --snap-dir output/snapshots/20261016   # 用已存快照复盘
  python screener.py live --source tencent     # 强制用备用源
==============================================================================
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

import datasource as ds
from datasource import APP_DIR, LOG_DIR, OUT_DIR, DataSource

SNAP_DIR = OUT_DIR / "snapshots"
CONFIG_FILE = APP_DIR / "config.json"

# 内置默认配置：config.json 不存在时自动落盘生成，客户可改
DEFAULT_CONFIG = {
    "min_amount_ratio_925_920": 12.0,
    "require_price_925_ge_924": True,
    "auction_chg_min": -1.5,
    "auction_chg_max": 6.0,
    "min_auction_amount": 3500000,
    "min_prev_turnover": 3.5,
    "max_prev_turnover": 20.0,
    "ma_period": 60,
    "include_prev_in_ma": True,
    "zt_lookback_days": 30,
    "zt_threshold_pct": 9.8,
    "include_prev_day_in_zt": True,
    "max_price": 50.0,
    "max_market_cap": 12000000000,
    "market_cap_field": "float_mv",
    "main_board_only": True,
    "exclude_st": True,
    "t2_chg_max": 6.0,
    "min_t2_over_t1_amount_ratio": 1.10,
    # ★ 以下三项是「防限流」参数，与 config.json 必须保持一致。
    # 2026-10-19 事故复盘：DEFAULT_CONFIG 里 page_sleep 曾长期留着 0.06，
    # 客户一旦删掉 config.json 就会退回 60 页 × 0.06s ≈ 16.7 次/秒，
    # 直接被东财判定为爬虫。改这里之前先想清楚后果。
    "page_size": 100,
    "page_sleep": 0.5,
    "kline_limit": 130,
    "max_candidates": 500,
    "http_retries": 5,
    "http_timeout": 20,
    # 预筛与高频采样（v1.2 主路径）
    "prefilter_relax": 1.25,
    # 0920 最密：条件1 的 9:20 累计额无法事后补，这一天只有这一次机会
    "collect_gap_0920": 1.5,
    "collect_gap_0924": 2.0,
    # 0925 是 9:25–9:29:50 的静止窗，快照冻结，密采没有意义
    "collect_gap_0925": 4.0,
    # 东财风控（v1.2 起仅兜底路径使用，只会调小不会调大）
    "detail_retries": 3,
    "detail_max_consec_fail": 6,
    "em_min_interval": 2.0,
}

# 主板代码前缀：沪 600/601/603/605，深 000/001/002/003
MAIN_BOARD_PREFIX = ("600", "601", "603", "605", "000", "001", "002", "003")


# ============================================================================
# 筛选漏斗
# ============================================================================
class Funnel:
    def __init__(self):
        self.steps: list[tuple[str, int]] = []

    def add(self, label: str, n: int):
        self.steps.append((label, n))

    def render(self) -> str:
        if not self.steps:
            return ""
        w = max(len(s[0]) for s in self.steps)
        out = []
        for i, (label, n) in enumerate(self.steps):
            mark = "  " if i == 0 else "→ "
            out.append(f"  {mark}{label:<{w}}  {n:>6}")
        return "\n".join(out)


# ============================================================================
# 条件判断
# ============================================================================
def _is_main_board(code: str) -> bool:
    return str(code).startswith(MAIN_BOARD_PREFIX)


def _is_st(name: str) -> bool:
    return "ST" in str(name).upper()


def stage_spot(df: pd.DataFrame, cfg: dict, funnel: Funnel,
               source_label: str | None = None) -> pd.DataFrame:
    """第一阶段：快照即可判定的条件（板块/ST/竞价涨幅/竞价金额/股价/市值）。

    source_label: 输入快照的覆盖范围。★ 默认写「全市场A股」只在真的喂了
    全市场快照时才对。走候选池模式时输入只有池内几百只，若仍写「全市场A股」，
    漏斗第一行会显示「全市场A股 100」，看起来像漏了 5400 只 —— 2026-10-20
    就被这个标签误导过。调用方必须显式传「候选池快照」。
    """
    funnel.add(source_label or "全市场A股", len(df))
    d = df.copy()

    if cfg["main_board_only"]:
        d = d[d["code"].map(_is_main_board)]
    if cfg["exclude_st"]:
        d = d[~d["name"].map(_is_st)]
    funnel.add("主板 & 非ST", len(d))

    # ---- 条件3后半：竞价涨跌幅区间 ----
    d = d.dropna(subset=["price", "prev_close"])
    d = d[d["prev_close"] > 0]
    d["auc_chg"] = (d["price"] / d["prev_close"] - 1.0) * 100.0
    d = d[(d["auc_chg"] > cfg["auction_chg_min"]) & (d["auc_chg"] < cfg["auction_chg_max"])]
    funnel.add(f"竞价涨跌幅 ({cfg['auction_chg_min']}%, {cfg['auction_chg_max']}%)", len(d))

    # ---- 条件3前半：竞价金额 ----
    d = d[d["amount"] >= cfg["min_auction_amount"]]
    funnel.add(f"9:25竞价金额 > {cfg['min_auction_amount']/1e4:.0f}万", len(d))

    # ---- 条件5静态项：股价 / 市值 ----
    d = d[d["price"] < cfg["max_price"]]
    funnel.add(f"股价 < {cfg['max_price']:.0f}元", len(d))
    cap_col = cfg["market_cap_field"]
    d = d[d[cap_col] < cfg["max_market_cap"]]
    funnel.add(f"市值 < {cfg['max_market_cap']/1e8:.0f}亿（{cap_col}）", len(d))
    return d


def stage_auction(d: pd.DataFrame, s920: pd.DataFrame, s924: pd.DataFrame,
                  cfg: dict, funnel: Funnel) -> pd.DataFrame:
    """第二阶段：需要 9:20 / 9:24 快照的条件（条件1、条件2）。"""
    d = d.copy()
    d["amount_920"] = s920["amount"].reindex(d.index)
    d["price_920"] = s920["price"].reindex(d.index)
    d["price_924"] = s924["price"].reindex(d.index)

    # ---- 条件1：金额放大倍数 ----
    d = d.dropna(subset=["amount_920"])
    zero = d["amount_920"] <= 0
    if zero.any():
        print(f"    [警告] {int(zero.sum())} 只股票的 9:20 竞价金额为 0，"
              f"无法计算条件1，已排除")
    d = d[~zero]
    d["amt_ratio"] = d["amount"] / d["amount_920"]
    d = d[d["amt_ratio"] > cfg["min_amount_ratio_925_920"]]
    funnel.add(f"9:25/9:20 金额 > {cfg['min_amount_ratio_925_920']:g}倍", len(d))

    # ---- 条件2：竞价价格上行 ----
    if cfg["require_price_925_ge_924"]:
        d = d.dropna(subset=["price_924"])
        d = d[d["price"] >= d["price_924"]]
        funnel.add("9:25价 >= 9:24价", len(d))
    return d


def _filter_by_pool(d: pd.DataFrame, pool: dict, funnel: Funnel) -> pd.DataFrame:
    """用 T-1 晚预筛好的候选池过滤，跳过日K拉取。

    条件 4/5/6 全部基于「昨日及之前」的数据，已在收盘后算完（见 prep.py）。
    这里只做交集，并把池子里的历史字段并进来供结果展示。
    好处：早上 9:26 出结果时不再需要拉 1900 只日K，省掉 1~3 分钟和数百次请求。
    """
    pmap = {str(it["code"]): it for it in (pool or {}).get("items", [])}
    if d.empty or not pmap:
        funnel.add("条4/5/6（预筛池）", 0)
        return d.iloc[0:0].copy()

    keep = d.index.intersection(list(pmap.keys()))
    funnel.add("条4/5/6（预筛池）", len(keep))
    if not len(keep):
        return d.iloc[0:0].copy()

    out = d.loc[keep].copy()
    cols = ["prev_date", "prev_close_d", "prev_turnover", "ma", "ma_bias_pct",
            "last_zt_date", "t2_date", "t2_chg_pct", "t2_amount", "t1_amount",
            "t2_over_t1"]
    for c in cols:
        out[c] = [pmap[str(code)].get(c) for code in keep]
    return out


def stage_history(d: pd.DataFrame, src: DataSource, cfg: dict,
                  today_str: str, funnel: Funnel,
                  pool: dict | None = None) -> pd.DataFrame:
    """第三阶段：拉日K逐只校验 条件4 / 条件5(涨停) / 条件6。

    pool 不为 None 时直接用预筛池，完全不发请求 —— 这是 v1.2 的主路径。
    """
    if pool is not None:
        return _filter_by_pool(d, pool, funnel)

    if d.empty:
        funnel.add("昨日换手率区间", 0)
        funnel.add("昨收 > MA", 0)
        funnel.add("前30日有涨停", 0)
        funnel.add("历史量价条件", 0)
        return d

    if len(d) > cfg["max_candidates"]:
        print(f"    [警告] 候选 {len(d)} 只超过上限 {cfg['max_candidates']}，"
              f"仅处理前 {cfg['max_candidates']} 只（可调大 config.max_candidates）")
        d = d.head(cfg["max_candidates"])

    total = len(d)
    kept = []
    n_turn = n_ma = n_zt = 0
    ma_p = int(cfg["ma_period"])
    lb = int(cfg["zt_lookback_days"])

    for i, (code, row) in enumerate(d.iterrows(), 1):
        if i % 20 == 0 or i == total:
            print(f"    日K校验 {i}/{total} ...", end="\r", flush=True)

        kdf = src.kline(code, row["market"])
        if kdf is None or kdf.empty:
            continue
        # 剔除今日未完成K线，保证最后一根 = 昨日(T-1)
        kdf = kdf[kdf["date"] < today_str].reset_index(drop=True)
        if len(kdf) < max(ma_p, lb) + 2:
            continue

        y = kdf.iloc[-1]        # 昨日 T-1
        t2 = kdf.iloc[-2]       # 上上个交易日 T-2

        # ---- 条件4a：昨日换手率 ----
        y_turn = float(y["turnover"]) if pd.notna(y["turnover"]) else None
        if y_turn is None or not (cfg["min_prev_turnover"] < y_turn < cfg["max_prev_turnover"]):
            continue
        n_turn += 1

        # ---- 条件4b：昨日收盘价 > MA ----
        ma_slice = kdf["close"].iloc[-ma_p:] if cfg["include_prev_in_ma"] \
            else kdf["close"].iloc[-(ma_p + 1):-1]
        ma_val = float(ma_slice.mean())
        y_close = float(y["close"])
        if not (y_close > ma_val):
            continue
        n_ma += 1

        # ---- 条件5b：前 N 日有涨停 ----
        zt_win = kdf["chg_pct"].iloc[-lb:] if cfg["include_prev_day_in_zt"] \
            else kdf["chg_pct"].iloc[-(lb + 1):-1]
        hit_pos = list(zt_win.index[zt_win >= cfg["zt_threshold_pct"]])
        if not hit_pos:
            continue
        n_zt += 1
        last_zt_date = kdf["date"].iloc[hit_pos[-1]]

        # ---- 条件6a：上上个交易日涨跌幅 < 6% ----
        t2_chg = float(t2["chg_pct"])
        if not (t2_chg < cfg["t2_chg_max"]):
            continue

        # ---- 条件6b：T-2 成交额 / T-1 成交额 > 110% ----
        t2_amt, t1_amt = float(t2["amount"]), float(y["amount"])
        if t1_amt <= 0:
            continue
        t2_over_t1 = t2_amt / t1_amt
        if not (t2_over_t1 > cfg["min_t2_over_t1_amount_ratio"]):
            continue

        rec = row.to_dict()
        rec.update({
            "prev_date": y["date"], "prev_close_d": y_close,
            "prev_turnover": y_turn, "ma": round(ma_val, 3),
            "ma_bias_pct": round((y_close / ma_val - 1) * 100, 2),
            "last_zt_date": last_zt_date, "t2_date": t2["date"],
            "t2_chg_pct": t2_chg, "t2_amount": t2_amt, "t1_amount": t1_amt,
            "t2_over_t1": round(t2_over_t1, 3),
        })
        kept.append(rec)

    print(" " * 46, end="\r")
    funnel.add("昨日换手率区间", n_turn)
    funnel.add("昨收 > MA", n_ma)
    funnel.add("前30日有涨停", n_zt)
    funnel.add("历史量价（条6）", len(kept))

    if not kept:
        return d.iloc[0:0].copy()
    return pd.DataFrame(kept).set_index("code", drop=False)


def screen(snaps: dict, src: DataSource, cfg: dict, today_str: str,
           pool: dict | None = None):
    """snaps: {'0920': df, '0924': df, '0925': df}

    pool: T-1 晚预筛的候选池（prep.py 产出）。传入即跳过日K拉取。
    """
    funnel = Funnel()
    # 同 collector._partial_output：只有真的只喂了池内那一批，才标「候选池快照」
    _n925 = 0 if snaps.get("0925") is None else len(snaps["0925"])
    d = stage_spot(snaps["0925"], cfg, funnel,
                   source_label=("候选池快照" if (pool and _n925 < 1000) else None))
    d = stage_auction(d, snaps["0920"], snaps["0924"], cfg, funnel)
    res = stage_history(d, src, cfg, today_str, funnel, pool=pool)
    return res, funnel


# ============================================================================
# 快照存取
# ============================================================================
def save_snapshot(df: pd.DataFrame, tag: str, ts: datetime) -> Path:
    day_dir = SNAP_DIR / f"{ts:%Y%m%d}"
    day_dir.mkdir(parents=True, exist_ok=True)
    fp = day_dir / f"spot_{tag}_{ts:%H%M%S}.json"
    payload = {
        "tag": tag, "captured_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(df),
        "rows": json.loads(df.to_json(orient="records", force_ascii=False)),
    }
    fp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return fp


def load_snapshot(fp: Path) -> pd.DataFrame:
    payload = json.loads(Path(fp).read_text(encoding="utf-8"))
    df = pd.DataFrame(payload["rows"])
    for c in ["price", "chg_pct", "volume", "amount", "turnover",
              "prev_close", "total_mv", "float_mv"]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    return df.set_index("code", drop=False)


def sanity_check(s_a: pd.DataFrame, s_c: pd.DataFrame) -> str | None:
    """对比首尾两次快照，判断是否真的处在动态更新的竞价时段。"""
    common = s_a.index.intersection(s_c.index)
    if len(common) == 0:
        return "两次快照无交集，数据异常"
    a, c = s_a.loc[common, "amount"], s_c.loc[common, "amount"]
    same = (a == c).mean()
    if same > 0.95:
        return (f"首尾两次快照的成交额有 {same:.0%} 完全相同 —— 当前很可能不是"
                f"集合竞价时段（竞价数据应当动态变化），本次结果不可信")
    return None


def wait_until(hh: int, mm: int, ss: int):
    target = datetime.now().replace(hour=hh, minute=mm, second=ss, microsecond=0)
    while True:
        left = (target - datetime.now()).total_seconds()
        if left <= 0:
            return
        time.sleep(min(left, 1.0))


# ============================================================================
# 输出
# ============================================================================
def render_result(res: pd.DataFrame, funnel: Funnel, meta: dict,
                  warn: str | None = None) -> str:
    L = ["=" * 100,
         f"  集合竞价选股结果   |   生成于 {datetime.now():%Y-%m-%d %H:%M:%S}"]
    for k, v in meta.items():
        L.append(f"     快照{k}: {v}")
    L.append("=" * 100)
    L.append("筛选漏斗：")
    L.append(funnel.render())
    L.append("=" * 100)
    if warn:
        L.append(f"  [!] {warn}")
        L.append("=" * 100)

    if res is None or res.empty:
        L.append("  无股票满足全部条件。")
        L.append("=" * 100)
        return "\n".join(L)

    L.append(f"命中 {len(res)} 只：")
    L.append("")
    L.append(f"{'代码':<8}{'名称':<10}{'竞价价':>8}{'竞价涨幅':>9}{'竞价金额万':>11}"
             f"{'9:20万':>10}{'放大':>7}{'昨换手':>8}{'昨收':>8}{'MA60':>8}"
             f"{'偏离%':>7}{'近30涨停日':>12}{'T-2涨跌':>9}{'T2/T1量':>9}")
    L.append("-" * 100)
    for _, r in res.iterrows():
        name = str(r["name"])[:8]
        L.append(
            f"{r['code']:<8}{name:<10}{r['price']:>8.2f}{r['auc_chg']:>9.2f}"
            f"{r['amount']/1e4:>11.0f}{r['amount_920']/1e4:>10.0f}"
            f"{r['amt_ratio']:>7.1f}{r['prev_turnover']:>8.2f}"
            f"{r['prev_close_d']:>8.2f}{r['ma']:>8.2f}{r['ma_bias_pct']:>7.2f}"
            f"{str(r['last_zt_date']):>12}{r['t2_chg_pct']:>9.2f}{r['t2_over_t1']:>9.2f}"
        )
    L.append("=" * 100)
    L.append(f"  数据基准：昨日 = {res['prev_date'].iloc[0]}，"
             f"上上个交易日 = {res['t2_date'].iloc[0]}")
    return "\n".join(L)


def export_csv(res: pd.DataFrame, ts: datetime) -> Path | None:
    if res is None or res.empty:
        return None
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fp = OUT_DIR / f"选股结果_{ts:%Y%m%d_%H%M%S}.csv"
    cols = ["code", "name", "price", "auc_chg", "amount", "amount_920", "amt_ratio",
            "prev_turnover", "prev_close_d", "ma", "ma_bias_pct", "last_zt_date",
            "t2_chg_pct", "t2_amount", "t1_amount", "t2_over_t1",
            "prev_date", "t2_date", "float_mv", "total_mv"]
    res[[c for c in cols if c in res.columns]].to_csv(fp, index=False, encoding="utf-8-sig")
    return fp


def save_latest(res: pd.DataFrame, csv_fp: Path | None, day: str) -> Path:
    """把当日结果另存为 output/最新结果.txt —— 客户双击「查看最新结果.bat」看这个。
    ★ 结果为空时也要写，否则客户会看到上一次的旧结果并误以为是最新的。"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fp = OUT_DIR / "最新结果.txt"
    L = [f"竞价选股结果   {day}", "=" * 64, ""]
    if res is None or res.empty:
        L.append("今日无股票满足全部条件。")
    else:
        L.append(f"命中 {len(res)} 只：")
        L.append("")
        for _, r in res.iterrows():
            L.append(f"{r['code']}  {str(r['name'])[:8]:<9} "
                     f"竞价{r['price']:>7.2f} ({r['auc_chg']:+6.2f}%)  "
                     f"金额{r['amount']/1e4:>8.0f}万  "
                     f"放大{r['amt_ratio']:>5.1f}倍  "
                     f"昨换手{r['prev_turnover']:>5.2f}%")
    if csv_fp is not None:
        L.append("")
        L.append(f"完整明细：{csv_fp.name}")
    fp.write_text("\n".join(L), encoding="utf-8-sig")
    return fp


# ============================================================================
# 主流程
# ============================================================================
def load_cfg() -> dict:
    """读取 config.json；文件不存在则自动写出默认配置（客户可直接编辑）。"""
    if not CONFIG_FILE.exists():
        CONFIG_FILE.write_text(
            json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f">>> 已生成默认配置：{CONFIG_FILE}")
        return dict(DEFAULT_CONFIG)
    raw = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit("config.json 格式错误：应为 JSON 对象")
    cfg = dict(DEFAULT_CONFIG)
    cfg.update({k: v for k, v in raw.items() if not k.startswith("_")})  # 缺项自动补默认
    return cfg


def log_line(msg: str):
    """追加写入 output/logs/run_YYYYMMDD.log，方便客户回传排查。

    用 utf-8 而非 utf-8-sig：追加模式下 sig 会每行重复写 BOM。
    Win10+ 记事本能正确识别无 BOM 的 UTF-8，中文 Windows 下显示正常。
    """
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_DIR / f"run_{datetime.now():%Y%m%d}.log", "a",
                  encoding="utf-8") as f:
            f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")
    except Exception:
        pass


def _grab_spot(cfg: dict, src: DataSource, tag: str):
    """抓一次全市场快照：主源失败 → 自动降级备用源 → 仍失败返回 (None, None)。

    ★ 绝不向上抛异常。竞价窗口只有 5 分钟，一次网络抖动不能让整场作废。
    修复背景（2026-10-19）：原实现直接 src.spot()，东财某一页请求失败即
    抛 RuntimeError，被 PyInstaller 顶层捕获后进程直接退出，日志一行不留，
    客户只看到"没结果"。
    """
    try:
        df = src.spot()
        if df is not None and len(df) > 0:
            return df, src
        log_line(f"快照[{tag}] {src.name} 返回空数据")
    except Exception as e:
        log_line(f"快照[{tag}] {src.name} 失败: {type(e).__name__}: {e}")
        print(f"    [!] 快照[{tag}] {src.name} 抓取失败（{type(e).__name__}），切换备用源 ...")

    try:
        alt = ds.build_source(cfg, prefer="tencent")
        df = alt.spot()
        if df is not None and len(df) > 0:
            log_line(f"快照[{tag}] 已降级到 {alt.name}，n={len(df)}")
            print(f"    [OK] 已降级到备用源 {alt.name}，n={len(df)}")
            return df, alt
    except Exception as e:
        log_line(f"快照[{tag}] 备用源也失败: {type(e).__name__}: {e}")
        print(f"    [X] 快照[{tag}] 备用源同样失败（{type(e).__name__}）")

    return None, None


def write_fail(day: str, reason: str, preserve_today: bool = True):
    """抓取失败时写一份说明 —— 但**不覆盖当天已经成功产出的结果**。

    ★ 双轨交付的关键细节（2026-10-19）：09:19 实时轨很可能已经出了今天的
    正式结果；09:26 回算轨若因东财分笔明细抽风而失败，把 09:25 的好结果
    覆盖成「失败」对客户是净损失。所以失败说明另存一个文件，原结果保留。
    例外：现有结果不是今天的（隔夜旧结果），必须覆盖 —— 不能让客户看着
    昨天的结果以为是今天的。
    """
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        fp = OUT_DIR / "最新结果.txt"

        if preserve_today and fp.exists():
            try:
                old = fp.read_text(encoding="utf-8-sig")
            except Exception:
                old = ""
            is_today = (day in old[:200]) and ("失败" not in old[:200])
            if is_today:
                alt = OUT_DIR / "回算未完成说明.txt"
                alt.write_text(
                    "\n".join(["本次回算未完成   " + day, "=" * 64, "",
                               "今天已经有一份结果（output\\最新结果.txt），已保留，",
                               "没有被本次失败覆盖。", "",
                               "本次失败原因：", "  " + reason, "",
                               "你可以：",
                               "  1. 直接看 output\\最新结果.txt（09:19 实时轨的结果）",
                               "  2. 过十几分钟再双击 3-立即选股.bat 重试回算",
                               "  3. 把 output\\logs\\ 里的日志发给技术支持", "",
                               "（本文件可删除，不影响已有结果）"]),
                    encoding="utf-8-sig")
                return alt

        fp.write_text(
            "\n".join([f"竞价选股失败   {day}", "=" * 64, "",
                       "今日未产生选股结果。", "", f"原因：{reason}", "",
                       "详细日志：output\\logs\\"]),
            encoding="utf-8-sig")
        return fp
    except Exception:
        return None


def run_live(cfg: dict, src: DataSource, immediate: bool = False):
    today_str = datetime.now().strftime("%Y-%m-%d")
    plan = [("0920", 9, 20, 2), ("0924", 9, 24, 2), ("0925", 9, 25, 6)]
    snaps, meta = {}, {}
    active_src = src

    for idx, (tag, hh, mm, ss) in enumerate(plan):
        if immediate:
            if idx > 0:
                time.sleep(12)
            print(f">>> [演练] 立即抓取快照 [{tag}] ...")
        else:
            print(f">>> 等待 {hh:02d}:{mm:02d}:{ss:02d} 抓取快照 [{tag}] ...")
            wait_until(hh, mm, ss)

        df, used = _grab_spot(cfg, active_src, tag)
        if df is None:
            msg = f"{hh:02d}:{mm:02d} 快照抓取失败，主源与备用源均未成功"
            log_line(f"!! {tag} 快照失败，任务中止：{msg}")
            print(f"    [X] 快照 [{tag}] 彻底失败，任务中止")
            write_fail(today_str, msg + "（网络异常或行情接口限流）")
            raise SystemExit(2)
        if used is not active_src:
            active_src = used

        ts = datetime.now()
        snaps[tag] = df
        fp = save_snapshot(df, tag, ts)
        meta[tag] = f"{ts:%H:%M:%S}  n={len(df)}"
        print(f"    已存 {fp.name}")

    warn = sanity_check(snaps["0920"], snaps["0925"])
    print("\n>>> 开始选股 ...")
    res, funnel = screen(snaps, active_src, cfg, today_str)
    text = render_result(res, funnel, meta, warn)
    print(text)
    log_line(f"命中 {0 if res is None else len(res)} 只\n{text}")
    fp = export_csv(res, datetime.now())
    if fp:
        print(f"\n结果已导出：{fp}")
    print(f"最新结果：{save_latest(res, fp, today_str)}")
    return res


def run_now(cfg: dict, src: DataSource):
    ts = datetime.now()
    print(f">>> 立即抓取全市场快照 @ {ts:%H:%M:%S} ...")
    snap = src.spot()
    save_snapshot(snap, "now", ts)
    snaps = {"0920": snap, "0924": snap, "0925": snap}
    res, funnel = screen(snaps, src, cfg, ts.strftime("%Y-%m-%d"))
    warn = ("非竞价时段运行（三次快照同源）：条件1/2 恒成立，条件3 口径为收盘数据。"
            "此模式仅用于验证代码链路，结果无实战意义。")
    print(render_result(res, funnel, {"now": f"{ts:%H:%M:%S} n={len(snap)}"}, warn))
    fp = export_csv(res, ts)
    if fp:
        print(f"\n结果已导出：{fp}")
    print(f"最新结果：{save_latest(res, fp, ts.strftime('%Y-%m-%d'))}")
    return res


def run_replay(cfg: dict, src: DataSource, snap_dir: Path,
               warn: str | None = None):
    files = sorted(Path(snap_dir).glob("spot_*.json"))
    by_tag = {f.name.split("_")[1]: f for f in files}
    need = ["0920", "0924", "0925"]
    missing = [t for t in need if t not in by_tag]
    if missing:
        raise SystemExit(f"快照目录 {snap_dir} 缺少 {missing}；实际有 {list(by_tag)}")
    snaps = {t: load_snapshot(by_tag[t]) for t in need}
    meta = {t: by_tag[t].name for t in need}
    res, funnel = screen(snaps, src, cfg, datetime.now().strftime("%Y-%m-%d"))
    print(render_result(res, funnel, meta, warn))
    fp = export_csv(res, datetime.now())
    if fp:
        print(f"\n结果已导出：{fp}")
    print(f"最新结果：{save_latest(res, fp, datetime.now().strftime('%Y-%m-%d'))}")
    return res


def run_probe(cfg: dict, src: DataSource, code: str):
    """打印单只股票在两个源上的原始字段，用于实盘对账 / 自检。"""
    print(f">>> 字段探测 {code}（当前源：{src.name}）")
    d = src.spot()
    if code in d.index:
        row = d.loc[code]
        for k in ["code", "name", "market", "price", "prev_close", "chg_pct",
                  "amount", "turnover", "float_mv", "total_mv"]:
            print(f"    {k:<12}= {row.get(k)}")
    else:
        print(f"    {code} 不在快照中")
    k = src.kline(code, d.loc[code, "market"] if code in d.index else 1)
    if k is not None and not k.empty:
        print("    最近 3 根日K：")
        print(k.tail(3).to_string(index=False))


def run_selftest(cfg: dict, src: DataSource) -> int:
    """环境自检：目录读写 / 配置 / 数据源 / 日K / 系统时间。
    最后一行输出纯 ASCII 标记供 .bat 判定（中文在 bat 里匹配不可靠）。"""
    ok = True
    print("=" * 70)
    print("  环境自检")
    print("=" * 70)

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        t = OUT_DIR / "_wtest.tmp"
        t.write_text("ok", encoding="utf-8")
        t.unlink()
        print(f"[OK]   输出目录可写 : {OUT_DIR}")
    except Exception as e:
        ok = False
        print(f"[FAIL] 输出目录不可写 : {e}")

    print(f"[OK]   配置已加载     : {len(cfg)} 项 -> {CONFIG_FILE}")

    try:
        snap = src.spot()
        n = len(snap)
        if n > 3000:
            print(f"[OK]   数据源 {src.name:<9}: 全市场快照 {n} 只")
        else:
            ok = False
            print(f"[FAIL] 快照数量异常   : 仅 {n} 只（应 >3000）")
    except Exception as e:
        ok = False
        print(f"[FAIL] 数据源不可用   : {type(e).__name__} {e}")

    try:
        k = src.kline("600000", 1)
        if k is not None and len(k) > 60:
            print(f"[OK]   日K数据可用    : 600000 共 {len(k)} 根")
        else:
            ok = False
            print("[FAIL] 日K数据异常")
    except Exception as e:
        ok = False
        print(f"[FAIL] 日K不可用     : {type(e).__name__} {e}")

    now = datetime.now()
    print(f"[INFO] 本机时间       : {now:%Y-%m-%d %H:%M:%S} 星期{now.isoweekday()}")
    print("=" * 70)
    print("SELF_TEST_RESULT=" + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


def main():
    ds.setup_std_encoding()
    try:
        return _cli()
    except SystemExit:
        raise
    except BaseException:
        import traceback
        tb = traceback.format_exc()
        log_line("!! 未捕获异常，进程退出\n" + tb)
        last = (tb.strip().splitlines() or ["未知错误"])[-1][:180]
        write_fail(datetime.now().strftime("%Y-%m-%d"), "程序运行异常：" + last)
        print("\n[!] 程序异常退出，详情见 output\\logs\\ 与 最新结果.txt")
        print(tb)
        return 1


def _cli():
    ap = argparse.ArgumentParser(description="集合竞价选股引擎")
    ap.add_argument("--source", default="auto",
                    choices=["auto", "eastmoney", "tencent"], help="数据源")
    sub = ap.add_subparsers(dest="mode", required=True)

    p_live = sub.add_parser("live", help="盘中实时（9:20/9:24/9:25 三次快照）")
    p_live.add_argument("--now", action="store_true", help="不等时间，立即抓三次（演练）")
    sub.add_parser("now", help="立即抓一次快照跑全流程（验证代码用）")
    p_re = sub.add_parser("replay", help="用已保存快照复盘")
    p_re.add_argument("--snap-dir", required=True)
    p_pr = sub.add_parser("probe", help="打印单只股票字段（自检）")
    p_pr.add_argument("--code", required=True)
    sub.add_parser("selftest", help="环境自检（供 .bat 调用）")

    args = ap.parse_args()
    cfg = load_cfg()
    src = ds.build_source(cfg, prefer=args.source)
    print(f">>> 数据源：{src.name}")
    log_line(f"启动 mode={args.mode} source={src.name}")

    if args.mode == "selftest":
        return run_selftest(cfg, src)
    if args.mode == "live":
        run_live(cfg, src, immediate=args.now)
    elif args.mode == "now":
        run_now(cfg, src)
    elif args.mode == "replay":
        run_replay(cfg, src, Path(args.snap_dir))
    elif args.mode == "probe":
        run_probe(cfg, src, args.code)
    return 0


if __name__ == "__main__":
    sys.exit(main())
