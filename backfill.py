# -*- coding: utf-8 -*-
"""
盘后完整选股 —— 用当日真实竞价数据还原全部 6 组条件。

=======================================================================
【原理】为什么不必盘中蹲点（2026-10-19 实测确认）
=======================================================================
东财分笔明细 /api/qt/stock/details/get 覆盖 09:15~09:25，每条形如：

    09:20:06,9.73,413,0,4
    09:24:57,9.73,1625,0,4
    09:25:00,9.72,1710,150,1

字段：[0]时间 [1]价格 [2]**累计竞价匹配量(手)** [3]笔数 [4]性质(4=竞价虚拟)

· 该累计量单调递增，09:25:00 达到最终撮合量；
· 实测 600000：09:25:00 = 1710 手，与分时 09:30 首点 1710 手 / 1662120 元
  **逐位一致**（1710×100×9.72 = 1,662,120）；
· 09:24 的价格在 09:24:5x 的记录里，09:20 的累计量在 09:20:0x 的记录里。

因此 9:20 / 9:24 / 9:25 三要素全部可精确还原，条件1、2、3 不再依赖盘中抓取。
注意：09:15~09:25 之间【价格】有、但【成交量/额】字段恒为 0 —— 那是虚拟撮合，
真实撮合只在 09:25:00 一次性产生；所谓"竞价金额"用的是累计匹配量折算。
=======================================================================
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

import datasource as ds
from datasource import APP_DIR, OUT_DIR, UA
from screener import (Funnel, _is_main_board, _is_st, load_cfg, log_line,
                      stage_history)

DETAIL_PATH = "/api/qt/stock/details/get"

# 每线程独立 Session（requests.Session 的连接池不是线程安全的）
_TL = threading.local()

# 流通股本缓存（code -> 股数），由一次腾讯快照填充，供所有线程反推换手率
_SHARES: dict = {}

# 当日日K缓存（"源:code" -> {"date": [...], "close": [...] ...}；{} 表示本次拉取失败）
# 条件4/5/6 只用 T-1 及更早的日K，同一交易日内不会变，所以完全可以复用。
#
# ★ 键必须带数据源前缀（2026-10-19 修，精度级 bug）：
#   原来键只有股票代码，导致 recheck_history_exact() 用 which="em" 去复核时，
#   直接命中了 stage_history_par() 用 which="tx" 写进去的缓存 —— 一次东财请求
#   都没发，却按「东财精确值」计入 n_em，并打印「东财精确通过 N 只」。
#   实际后果：声称做了「精确复核、排除估算误差翻转」，其实压根没复核，
#   条件6 的 110% 边界翻转从未被兜住。属于「把估算当精确值静默使用」。
_KLINE_CACHE: dict = {}

_CK_TX = "tx"          # 腾讯前复权（主用）
_CK_EM = "em"          # 东财原始字段（精确复核）


def _ck(code, which: str) -> str:
    """缓存键：必须区分数据源，否则 em 复核会读到 tx 的数据。"""
    return "%s:%s" % (which, str(code))


def _cache_fp(day: str) -> Path:
    return OUT_DIR / "cache" / ("kline_%s.json.gz" % day.replace("-", ""))


def load_kline_cache(day: str):
    """把当天已抓过的日K读进内存，避免重复请求。

    ★ 为什么必须有（2026-10-19 实测）：连续跑几次全市场后，腾讯日K 接口
    开始返回 HTTP 501（WAF 验证页），单只耗时从 0.04s 劣化到 6s，1900 只
    要跑 20 分钟。同一天重跑时如果还去逐只拉，等于自己加速被封。
    """
    fp = _cache_fp(day)
    if not fp.exists():
        return
    try:
        with gzip.open(str(fp), "rt", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data:
            # 磁盘文件是旧格式（裸代码键），内容一律来自腾讯主路径，
            # 读进来时补上 tx: 前缀；已是新格式的键原样保留。
            _KLINE_CACHE.update(
                {k if ":" in str(k) else _ck(k, _CK_TX): v
                 for k, v in data.items()})
            print("    日K缓存命中 %d 只（%s），跳过重复请求" % (len(data), fp.name))
    except Exception as e:
        log_line("读取日K缓存失败 %s: %s" % (fp.name, e))


def save_kline_cache(day: str):
    # 只落盘腾讯主路径（tx:）—— 东财复核那批数量少且口径不同，
    # 混进同一个文件会让下次读到「以为都是 tx」的错误数据。
    ok = {str(k).split(":", 1)[1]: v for k, v in _KLINE_CACHE.items()
          if v and str(k).startswith(_CK_TX + ":")}
    if not ok:
        return
    fp = _cache_fp(day)
    try:
        fp.parent.mkdir(parents=True, exist_ok=True)
        tmp = fp.with_name(fp.name + ".tmp")
        with gzip.open(str(tmp), "wt", encoding="utf-8") as f:
            json.dump(ok, f, ensure_ascii=False)
        os.replace(str(tmp), str(fp))
    except Exception as e:
        log_line("写入日K缓存失败: %s" % e)


def _cache_shares(df: pd.DataFrame):
    if df is None or df.empty or "float_mv" not in df.columns:
        return
    ok = df["price"].notna() & (df["price"] > 0) & df["float_mv"].notna()
    for c, v in zip(df.loc[ok, "code"], df.loc[ok, "float_mv"] / df.loc[ok, "price"]):
        _SHARES[str(c)] = float(v)


def _thread_session() -> requests.Session:
    s = getattr(_TL, "sess", None)
    if s is None:
        s = ds.make_session()
        _TL.sess = s
    return s


# ---------------------------------------------------------------- 分笔明细
def fetch_details(session: requests.Session, secid: str, cfg: dict):
    """拉取分笔明细（含 09:15~09:25 竞价段）。pos=-6000 覆盖全天。

    ★ 2026-10-19 改造（务必保留，改动前先读 memory 2026-10-19.md）：
    旧版 retries 默认 **30** 次 × 4 域名轮换，单只股票最多发 30 个请求，
    45 只候选就是上千次 —— 实测这是「客户机跑一次就被东财风控打进冷却期」
    的直接原因（社区阈值：1 分钟 ≥200 次即触发封禁）。

    新版三处收紧：
      1. 重试 3 次（config.detail_retries），不再暴力顶
      2. 每次请求前过 ds.em_throttle()，全局串行 + 间隔 ≥2s + 抖动
      3. 失败按「请求」计熔断（threshold=8），8 次连续失败即停整轮，
         而不是逐只硬顶到底
    """
    retries = max(1, int(cfg.get("detail_retries", 3)))
    timeout = int(cfg.get("http_timeout", 12))
    iv = float(cfg.get("em_min_interval", ds.EM_MIN_INTERVAL))
    for i in range(retries):
        if ds.EM_BREAKER.is_open():
            return None          # 熔断中：别再对每只股票硬顶
        host = ds.EM_DETAIL_HOSTS[i % len(ds.EM_DETAIL_HOSTS)]
        ds.em_throttle(iv)       # ★ 全局排队：绝不并发打东财
        try:
            r = session.get("https://%s%s" % (host, DETAIL_PATH),
                            params={"secid": secid, "fields1": "f1,f2,f3,f4",
                                    "fields2": "f51,f52,f53,f54,f55",
                                    "pos": "-6000"},
                            timeout=timeout)
            det = ((r.json() or {}).get("data") or {}).get("details") or []
            if det:
                ds.EM_BREAKER.mark_ok()
                return det
        except Exception:
            pass
        ds.EM_BREAKER.mark_fail()
    return None


def parse_auction(det):
    """从分笔明细还原竞价三要素。

    返回 dict 或 None：
      q920/p920  9:20 时点的累计匹配量(手) / 价格
      p924       9:24 时点的价格
      q925/p925  竞价段最后一条（= 9:25 撮合）的累计量与价格
    """
    if not det:
        return None
    rows = []
    for x in det:
        f = x.split(",")
        if len(f) < 3:
            continue
        t = f[0]
        if t > "09:25:59":       # 竞价段结束，后面是盘中连续竞价
            break
        try:
            rows.append((t, float(f[1]), float(f[2])))
        except Exception:
            continue
    if not rows:
        return None

    # 9:20 —— 取 09:20:00~09:20:59 第一条（最贴近 09:20:00 时点）；
    #         该窗口无记录时，退回 09:20 之前最后一条
    win = [r for r in rows if "09:20:00" <= r[0] <= "09:20:59"]
    if win:
        _, p920, q920 = win[0]
    else:
        before = [r for r in rows if r[0] < "09:20:00"]
        if not before:
            return None            # 9:20 前无任何竞价记录
        _, p920, q920 = before[-1]

    # 9:24 —— 09:24:00~09:24:59 最后一条；无则退回竞价段倒数第二条
    w924 = [r for r in rows if "09:24:00" <= r[0] <= "09:24:59"]
    if w924:
        p924 = w924[-1][1]
    elif len(rows) >= 2:
        p924 = rows[-2][1]
    else:
        p924 = rows[-1][1]

    # 9:25 撮合 —— 竞价段最后一条的累计量
    _, p925, q925 = rows[-1]
    return {"q920": q920, "p920": p920, "p924": p924, "q925": q925, "p925": p925}


# ---------------------------------------------------------------- 第一层
def stage_pre(d: pd.DataFrame, cfg: dict, f: Funnel) -> pd.DataFrame:
    """仅凭全市场快照即可判定的条件。竞价价用「今开」（= 9:25 撮合价）。"""
    f.add("全市场A股", len(d))
    d = d.copy()
    if cfg["main_board_only"]:
        d = d[d["code"].map(_is_main_board)]
    if cfg["exclude_st"]:
        d = d[~d["name"].map(_is_st)]
    f.add("主板 & 非ST", len(d))

    d = d.dropna(subset=["open", "prev_close"])
    d = d[(d["open"] > 0) & (d["prev_close"] > 0)]
    d["auc_chg"] = (d["open"] / d["prev_close"] - 1.0) * 100.0
    d = d[(d["auc_chg"] > cfg["auction_chg_min"]) &
          (d["auc_chg"] < cfg["auction_chg_max"])]
    f.add("今日竞价涨跌幅 (%.1f%%, %.1f%%)" % (cfg["auction_chg_min"],
                                              cfg["auction_chg_max"]), len(d))

    d = d[d["open"] < cfg["max_price"]]
    f.add("竞价价 < %.0f元" % cfg["max_price"], len(d))

    cap_col = cfg["market_cap_field"]
    d = d[d[cap_col].notna() & (d[cap_col] < cfg["max_market_cap"])]
    f.add("流通市值 < %.0f亿" % (cfg["max_market_cap"] / 1e8), len(d))
    return d


# ---------------------------------------------------------------- 第二层（并行日K）
def _thread_source(cfg: dict, which: str = "tx") -> ds.DataSource:
    """每线程独立数据源 —— requests.Session 的连接池不是线程安全的。

    which="tx" 腾讯（稳定，换手率由流通股本反推，成交额按量×均价估算）
    which="em" 东财（精确，但沙箱代理下是「窗口式封锁」，只用于少量复核）
    """
    key = "_src_" + which
    s = getattr(_TL, key, None)
    if s is None:
        if which == "tx":
            s = ds.TencentSource(cfg)
            s.use_sina = False     # 多线程禁用 akshare/新浪（py_mini_racer 非线程安全）
            if _SHARES:
                s._shares = dict(_SHARES)   # 复用主流程那次快照，避免每线程重拉
            else:
                s.spot()          # 填充 _shares（流通股本），否则换手率算不出来
        else:
            s = ds.EastMoneySource(cfg)
        setattr(_TL, key, s)
    return s


def _kline_cached(code, market, cfg, which: str = "tx"):
    """优先用当日缓存，未命中才真正请求。

    缓存粒度是「单只股票的原始日K」，不缓存判定结果 —— 这样客户改了
    config 阈值（换手率区间 / 均线周期 / 涨停回看天数）也能直接复用。
    """
    k = _ck(code, which)
    if k in _KLINE_CACHE:
        hit = _KLINE_CACHE[k]
        if not hit:
            return None                  # 本进程内已确认失败，不再重复请求
        df = pd.DataFrame(hit)
        for c in ("close", "chg_pct", "amount", "turnover"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    src = _thread_source(cfg, which)
    # ★ 必须传纯代码 code，不能传带源前缀的缓存键 k（"tx:600127"）。
    # 2026-10-18 引入 tx:/em: 前缀时这里漏改，`src.kline("tx:600127", ...)`
    # 请求到的是非法代码 → 全部返回 0 行 → 预筛 2272 只无一通过 →
    # stage_history_par 末尾判定「日K接口全部不可用」直接抛错。
    # 后果：2026-10-20 15:30 预筛失败，10-21 候选池缺失，采样退化为
    # 全市场 5574 只，条件4/5/6 静默不执行，输出 78 只假结果。
    # 实测对照：kline("tx:600127") → 0 行 / 2.97s；kline("600127") → 131 行 / 0.10s。
    df = src.kline(code, market)
    if df is None or df.empty:
        _KLINE_CACHE[k] = {}
        return None
    cols = [c for c in ("date", "close", "chg_pct", "amount", "turnover")
            if c in df.columns]
    _KLINE_CACHE[k] = df[cols].copy().to_dict(orient="list")
    return df


def _check_history_one(row, cfg, today_str, which: str = "tx"):
    """单只股票的条件4/5/6 校验。返回 (stage, rec)。

    stage: 0=日K无效 1=换手率过 2=MA过 3=涨停过 4=T2涨幅过 5=全过
    """
    code = row["code"]
    kdf = _kline_cached(code, row["market"], cfg, which)
    if kdf is None or kdf.empty:
        return 0, None
    kdf = kdf[kdf["date"] < today_str].reset_index(drop=True)
    ma_p = int(cfg["ma_period"])
    lb = int(cfg["zt_lookback_days"])
    if len(kdf) < max(ma_p, lb) + 2:
        return 0, None

    y = kdf.iloc[-1]        # 昨日 T-1
    t2 = kdf.iloc[-2]       # 上上个交易日 T-2

    # 条件4a：昨日换手率
    y_turn = float(y["turnover"]) if pd.notna(y["turnover"]) else None
    if y_turn is None or not (cfg["min_prev_turnover"] < y_turn
                              < cfg["max_prev_turnover"]):
        return 0, None

    # 条件4b：昨日收盘价 > MA
    ma_slice = (kdf["close"].iloc[-ma_p:] if cfg["include_prev_in_ma"]
                else kdf["close"].iloc[-(ma_p + 1):-1])
    ma_val = float(ma_slice.mean())
    y_close = float(y["close"])
    if not (y_close > ma_val):
        return 1, None

    # 条件5b：前 N 日有涨停
    zt_win = (kdf["chg_pct"].iloc[-lb:] if cfg["include_prev_day_in_zt"]
              else kdf["chg_pct"].iloc[-(lb + 1):-1])
    hit_pos = list(zt_win.index[zt_win >= cfg["zt_threshold_pct"]])
    if not hit_pos:
        return 2, None
    last_zt_date = kdf["date"].iloc[hit_pos[-1]]

    # 条件6a：上上个交易日涨跌幅 < 6%
    t2_chg = float(t2["chg_pct"])
    if not (t2_chg < cfg["t2_chg_max"]):
        return 3, None

    # 条件6b：T-2 成交额 / T-1 成交额 > 阈值
    t2_amt, t1_amt = float(t2["amount"]), float(y["amount"])
    if t1_amt <= 0:
        return 4, None
    t2_over_t1 = t2_amt / t1_amt
    if not (t2_over_t1 > cfg["min_t2_over_t1_amount_ratio"]):
        return 4, None

    rec = row.to_dict()
    rec.update({
        "prev_date": y["date"], "prev_close_d": y_close,
        "prev_turnover": y_turn, "ma": round(ma_val, 3),
        "ma_bias_pct": round((y_close / ma_val - 1) * 100, 2),
        "last_zt_date": last_zt_date, "t2_date": t2["date"],
        "t2_chg_pct": t2_chg, "t2_amount": t2_amt, "t1_amount": t1_amt,
        "t2_over_t1": round(t2_over_t1, 3),
    })
    return 5, rec


def stage_history_par(d: pd.DataFrame, cfg: dict, today_str: str,
                      f: Funnel, workers: int = 10,
                      which: str = "tx") -> pd.DataFrame:
    """条件4/5/6 的并行版。

    为什么必须并行：盘后回算候选近 1900 只，单只日K在代理不稳时可达 1.5s，
    串行要 45 分钟以上。并行 10 路后约 1~3 分钟。
    """
    empty_cols = ("昨日换手率区间", "昨收 > MA", "前30日有涨停", "历史量价（条6）")
    if d.empty:
        for n in empty_cols:
            f.add(n, 0)
        return d

    total = len(d)
    load_kline_cache(today_str)
    stages = {i: 0 for i in range(6)}
    kept = []
    done = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_check_history_one, row, cfg, today_str, which): code
                for code, row in d.iterrows()}
        for fu in as_completed(futs):
            done += 1
            try:
                st, rec = fu.result()
            except Exception:
                st, rec = 0, None
            stages[st] = stages.get(st, 0) + 1
            if rec is not None:
                kept.append(rec)
            if done % 50 == 0 or done == total:
                el = time.time() - t0
                print("    日K校验 %d/%d  已用 %.0fs" % (done, total, el),
                      end="\r", flush=True)
    print(" " * 50, end="\r")
    save_kline_cache(today_str)

    # 数据源整体不可用必须立刻报错，不能静默返回「0 只通过」——
    # 那会让客户以为今天没有符合条件的股票。
    # 只统计腾讯主路径：em 复核那批是补充数据，不该参与「接口是否整体不可用」的判断
    _tx_cache = {k: v for k, v in _KLINE_CACHE.items()
                 if str(k).startswith(_CK_TX + ":")}
    n_all = len(_tx_cache)
    n_bad = sum(1 for v in _tx_cache.values() if not v)
    if n_all >= 30 and n_bad >= n_all:
        raise RuntimeError("日K接口全部不可用（腾讯 WAF 拦截或网络异常），"
                           "条件4/5/6 无法校验；请稍后重试")

    n_turn = total - stages[0]
    n_ma = sum(stages[i] for i in range(2, 6))
    n_zt = sum(stages[i] for i in range(3, 6))
    f.add("昨日换手率区间", n_turn)
    f.add("昨收 > MA", n_ma)
    f.add("前30日有涨停", n_zt)
    f.add("历史量价（条6）", len(kept))

    if not kept:
        return d.iloc[0:0].copy()
    return pd.DataFrame(kept).set_index("code", drop=False)


# ---------------------------------------------------------------- 精确复核
def recheck_history_exact(d: pd.DataFrame, cfg: dict, today_str: str,
                          workers: int = 3):
    """对腾讯预筛出的少量候选，用东财日K重算条件4/5/6（精确成交额/换手率）。

    为什么需要：腾讯日K无「成交额/换手率」原始字段，成交额按量×均价估算，
    误差 <1%。条件6 的 110% 阈值若刚好卡在边界，估算值可能翻转结论。
    所以最终入选名单一律用东财原始字段复核；东财此刻拿不到时保留腾讯值并标记。

    返回 (df, n_em, n_fail)
    """
    if d.empty:
        return d, 0, 0

    rows, n_em, n_fail = [], 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_check_history_one, row, cfg, today_str, "em"): code
                for code, row in d.iterrows()}
        for fu in as_completed(futs):
            code = futs[fu]
            try:
                st, rec = fu.result()
            except Exception:
                st, rec = 0, None
            base = d.loc[code].to_dict()
            if st == 5 and rec is not None:
                rec["hist_src"] = "em"
                rows.append(rec)
                n_em += 1
            elif st == 0:
                # 东财不可达 —— 保留腾讯结论，明确标记，不静默当成精确值
                n_fail += 1
                base["hist_src"] = "tx"
                rows.append(base)
            else:
                # 东财精确值未通过（腾讯估算虚高）→ 淘汰
                pass
    if not rows:
        return d.iloc[0:0].copy(), n_em, n_fail
    return pd.DataFrame(rows).set_index("code", drop=False), n_em, n_fail


# ---------------------------------------------------------------- 第三层
def stage_auction_from_detail(d: pd.DataFrame, cfg: dict, f: Funnel,
                              workers: int = 5,
                              stats: dict | None = None) -> pd.DataFrame:
    """逐只拉分笔明细，还原 9:20 / 9:24 / 9:25 并判定条件1、2、3(金额)。

    ★ 2026-10-19 起为**串行**请求，每只之间由 ds.em_throttle 保证 ≥2 秒间隔；
    连续 detail_max_consec_fail 只失败即中止整轮（防把 IP 打进冷却期）。

    stats（可选）回填本次明细抓取的可用性统计，供统一入口判断是否需要降级：
        total / req_fail / no_auction / parsed / aborted
    req_fail == total 说明东财分笔整段不可用，此时竞价三要素无法还原；
    aborted=True 说明中途因连续失败主动放弃，剩余股票未尝试。
    """
    if d.empty:
        for n in ("9:25竞价金额", "9:25/9:20 放大倍数", "9:25价 >= 9:24价"):
            f.add(n, 0)
        return d

    total = len(d)
    results = {}
    done = 0
    n_req_fail = 0
    n_no_auction = 0

    # ★ 2026-10-19 改为串行 + 连续失败即中止（原为 ThreadPoolExecutor 并发）。
    # 两个理由：
    #   1) 东财风控按 QPS 触发，并发打等于自投罗网（社区阈值：单 IP 并发 ≥10
    #      或 1 分钟 ≥200 次即封）；em_throttle 已在底层串行化，这里再保留
    #      线程池只会堆任务、不提升速度。
    #   2) 进入冷却期后，45 只逐只硬顶要耗好几分钟才失败，客户看到的就是
    #      「程序卡住」。连续 N 只拿不到就立刻中止，几十秒内给出明确结论。
    # workers 参数保留只为兼容旧调用方，实际不再控制并发。
    max_consec = max(1, int(cfg.get("detail_max_consec_fail", 6)))
    consec_fail = 0
    aborted = False
    t0 = time.time()
    for code, row in d.iterrows():
        done += 1
        if aborted:
            n_req_fail += 1
            results[code] = None
            continue
        secid = "%d.%s" % (int(row["market"]), code)
        det = fetch_details(_thread_session(), secid, cfg)
        if det is None:
            n_req_fail += 1
            consec_fail += 1
            results[code] = None
            if consec_fail >= max_consec:
                aborted = True
                print()
                print("    [!] 连续 %d 只拿不到竞价明细 —— 判定接口正在风控，"
                      "中止剩余 %d 只，不再继续发请求"
                      % (consec_fail, total - done))
        else:
            consec_fail = 0
            au = parse_auction(det)
            if au is None:
                n_no_auction += 1
            results[code] = au
        if done % 5 == 0 or done == total:
            print("    竞价明细 %d/%d  已用 %.0fs"
                  % (done, total, time.time() - t0), end="\r", flush=True)
    print(" " * 50, end="\r")

    ok = 0
    rows = []
    for code, row in d.iterrows():
        au = results.get(code)
        if not au or not au["q920"] or not au["q925"]:
            continue
        ok += 1
        open_px = float(row["open"])
        amt_925 = au["q925"] * 100.0 * open_px        # 9:25 撮合金额(元)
        amt_920 = au["q920"] * 100.0 * au["p920"]     # 9:20 时点匹配金额(元)
        rec = row.to_dict()
        rec.update({
            "q920": au["q920"], "p920": au["p920"], "p924": au["p924"],
            "q925": au["q925"],
            "auction_amount": amt_925,
            "amount_ratio": (amt_925 / amt_920) if amt_920 > 0 else None,
        })
        rows.append(rec)

    print("    明细：请求失败 %d 只 / 竞价段无匹配记录 %d 只 / 解析成功 %d 只"
          % (n_req_fail, n_no_auction, len(rows)))
    if stats is not None:
        stats.update({"total": total, "req_fail": n_req_fail,
                      "no_auction": n_no_auction, "parsed": len(rows),
                      "aborted": aborted})
    if not rows:
        for n in ("9:25竞价金额", "9:25/9:20 放大倍数", "9:25价 >= 9:24价"):
            f.add(n, 0)
        return d.iloc[0:0].copy()

    a = pd.DataFrame(rows).set_index("code", drop=False)

    # 打标记而不是直接过滤 —— 便于输出「差一点」清单，让用户看到差在哪
    a["ok_amt"] = a["auction_amount"] >= cfg["min_auction_amount"]
    a["ok_ratio"] = a["amount_ratio"].fillna(0) > cfg["min_amount_ratio_925_920"]
    a["ok_price"] = (a["open"] >= a["p924"]) if cfg["require_price_925_ge_924"] \
        else True

    f.add("9:25竞价金额 > %.0f万" % (cfg["min_auction_amount"] / 1e4),
          int(a["ok_amt"].sum()))
    f.add("9:25/9:20 金额 > %g倍" % cfg["min_amount_ratio_925_920"],
          int(a["ok_ratio"].sum()))
    if cfg["require_price_925_ge_924"]:
        f.add("9:25价 >= 9:24价", int(a["ok_price"].sum()))
    return a


# ---------------------------------------------------------------- 输出
def render(res: pd.DataFrame, funnel: Funnel, day: str, meta: dict,
           near: pd.DataFrame | None = None) -> str:
    L = []
    L.append("=" * 78)
    L.append("  集合竞价选股结果   %s" % day)
    L.append("=" * 78)
    L.append("  数据源: %s | 回算方式: 分笔明细还原 9:20/9:24/9:25" % meta.get("src"))
    L.append("  全市场 %d 只 -> 命中 %d 只" % (meta.get("total", 0), len(res)))
    L.append("")
    L.append("  【漏斗】（按执行顺序）")
    for name, cnt in funnel.steps:
        L.append("    %-34s %6d" % (name, cnt))
    L.append("")
    if res is None or res.empty:
        L.append("  今日无股票满足全部条件。")
    else:
        L.append("  %-8s %-10s %8s %8s %10s %8s %8s %8s %8s %8s"
                 % ("代码", "名称", "竞价价", "竞价%", "竞价金额万",
                    "放大倍", "昨换手%", "9:24价", "MA60", "T2/T1"))
        L.append("  " + "-" * 92)
        for _, r in res.sort_values("amount_ratio", ascending=False).iterrows():
            L.append("  %-8s %-10s %8.2f %+8.2f %10.0f %8.2f %8.2f %8.2f %8.2f %8.2f"
                     % (r["code"], str(r["name"])[:9], r["open"], r["auc_chg"],
                        r["auction_amount"] / 1e4, r["amount_ratio"],
                        r["prev_turnover"], r["p924"], r["ma"],
                        r["t2_over_t1"]))

    if near is not None and not near.empty:
        L.append("")
        L.append("  【接近命中】其它 5 组全过，仅「9:25/9:20 放大倍数」不足 %g 倍："
                 % (meta.get("ratio_thr") or 12))
        L.append("  %-8s %-10s %8s %8s %10s %8s %8s"
                 % ("代码", "名称", "竞价价", "竞价%", "竞价金额万", "放大倍", "昨换手%"))
        L.append("  " + "-" * 66)
        for _, r in near.sort_values("amount_ratio", ascending=False).head(15).iterrows():
            L.append("  %-8s %-10s %8.2f %+8.2f %10.0f %8.2f %8.2f"
                     % (r["code"], str(r["name"])[:9], r["open"], r["auc_chg"],
                        r["auction_amount"] / 1e4, r["amount_ratio"],
                        r["prev_turnover"]))
    L.append("=" * 78)
    return "\n".join(L)


def run_backfill(cfg: dict | None = None, *, source: str = "auto",
                 workers: int = 5, kworkers: int = 12, eworkers: int = 3,
                 exact: bool = True, limit: int = 0,
                 stats: dict | None = None) -> int:
    """盘后回算主流程（统一入口 runner 直接调用，也被本文件 main() 复用）。

    与 live 轨的分工：本函数用当日分笔明细还原 9:20/9:24/9:25 的累计匹配量
    与价格，结果与"盘中蹲点抓三次快照"等价，但只能在 09:25:00 之后执行。
    返回 0=正常（含命中 0 只），1=无法完成（快照为空等）。
    """
    if cfg is None:
        cfg = load_cfg()
    # stage_history 默认对候选数封顶（盘中模式防跑飞）。盘后回算候选动辄
    # 上千只，必须解除封顶，否则会静默漏掉大量候选。
    cfg = dict(cfg)
    cfg["max_candidates"] = 10 ** 6
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    print(">>> 盘后回算模式   %s" % now.strftime("%Y-%m-%d %H:%M:%S"))

    src = ds.build_source(cfg, prefer=source)
    print(">>> 数据源: %s" % src.name)

    print(">>> [1/3] 拉取全市场快照 ...")
    try:
        spot = src.spot()
    except Exception as e:
        print("    [警告] 主源快照失败（%s），切换备用源重试" % type(e).__name__)
        src = ds.build_source(cfg, prefer="tencent")
        print("    备用源: %s" % src.name)
        spot = src.spot()
    if spot is None or spot.empty:
        print("[X] 快照为空，退出")
        return 1

    # ★ 完整性闸门（2026-10-19 加）：东财「半死」时最危险 —— 能连通、
    # 但 60 页里 58 页取不到，只回来 200 只。这种残缺数据不会报错，
    # 会让后续每个条件都基于 3% 的样本得出"今天没有股票"的假结论。
    if len(spot) < 1000:
        raise RuntimeError("快照只取到 %d 只（全市场应有 5000+），数据源返回不完整，"
                           "已中止以免得出错误结论" % len(spot))
    need_cols = ["code", "name", "market", "open", "prev_close",
                 "price", "float_mv", "total_mv"]
    miss = [c for c in need_cols if c not in spot.columns]
    if miss:
        raise RuntimeError("快照缺少必需字段 %s，无法回算竞价条件" % miss)
    print("    共 %d 只" % len(spot))
    _cache_shares(spot)

    funnel = Funnel()
    d = stage_pre(spot, cfg, funnel)

    if limit:
        d = d.head(limit)
        print("    [调试] 限制前 %d 只" % len(d))

    # 顺序说明：日K 单只约 0.04s，明细单只 0.3~2s 且易受代理影响。
    # 先跑日K（1889 只 -> 数十只），再对少量候选拉明细，总耗时最短。
    print(">>> [2/3] 校验历史量价条件（条件4/5/6，%d 只）[腾讯日K预筛] ..." % len(d))
    try:
        d = stage_history_par(d, cfg, today_str, funnel, workers=kworkers,
                              which="tx")
    except Exception as e:
        if stats is not None:
            stats["error"] = str(e)
        raise
    print("    通过历史条件 %d 只" % len(d))
    # 口径提示：腾讯 WAF 会把日K 打到新浪「不复权」，MA 偏高 → 漏临界股。
    # 预筛侧也有同一行提示，这里补上是为了兜底回算路径不静默降级。
    _adj = ds.kline_adj_summary()
    if _adj:
        print("    " + _adj)
        if ds.KLINE_ADJ.get("none"):
            log_line("!! 回算日K口径降级 " + _adj)

    if len(d) and exact:
        _n_in = len(d)
        print(">>> [2.5] 东财精确复核（%d 只，排除估算误差翻转）..." % _n_in)
        d, n_em, n_fail = recheck_history_exact(d, cfg, today_str,
                                                workers=eworkers)
        _dropped = _n_in - n_em - n_fail
        # ★ 三种情况必须区分开说（2026-10-19 修）：东财一只都没通，和「全部复核通过」
        #   在旧文案下都容易读成「已精确复核」，但那正是把估算值当精确值用的口子。
        if n_em == 0 and n_fail > 0:
            print("    [!] 东财精确复核未执行：%d 只全部不可达 —— 全部沿用备用源"
                  "估算值，条件6 的 110%% 边界可能不准" % n_fail)
        elif n_fail:
            print("    东财精确通过 %d 只；东财不可达、保留备用源估算 %d 只"
                  % (n_em, n_fail))
        else:
            print("    东财精确通过 %d 只（全部复核）" % n_em)
        if _dropped:
            print("    东财复核淘汰 %d 只（备用源估算虚高，精确值未达标）"
                  % _dropped)

    print(">>> [3/3] 还原竞价明细（条件1/2/3，%d 只）..." % len(d))
    allc = stage_auction_from_detail(d, cfg, funnel, workers=workers,
                                     stats=stats)

    near = None
    if allc is None or allc.empty or "ok_amt" not in allc.columns:
        res = d.iloc[0:0]
    else:
        res = allc[allc["ok_amt"] & allc["ok_ratio"] & allc["ok_price"]]
        # 接近命中：竞价金额、价格都合格，只差「放大倍数」这一项
        near = allc[allc["ok_amt"] & allc["ok_price"] & ~allc["ok_ratio"]]

    # ★ 明细全失败 ≠ 今天没有股票（2026-10-19 修，客户投诉级问题）
    # 前者是数据源挂了，后者是真实结论。若把前者写成「今日无股票满足全部条件」，
    # 客户会以为今天真没机会，而实际是接口不可用 —— 这种静默错误比报错危险得多。
    _total = int((stats or {}).get("total") or 0)
    _fail = int((stats or {}).get("req_fail") or 0)
    _abort = bool((stats or {}).get("aborted"))
    if _total and (_fail >= _total or _abort):
        print()
        print("[X] 竞价明细 %d/%d 只全部失败%s —— 条件1/2/3 无法判定。"
              % (_fail, _total, "（已主动中止）" if _abort else ""))
        print("    ★ 这不等于「今天没有股票」：是数据接口不可用，不能据此判断。")
        if stats is not None:
            stats["detail_dead"] = True     # 让上层走「明细不可用」分支，而非「运行出错」
            stats["error"] = None
        log_line("backfill %s 明细全失败 %d/%d，未写结果文件" % (today_str, _fail, _total))
        return 1

    meta = {"src": src.name, "total": len(spot),
            "ratio_thr": cfg["min_amount_ratio_925_920"]}
    text = render(res, funnel, today_str, meta, near)
    print()
    print(text)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if res is not None and not res.empty:
        cols = ["code", "name", "open", "auc_chg", "auction_amount",
                "q920", "p920", "q925", "p924", "amount_ratio",
                "prev_turnover", "prev_close_d", "ma", "ma_bias_pct",
                "last_zt_date", "t2_chg_pct", "t2_over_t1"]
        fp = OUT_DIR / ("竞价选股结果_%s.csv" % now.strftime("%Y%m%d_%H%M%S"))
        res[[c for c in cols if c in res.columns]].to_csv(
            fp, index=False, encoding="utf-8-sig")
        print("\n已导出: %s" % fp)
        (OUT_DIR / "最新结果.txt").write_text(text, encoding="utf-8-sig")
    else:
        (OUT_DIR / "最新结果.txt").write_text(text, encoding="utf-8-sig")
    log_line("backfill %s 命中 %d 只" % (today_str, 0 if res is None else len(res)))
    return 0


def main():
    """命令行入口（保留原用法：python backfill.py [--limit N] ...）。"""
    ds.setup_std_encoding()
    ap = argparse.ArgumentParser(description="盘后完整选股（用当日真实竞价明细）")
    ap.add_argument("--source", default="auto",
                    choices=["auto", "eastmoney", "tencent"])
    ap.add_argument("--workers", type=int, default=5, help="明细并发数")
    ap.add_argument("--kworkers", type=int, default=12, help="日K并发数")
    ap.add_argument("--eworkers", type=int, default=3, help="东财复核并发数")
    ap.add_argument("--no-exact", action="store_true",
                    help="跳过东财精确复核（全部用腾讯估算值）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前N只（调试）")
    args = ap.parse_args()
    return run_backfill(load_cfg(), source=args.source,
                        workers=args.workers, kworkers=args.kworkers,
                        eworkers=args.eworkers, exact=not args.no_exact,
                        limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())
