"""盘中高频采样 —— 把「三个时点各抓一次」改成「按可补性分配资源的多轮采样」。

【六个条件的数据可补性（2026-10-19 实测，决定了下面的资源分配）】
    条件1  9:20 累计成交额     —— 只能实时抓，**无任何事后替代**（交易所不发布竞价中间态）
    条件2  9:24 竞价价         —— 只能实时抓，但丢失可用邻近轮次顶替（偏宽松）
    条件3  9:25 竞价价+竞价额  —— **腾讯分时 minute/query 第一根盘后可补，逐位一致**
    条件4/5/6 历史指标         —— T-1 15:30 预筛已算完
  结论：真正不可替代的只有 9:20 那一次。因此三个窗口不再等权：
    0920 加密（1.5s/轮，双源并行）→ 唯一硬约束
    0924 常规（2.0s/轮）          → 可邻近顶替
    0925 改成「静止窗」9:25–9:29:50（4s/轮，定型即停）→ 竞价已结束、连续竞价未开始，
         这段时间快照的 (价,量,额) 完全冻结，抓一次即定，不必挤在 45 秒里抢。

【双源并行（2026-10-19 新增）】
腾讯 `qt.gtimg.cn` 与新浪 `hq.sinajs.cn` 是两个独立域名族，历史上从未同时挂过。
每轮先打腾讯（快、批量 600），失败立刻换新浪（1.4s/批，批量 200）。
落盘记录 `source`，事后可复盘当天用的是哪条通道。

【红线】
不要为了"测一下"反复跑采样。窗口轮次是按需求精算的，随意加窗口或反复跑
只会把自己送进限流。9:20 那一轮尤其不能人工试跑 —— 试掉的就是当天唯一一次机会。
"""
from __future__ import annotations

import json
import time
from datetime import datetime, time as dtime
from pathlib import Path

import pandas as pd

import datasource as ds
from datasource import OUT_DIR
from screener import SNAP_DIR, load_cfg, log_line

ROUND_DIR_NAME = "rounds"

# (窗口标签, 开始, 结束, config 里的间隔键)
# 顺序即执行顺序；结束时间到了就跳过（不补采）。
WINDOWS = [
    ("0920", dtime(9, 19, 40), dtime(9, 20, 30), "collect_gap_0920"),
    ("0924", dtime(9, 23, 40), dtime(9, 24, 30), "collect_gap_0924"),
    # 静止窗：9:25 撮合完成后到 9:30 连续竞价开始之间，快照完全不动。
    # 原来只到 9:25:45，白扔了后面 4 分钟 —— 这 4 分钟恰恰是最稳的一段。
    ("0925", dtime(9, 25, 0), dtime(9, 29, 50), "collect_gap_0925"),
]

TARGET_0920 = dtime(9, 20, 0)
TARGET_0924 = dtime(9, 24, 0)
TARGET_0925 = dtime(9, 25, 0)

# 静止窗里检测到定型就提前收工，不用把 5 分钟跑满
SETTLE_MIN_ROUNDS = 2


# ---------------------------------------------------------------------------
# 落盘
# ---------------------------------------------------------------------------
def round_dir(day: str) -> Path:
    return SNAP_DIR / day.replace("-", "") / ROUND_DIR_NAME


def save_round(df: pd.DataFrame, tag: str, ts: datetime, day: str,
               source: str = "tx") -> Path:
    """落盘一轮采样。结构与 screener.save_snapshot 一致，便于复用读取逻辑。

    数据来源写进文件名（`r0920_092001_tx.json` / `r0920_092001_sina.json`），
    这样复盘时不用打开文件就知道当天走的是哪条通道，也不污染 DataFrame 的列。
    """
    d = round_dir(day)
    d.mkdir(parents=True, exist_ok=True)
    fp = d / ("r%s_%s_%s.json" % (tag, ts.strftime("%H%M%S"), source))
    payload = {
        "tag": tag,
        "source": source,
        "captured_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(df),
        "rows": json.loads(df.to_json(orient="records", force_ascii=False)),
    }
    fp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return fp


def load_rounds(day: str) -> dict[str, list[tuple[datetime, pd.DataFrame]]]:
    """读回当天所有轮次，按窗口分组，组内按时间升序。"""
    out: dict[str, list[tuple[datetime, pd.DataFrame]]] = {}
    d = round_dir(day)
    if not d.exists():
        return out
    for fp in sorted(d.glob("r*.json")):
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
            ts = datetime.strptime(obj["captured_at"], "%Y-%m-%d %H:%M:%S")
            df = pd.DataFrame(obj["rows"])
            if df.empty:
                continue
            for c in ["price", "chg_pct", "volume", "amount", "turnover",
                      "open", "prev_close", "total_mv", "float_mv"]:
                df[c] = pd.to_numeric(df.get(c), errors="coerce")
            df = df.set_index("code", drop=False)
            out.setdefault(obj.get("tag") or fp.name[1:5], []).append((ts, df))
        except Exception:
            continue
    for k in out:
        out[k].sort(key=lambda x: x[0])
    return out


def prune_rounds(keep_days: int = 7) -> None:
    """清理旧采样目录，避免长期累积（每个交易日约 40 轮 × 100KB）。"""
    try:
        if not SNAP_DIR.exists():
            return
        dirs = sorted([p for p in SNAP_DIR.iterdir() if p.is_dir()])
        for p in dirs[:-keep_days]:
            rd = p / ROUND_DIR_NAME
            if not rd.exists():
                continue
            for f in rd.glob("r*.json"):
                f.unlink(missing_ok=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 采样
# ---------------------------------------------------------------------------
def _wait_until(t: dtime) -> None:
    target = datetime.now().replace(hour=t.hour, minute=t.minute,
                                    second=t.second, microsecond=0)
    while True:
        left = (target - datetime.now()).total_seconds()
        if left <= 0:
            return
        time.sleep(min(left, 1.0))


def _backfill_static(df: pd.DataFrame, pool: dict | None,
                     full: bool = False) -> pd.DataFrame:
    """用预筛池的静态字段补快照缺的列。

    必须补 float_mv：条件5 的市值门槛在 stage_spot 里过滤，float_mv 为 NaN
    时比较结果为 False，**整批股票会被静默剔除**，看起来像"今天没有股票"。
    流通股本在池子里是 T-1 的精确值，乘以当日价即当日流通市值。

    full=True 时连 prev_close / name / open / chg_pct 一起补 —— 腾讯分时
    只给 (价, 量, 额)，其余全靠池子，缺 prev_close 就算不出竞价涨跌幅。
    """
    if df is None or df.empty or not pool:
        return df
    pmap = {str(it["code"]).zfill(6): it for it in pool.get("items", [])}
    if not pmap:
        return df
    df = df.copy()
    if "float_mv" not in df.columns:
        df["float_mv"] = float("nan")

    def col(key):
        return pd.to_numeric(
            df["code"].map(lambda c: (pmap.get(str(c).zfill(6)) or {}).get(key)),
            errors="coerce")

    shares = col("shares")
    need = df["float_mv"].isna() & shares.notna() & (shares > 0) & df["price"].notna()
    if need.any():
        df.loc[need, "float_mv"] = (shares[need] * df.loc[need, "price"]).values

    if full:
        # prev_close 必须是「T-1 收盘价」—— 条件3 的 auc_chg 就是按它算的。
        # 优先 prev_close_d（日K 归一化出来的 T-1 真值）；ref_close 只是**建池那一刻的
        # 快照现价**：正常流程（15:30 建池、服务次日）两者恰好相等，所以从没暴露过，
        # 但只要建池时点不是「收盘后为次日建」，两者就不等，进而**静默算错竞价涨幅**。
        # 2026-10-19 实测：600354 ref_close=7.94 / prev_close_d=8.38，
        # 涨幅被算成 +4.66%，真实值 -0.84% —— 条件3 会因此误纳/误弃。
        prev = col("prev_close_d")
        _alt = col("ref_close")
        prev = prev.where(prev.notna(), _alt)   # 逐只兜底，不是整列兜底
        if "prev_close" not in df.columns:
            df["prev_close"] = float("nan")
        miss = df["prev_close"].isna() & prev.notna()
        if miss.any():
            df.loc[miss, "prev_close"] = prev[miss].values
        if "name" not in df.columns:
            df["name"] = None
        nmiss = df["name"].isna()
        if nmiss.any():
            df.loc[nmiss, "name"] = df.loc[nmiss, "code"].map(
                lambda c: (pmap.get(str(c).zfill(6)) or {}).get("name"))
        if "open" not in df.columns:
            df["open"] = float("nan")
        df["open"] = df["open"].fillna(df["price"])
        df["chg_pct"] = (df["price"] / df["prev_close"] - 1.0) * 100.0
        if "turnover" not in df.columns:
            df["turnover"] = float("nan")
        if "total_mv" not in df.columns:
            df["total_mv"] = float("nan")
    return df


def _same_snapshot(a: pd.DataFrame | None, b: pd.DataFrame | None,
                   tol: float = 0.98) -> bool:
    """两轮快照的 (成交量, 成交额) 是否几乎逐只相同 —— 竞价已撮合定型。"""
    if a is None or b is None or a.empty or b.empty:
        return False
    common = a.index.intersection(b.index)
    if len(common) < 10:
        return False
    same = ((a.loc[common, "volume"] == b.loc[common, "volume"]) &
            (a.loc[common, "amount"] == b.loc[common, "amount"])).mean()
    return bool(same > tol)


def _fetch_round(src, items, use_pool: bool,
                 pool: dict | None, tag: str | None = None
                 ) -> list[tuple[pd.DataFrame, str]]:
    """一轮抓取，返回 [(df, 来源标记), ...]。

    【2026-10-20 实盘事故后的改动：9:20 / 9:24 必须双源都打】

    条件1 的分母是「9:20 累计竞价金额」，而各源在集合竞价期间的能力不同：
    实测腾讯 qt.gtimg.cn 在 9:25 撮合前只给匹配价，[6]成交量/[37]成交额恒为 0。
    旧的策略是「腾讯优先，失败才换新浪」—— 可腾讯这次是「成功但金额全为 0」，
    不算失败，于是根本不会切源。结果在条件1 唯一的机会窗口里，
    全程只用了那个不给额度的源，17 只候选被静默剔光。

    改为：这两个窗口两源都抓、都落盘，事后由 pick_samples 挑金额有效的那份。
    代价是每轮多约 1.4 秒（窗口内轮数减半，仍远多于所需），换来的是
    「至少还有一份带金额的数据」，值得。
    9:25 窗口只需要价、不需要金额交叉，保持单源以省时间。
    """
    def _try(fn, label, need_pool_fixup):
        try:
            df = fn()
        except Exception as e:
            log_line("采样[%s] 异常 %s: %s" % (label, type(e).__name__, e))
            return None
        if df is None or not len(df):
            return None
        return _backfill_static(df, pool) if need_pool_fixup else df

    if not use_pool:
        # 全市场模式：新浪 list 拼接会超 URL 长度，不走第二通道
        df = _try(src.spot, "腾讯", False)
        return [(df, "tx")] if df is not None else []

    out: list[tuple[pd.DataFrame, str]] = []
    df = _try(lambda: src.spot_codes(items), "腾讯", False)
    if df is not None:
        out.append((df, "tx"))

    sina_fn = getattr(src, "spot_codes_sina", None)
    dual = tag in ("0920", "0924")
    if sina_fn is not None and (dual or not out):
        df2 = _try(lambda: sina_fn(items), "新浪", True)
        if df2 is not None:
            out.append((df2, "sina"))
    return out


def collect(cfg: dict | None = None, day: str | None = None,
            src=None, pool_items=None, quiet: bool = False,
            windows=None, pool: dict | None = None) -> dict:
    """在三个窗口内高频采样候选池，返回统计信息。

    单轮失败不中断 —— 这一轮只是几十个样本中的一个，下一轮再来即可。

    pool: 预筛池对象。显式传入优先；否则按 day 自动加载。
          它有两个用途：① 给出采样代码列表 ② 新浪通道缺市值时补 shares。
    """
    cfg = cfg or load_cfg()
    day = day or datetime.now().strftime("%Y-%m-%d")
    stats = {"day": day, "rounds": {}, "fail": 0, "total": 0, "source": None,
             "by_source": {}}

    if pool is None:
        from prep import load_pool
        pool = load_pool(day)
    items = pool_items
    if items is None and pool and pool.get("items"):
        items = [(it["code"], it["market"]) for it in pool["items"]]
    use_pool = bool(items)

    if src is None:
        src = ds.build_source(cfg, prefer="tencent")
    stats["source"] = src.name

    if not quiet:
        print(">>> 高频采样  %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if use_pool:
            print("    候选池 %d 只（每轮 1 个请求，腾讯失败自动切新浪）" % len(items))
        else:
            print("    [!] 没有候选池，退化为全市场采样（每轮约 10 个请求，"
                  "更慢也更费流量）")

    for tag, t_start, t_end, gap_key in (windows or WINDOWS):
        gap = float(cfg.get(gap_key, 2.0))
        now_t = datetime.now().time()
        if now_t >= t_end:
            if not quiet:
                print("    [跳过] %s 窗口已过（%s 之后无法补采）"
                      % (tag, t_end.strftime("%H:%M:%S")))
            continue
        if now_t < t_start:
            if not quiet:
                left = (datetime.now().replace(hour=t_start.hour, minute=t_start.minute,
                                               second=t_start.second, microsecond=0)
                        - datetime.now()).total_seconds()
                print("    等待 %s 窗口（%.0f 秒后开始）..." % (tag, max(0, left)))
            _wait_until(t_start)

        n = ok = 0
        used_count: dict[str, int] = {}
        prev_df = None
        settled = False
        while datetime.now().time() < t_end:
            t0 = time.time()
            n += 1
            got = _fetch_round(src, items, use_pool, pool, tag)
            if got:
                ok += 1
                for _df, _src in got:
                    save_round(_df, tag, datetime.now(), day, _src or "tx")
                    used_count[_src or "?"] = used_count.get(_src or "?", 0) + 1
                df = got[0][0]
                # 静止窗：相邻两轮完全一致 = 撮合定型，不用把 5 分钟跑满
                if tag == "0925" and ok >= SETTLE_MIN_ROUNDS and \
                        _same_snapshot(prev_df, df):
                    settled = True
                prev_df = df
            else:
                stats["fail"] += 1
            if not quiet:
                print("    %s 第 %d 轮 %s  已成功 %d%s" %
                      (tag, n, "OK(%s)" % "+".join(s for _d, s in got) if got else "失败",
                       ok, "  已定型" if settled else ""),
                      end="\r", flush=True)
            if settled:
                break
            sleep_left = gap - (time.time() - t0)
            if sleep_left > 0:
                time.sleep(sleep_left)

        stats["rounds"][tag] = {"tries": n, "ok": ok, "by_source": used_count,
                                "settled": settled}
        stats["total"] += ok
        for k, v in used_count.items():
            stats["by_source"][k] = stats["by_source"].get(k, 0) + v
        if not quiet:
            print(" " * 60, end="\r")
            extra = "，来源 " + "/".join("%s×%d" % (k, v) for k, v in used_count.items()) \
                if used_count else ""
            print("    %s 窗口结束：尝试 %d 轮，成功 %d 轮%s%s"
                  % (tag, n, ok, extra, "（已确认定型，提前收工）" if settled else ""))

    prune_rounds()
    return stats


# ---------------------------------------------------------------------------
# 挑代表样本
# ---------------------------------------------------------------------------
def _nearest(seq, target: dtime, need_amount: bool = False
             ) -> tuple[pd.DataFrame | None, float, int]:
    """取离 target 最近的一轮。返回 (df, 偏差秒数, 该窗口总轮数)。

    同等距离时优先取 target 之后的那轮。

    need_amount=True 时先剔除「金额整列为 0」的轮次再挑 —— 2026-10-20 起
    9:20/9:24 双源落盘，同一时刻会同时存在 tx（只有价）和 sina（可能有额）两份，
    不筛的话可能正好挑中那份没金额的，等于白采。
    只有在全部轮次都没金额时才退回原逻辑（此时由调用方的健康闸门报故障）。
    """
    if not seq:
        return None, float("nan"), 0

    def _delta(ts) -> float:
        return (ts.hour * 3600 + ts.minute * 60 + ts.second) - (
            target.hour * 3600 + target.minute * 60 + target.second)

    pool_seq = seq
    if need_amount and "amount" in seq[0][1].columns:
        rich = [(ts, df) for ts, df in seq
                if (pd.to_numeric(df["amount"], errors="coerce").fillna(0) > 0).any()]
        if rich:
            pool_seq = rich

    best, best_key = None, None
    for ts, df in pool_seq:
        delta = _delta(ts)
        key = (abs(delta), 0 if delta >= 0 else 1)
        if best_key is None or key < best_key:
            best, best_key = (df, float(delta)), key
    return best[0], best[1], len(seq)


def try_minute_0925(pool: dict | None) -> tuple:
    """采样没拿到 9:25 时，用腾讯分时第一根还原集合竞价（补条件3）。

    【为什么值得做】2026-10-19 实测：分时第一根 `0930 8.31 4905 4076055.00`
    与当天真实结果的 open / 竞价量 / 竞价额逐位一致。也就是说条件3 根本不需要
    挤在 9:25 那 45 秒里抢，盘中任何时刻（09:30 之后）都能补回来。

    【做不到什么】补不了 9:20 的累计额和 9:24 的价 —— 分时从 09:30 才开始。
    条件1/2 仍然只能在当时抓。

    返回 (df 或 None, 可读说明)。
    """
    if not pool or not pool.get("items"):
        return None, "没有候选池，无法定位要补哪些股票"
    codes = [(it["code"], it["market"]) for it in pool["items"]]
    print("\n>>> 9:25 采样缺失 —— 改用腾讯分时第一根还原竞价数据（%d 只）..."
          % len(codes))
    print("    逐只请求，约 0.25 秒/只，预计 %.0f 秒" % (len(codes) * 0.35))

    def _p(i, total, ok, fail):
        if i % 20 == 0 or i == total:
            print("    分时还原 %d/%d  成功 %d" % (i, total, ok), end="\r", flush=True)

    try:
        df = ds.minute_auction_batch(codes, gap=0.25, on_progress=_p)
    except Exception as e:
        print(" " * 60, end="\r")
        return None, "腾讯分时请求异常：%s" % type(e).__name__
    print(" " * 60, end="\r")
    if df is None or df.empty:
        return None, "腾讯分时也没取到（%d 只全部失败）" % len(codes)
    # 分时只给价/量/额，其余字段靠池子的静态值补
    df = _backfill_static(df, pool, full=True)
    got = len(df)
    txt = "腾讯分时还原成功 %d/%d 只（口径与实时快照一致）" % (got, len(codes))
    if got < len(codes) * 0.9:
        txt += "，缺口 %d 只（停牌或当天无分时）" % (len(codes) - got)
    return df, txt


def _settled(seq) -> tuple[pd.DataFrame | None, bool, float]:
    """找 9:25 撮合定型的那一轮。

    判据：相邻两轮里，绝大多数股票 (成交量, 成交额) 完全一致 —— 竞价已撮合完、
    开始进入连续竞价前的静止状态。取这一对里的后者。

    找不到就退化取最后一轮，并返回 settled=False，交由上层明确标注。
    """
    if not seq:
        return None, False, float("nan")
    target_s = TARGET_0925.hour * 3600 + TARGET_0925.minute * 60 + TARGET_0925.second
    for i in range(1, len(seq)):
        if _same_snapshot(seq[i - 1][1], seq[i][1]):
            ts = seq[i][0]
            return seq[i][1], True, float(ts.hour * 3600 + ts.minute * 60 + ts.second - target_s)
    ts, df = seq[-1]
    return df, False, float(ts.hour * 3600 + ts.minute * 60 + ts.second - target_s)


def pick_samples(day: str) -> tuple[dict, dict, list[str]]:
    """从落盘轮次里挑出 9:20 / 9:24 / 9:25 三个代表样本。

    返回 (snaps, meta, problems)：
        snaps    {'0920': df, '0924': df, '0925': df}，缺的键不出现
        meta     每个时点的人类可读说明
        problems 缺失/未定型的明确说明（空列表表示全部正常）
    """
    rounds = load_rounds(day)
    snaps, meta, problems = {}, {}, []

    # 9:20 必须挑「金额有值」的那份：条件1 的分母是竞价金额，
    # 双源落盘后同一时刻会有 tx/sina 两份，tx 的 amount 在撮合前恒为 0。
    s920, d920, n920 = _nearest(rounds.get("0920", []), TARGET_0920, need_amount=True)
    if s920 is None:
        problems.append("9:20 快照完全没有采到（窗口 %s）" % window_desc("0920"))
    else:
        snaps["0920"] = s920
        meta["0920"] = "9:20 用 %+.1fs 那轮（窗口 %d 轮）" % (d920, n920)
        if abs(d920) > 5:
            problems.append("9:20 最近的样本偏离 %.1f 秒，条件1 的分母会有偏差" % abs(d920))

    s924, d924, n924 = _nearest(rounds.get("0924", []), TARGET_0924)
    if s924 is None:
        problems.append("9:24 快照完全没有采到（窗口 %s）" % window_desc("0924"))
    else:
        snaps["0924"] = s924
        meta["0924"] = "9:24 用 %+.1fs 那轮（窗口 %d 轮）" % (d924, n924)
        if abs(d924) > 5:
            problems.append("9:24 最近的样本偏离 %.1f 秒，条件2 的基准价会有偏差" % abs(d924))

    s925, settled, d925 = _settled(rounds.get("0925", []))
    if s925 is None:
        problems.append("9:25 快照完全没有采到（窗口 %s）" % window_desc("0925"))
    else:
        snaps["0925"] = s925
        if settled:
            meta["0925"] = "9:25 已确认撮合定型（%+.1fs 那轮）" % d925
        else:
            meta["0925"] = "9:25 未确认定型，用最后一轮（%+.1fs）" % d925
            problems.append("9:25 没能确认撮合定型，用的是窗口最后一轮，"
                            "结果可能与真实竞价有偏差")

    return snaps, meta, problems


def amount_health(snaps: dict) -> dict:
    """检查 9:20 / 9:24 快照里的「竞价金额」到底有没有值。

    【为什么必须有这道闸门 —— 2026-10-20 实盘事故】
    腾讯 qt.gtimg.cn 在 9:25:00 撮合之前，[6]成交量 / [37]成交额 **一律返回 0**，
    只有 price（匹配价）是有的。而条件1 的分母正是 9:20 的累计竞价金额。
    结果那天：9:20 窗口 32 轮、覆盖率 100/100、最贴近 9:20:00 的一轮偏离 0.0 秒，
    体检全绿；可 17 只候选的 amount_920 全是 0 → 被 screener.stage_auction
    的 `zero` 分支静默剔光 → 输出「今日无股票满足全部条件」。
    **这是假阴性**：不是没有股票，是我们的数据源拿不到条件1 的分母。

    教训：窗口「采到了」不等于数据「可用」。挑选样本之后、进筛选之前，
    必须单独验一次数值有效性，否则失败会伪装成正常的「空结果」。
    """
    out = {"0920_ok": False, "0924_ok": False,
           "0920_rows": 0, "0924_rows": 0,
           "0920_nonzero": 0, "0924_nonzero": 0,
           "0924_price_ok": False}
    for key, tag in (("0920", "0920_ok"), ("0924", "0924_ok")):
        df = snaps.get(key)
        if df is None or len(df) == 0 or "amount" not in df.columns:
            continue
        nz = int((pd.to_numeric(df["amount"], errors="coerce").fillna(0) > 0).sum())
        out[key + "_rows"] = int(len(df))
        out[key + "_nonzero"] = nz
        # 过半有值才算可用：个别停牌股为 0 是正常的，整列全 0 才是数据源问题
        out[tag] = nz >= max(1, int(len(df) * 0.5))

    # 条件2 只用「价」，竞价期间腾讯恰好只给价不给额 —— 金额废了不代表条件2 也废。
    # 这两个判断必须分开，否则会把本来能验证的条件2 一起丢掉（2026-10-20 首版就错了）。
    df924 = snaps.get("0924")
    if df924 is not None and len(df924) and "price" in df924.columns:
        nzp = int((pd.to_numeric(df924["price"], errors="coerce").fillna(0) > 0).sum())
        out["0924_price_ok"] = nzp >= max(1, int(len(df924) * 0.5))
    return out


def _round_sources(day: str) -> dict[str, dict[str, int]]:
    """{tag: {来源: 轮数}} —— 从落盘文件名解析，不读文件内容。"""
    out: dict[str, dict[str, int]] = {}
    d = round_dir(day)
    if not d.exists():
        return out
    for fp in d.glob("r*.json"):
        parts = fp.stem.split("_")          # r0920_092001_tx / r0920_092001(旧)
        if len(parts) < 2:
            continue
        tag = parts[0][1:]
        src = parts[2] if len(parts) >= 3 else "tx"
        out.setdefault(tag, {})
        out[tag][src] = out[tag].get(src, 0) + 1
    return out


def describe_rounds(day: str) -> str:
    """采样情况摘要，用于结果文件与日志。"""
    rounds = load_rounds(day)
    if not rounds:
        return "  （当天没有任何采样记录）"
    srcs = _round_sources(day)
    L = []
    for tag in ("0920", "0924", "0925"):
        seq = rounds.get(tag, [])
        if not seq:
            L.append("    %s : 0 轮" % tag)
            continue
        span = "%s ~ %s" % (seq[0][0].strftime("%H:%M:%S"), seq[-1][0].strftime("%H:%M:%S"))
        by = srcs.get(tag) or {}
        src_txt = ("  来源 " + "/".join("%s×%d" % (k, v) for k, v in by.items())) if by else ""
        L.append("    %s : %d 轮  %s  每次约 %d 只%s"
                 % (tag, len(seq), span, int(seq[-1][1].shape[0]), src_txt))
    return "\n".join(L)


def window_desc(tag: str) -> str:
    """把窗口的起止时间格式化成 `09:19:40–09:20:30`，用于告警文案。"""
    for t, a, b, _ in WINDOWS:
        if t == tag:
            return "%s–%s" % (a.strftime("%H:%M:%S"), b.strftime("%H:%M:%S"))
    return tag


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def run_collect(cfg: dict | None = None, day: str | None = None,
                src=None, quiet: bool = False) -> int:
    """采样轨主流程：高频采样 → 挑代表样本 → 选股 → 输出。"""
    import screener as sc
    from prep import load_pool

    cfg = cfg or load_cfg()
    day = day or datetime.now().strftime("%Y-%m-%d")
    pool = load_pool(day)

    print(">>> 采样轨（多轮冗余）  %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if pool and pool.get("items"):
        print("    候选池 %d 只（%s 生成，依据 T-1 = %s）"
              % (len(pool["items"]), pool.get("built_at", "?"),
                 pool.get("prev_date")))
    else:
        print("    [!] 没有 %s 的候选池 —— 将退化为全市场采样，"
              "且出结果时需现场拉日K（慢）" % day)

    collect(cfg, day=day, src=src, quiet=quiet)
    if src is None:
        src = ds.build_source(cfg, prefer="tencent")

    print("\n采样情况：")
    print(describe_rounds(day))

    snaps, meta, problems = pick_samples(day)
    if "0925" not in snaps:
        # 9:25 缺失不必立刻放弃 —— 腾讯分时第一根盘后能还原，先试一次
        df925, note = try_minute_0925(pool)
        if df925 is not None:
            snaps["0925"] = df925
            meta["0925"] = "9:25 %s" % note
            problems = [p for p in problems if "9:25" not in p]
            print("    [OK] %s" % note)
        else:
            msg = "；".join(problems) or "9:25 窗口一轮都没成功"
            sc.write_fail(day, "9:25 竞价快照没拿到，腾讯分时也补不上：%s；%s"
                          % (msg, note))
            print("\n[X] %s" % msg)
            print("    [!] %s" % note)
            log_line("!! 采样轨失败 %s | %s" % (msg, note))
            return 3

    # 缺 9:20 或 9:24 时，条件1/2 无法判定 —— 不出正式结果，但给参考清单
    missing = [k for k in ("0920", "0924") if k not in snaps]
    if missing:
        cond = "、".join("条件%d" % (1 if k == "0920" else 2) for k in missing)
        print("\n[!] 缺少 %s 窗口数据，%s 无法判定" % ("/".join(missing), cond))
        return _partial_output(sc, cfg, src, day, snaps, pool, problems,
                               cond, missing)

    print("\n代表样本：")
    for k in ("0920", "0924", "0925"):
        print("    %s" % meta.get(k, "-"))

    # ★ 窗口齐全 ≠ 数据可用，采样轨也必须过闸门。
    # 2026-10-21 事故：闸门只加在 runner._do_resolve（09:31 定型轨）上，
    # 采样轨在 09:25:18 抢先跑了 screener.screen，amount_920 全 0 →
    # stage_auction 的 zero 分支把 102 只全剔除 → 写出「无股票满足全部条件」
    # 并覆盖最新结果.txt。客户 09:25~09:31 看到的是假阴性，6 分钟后
    # 定型轨又给出 78 只，两份结论自相矛盾。两条轨必须用同一套判断。
    health = amount_health(snaps)
    if not health["0920_ok"]:
        print("\n[!] 9:20 快照采到了（%d 只）但竞价金额全是 0 —— 数据源不给竞价期金额"
              % health["0920_rows"])
        print("    → 条件1（9:25额/9:20额）无法判定，条件2 的 9:24 价格仍有效。")
        reason = ("9:20 快照采到了 %d 只（覆盖率正常），但当时的累计竞价金额全为 0 —— "
                  "数据源在集合竞价撮合前不返回成交额，不是采样失败，也不是今天没有股票。"
                  % health["0920_rows"])
        log_line("!! 采样轨 %s 9:20 金额全 0，改出低置信度" % day)
        return _partial_output(sc, cfg, src, day, snaps, pool, problems,
                               "条件1（9:20 竞价金额）", ["0920"],
                               reason=reason,
                               cond2_ok=bool(health["0924_price_ok"]))

    print("\n>>> 开始选股 ...")
    res, funnel = sc.screen(snaps, src, cfg, day, pool=pool)

    warn = None
    if problems:
        warn = "；".join(problems)
    text = sc.render_result(res, funnel, meta, warn)
    print(text)
    log_line("采样轨 %s 命中 %d 只\n%s" % (day, 0 if res is None else len(res), text))

    fp = sc.export_csv(res, datetime.now())
    if fp:
        print("\n结果已导出：%s" % fp)
    print("最新结果：%s" % sc.save_latest(res, fp, day))
    return 0


def _partial_output(sc, cfg, src, day, snaps, pool, problems, cond, missing,
                    reason: str | None = None, cond2_ok: bool = False) -> int:
    """缺 9:20 / 9:24 时的低置信度输出。

    【产品决定（2026-10-19 甲方确认）】
    条件1/2 无法判定时，不再只写「今日无股票」——那会让客户空等一早上，
    而且技术上并不诚实：我们确实不知道，不等于没有。
    改为输出「满足其余条件的候选名单」，并**逐条标注哪几个条件没验证**。

    三条底线：
      1. 标题必须带「低置信度」，不能让它看起来像完整结果
      2. 未验证的条件必须写在最显眼处，不能塞到文末
      3. 文件名也带标记，避免和正式结果混在同一个目录里

    2026-10-20 扩展：
      · reason   —— 自定义缺失原因。窗口采到了但金额全 0 时，
                    不能再说「快照没有采到」（那是假话），要说数据源拿不到。
      · cond2_ok —— 9:24 的价格有值（腾讯竞价期只给价不给额），
                    此时条件2 其实可以判，不该跟着条件1 一起丢掉。
    """
    from screener import Funnel, stage_spot, _filter_by_pool
    funnel = Funnel()
    # 标签要反映「这份快照到底覆盖多少只」，不能只看有没有池子：
    # 池子缺失那天采的是全市场 5574 只，标成「候选池快照」同样误导。
    _n = 0 if snaps.get("0925") is None else len(snaps["0925"])
    _label = "候选池快照" if (pool and _n < 1000) else None
    d = stage_spot(snaps["0925"], cfg, funnel, source_label=_label)
    if pool:
        d = _filter_by_pool(d, pool, funnel)

    # 条件2 能判就判掉，别把可用的信息一起扔掉
    if cond2_ok and d is not None and len(d) and "0924" in snaps:
        p924 = snaps["0924"]["price"].reindex(d.index)
        keep = p924.notna() & (d["price"] >= p924)
        d = d[keep]
        funnel.add("9:25价 >= 9:24价", len(d))

    # ★ 不能无条件写「已验证条件4/5/6」：pool 为空时这里根本没做任何历史条件
    # 校验（_filter_by_pool 是跳过状态），2026-10-21 的 78 只全部只过了
    # 条件3 与条件5 静态项，报告却宣称「已验证条件4、5、6」——对客户是假话。
    if pool:
        ok_conds = "条件3（竞价涨跌幅 + 竞价金额）、条件4、条件5、条件6（与预筛池取交集）"
    else:
        ok_conds = "条件3（竞价涨跌幅 + 竞价金额）、条件5 静态项（股价 / 市值）"
    if cond2_ok:
        ok_conds += "、条件2（9:25价 >= 9:24价）"
    unverified = cond if pool else ("%s；条件4 / 条件5 历史项 / 条件6 今天未执行（没有候选池）"
                                    % cond)
    n = 0 if d is None else len(d)
    stamp = datetime.now()
    miss_txt = "、".join("9:20" if k == "0920" else "9:24" for k in missing)

    L = ["=" * 96,
         "  集合竞价选股结果（低置信度）   |   生成于 %s" % stamp.strftime("%Y-%m-%d %H:%M:%S"),
         "=" * 96,
         "  ★ 这份结果没有通过全部 6 个条件，请按「参考」使用，不要当成买入信号。",
         "",
         "  已验证：%s" % ok_conds,
         "  未验证：%s" % unverified,
         "          原因：%s" % (reason or ("%s 的快照没有采到。" % miss_txt)),
         "",
         "  也就是说：下面这些股票「可能」满足条件1/2，也可能不满足 —— 我们没数据判定。" if pool
         else "  也就是说：下面这些股票只过了条件3 与条件5 静态项，完全没做历史条件筛选 —— 仅供参考。",
         "=" * 96]
    L.append("筛选漏斗（只到已验证的部分）：")
    L.append(funnel.render())
    L.append("=" * 96)

    if n == 0:
        L.append("  连条件3/4/5/6 都没有股票满足 —— 今天可以放弃，不必再等。")
        L.append("=" * 96)
    else:
        L.append("满足已验证条件的股票（%d 只）：" % n)
        L.append("")
        L.append("%-8s%-10s%8s%10s%12s%9s%8s%8s%8s%12s%9s%9s"
                 % ("代码", "名称", "竞价价", "竞价涨幅", "竞价金额万",
                    "昨换手", "昨收", "MA60", "偏离%", "近30涨停日",
                    "T-2涨跌", "T2/T1量"))
        L.append("-" * 96)
        for _, r in d.iterrows():
            L.append("%-8s%-10s%8.2f%10.2f%12.0f%9s%8.2f%8.2f%8.2f%12s%9.2f%9.2f"
                     % (r["code"], str(r["name"])[:9], r["price"], r["auc_chg"],
                        r["amount"] / 1e4,
                        ("%.2f" % r["prev_turnover"]) if pd.notna(r.get("prev_turnover")) else "-",
                        r.get("prev_close_d") if pd.notna(r.get("prev_close_d")) else 0,
                        r.get("ma") if pd.notna(r.get("ma")) else 0,
                        r.get("ma_bias_pct") if pd.notna(r.get("ma_bias_pct")) else 0,
                        str(r.get("last_zt_date") or "-"),
                        r.get("t2_chg_pct") if pd.notna(r.get("t2_chg_pct")) else 0,
                        r.get("t2_over_t1") if pd.notna(r.get("t2_over_t1")) else 0))
        L.append("=" * 96)

    L.append("采样情况：")
    L.append(describe_rounds(day))
    if problems:
        L.append("")
        L.append("数据提示：")
        for p in problems:
            L.append("  · " + p)
    L.append("=" * 96)
    L.append("  怎么才能避免低置信度：9:19 前保持开机。9:20 那一次快照是条件1 唯一的机会，")
    L.append("  错过就没有任何接口能补回来（9:25 的竞价数据反而可以盘后补）。")

    text = "\n".join(L)
    print("\n" + text)

    # ---- 导出：文件名与内容都带「低置信度」，避免和正式结果混淆 ----
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fp_txt = OUT_DIR / "最新结果.txt"
    fp_txt.write_text(text, encoding="utf-8-sig")

    fp_csv = None
    if n:
        fp_csv = OUT_DIR / ("选股结果_低置信度_%s.csv" % stamp.strftime("%Y%m%d_%H%M%S"))
        cols = ["code", "name", "price", "auc_chg", "amount", "prev_turnover",
                "prev_close_d", "ma", "ma_bias_pct", "last_zt_date",
                "t2_chg_pct", "t2_amount", "t1_amount", "t2_over_t1",
                "prev_date", "t2_date", "float_mv"]
        out = d[[c for c in cols if c in d.columns]].copy()
        out.insert(0, "置信度", "低")
        out.insert(1, "未验证条件", cond)
        out.to_csv(fp_csv, index=False, encoding="utf-8-sig")
        print("\n结果已导出：%s" % fp_csv)
    print("最新结果：%s" % fp_txt)

    log_line("采样轨 %s 低置信度结果：%s 缺失，候选 %d 只" % (day, cond, n))
    return 4



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None):
    import argparse
    from datetime import datetime as _dt
    ds.setup_std_encoding()
    ap = argparse.ArgumentParser(description="盘中高频采样（只采样，不算结果）")
    ap.add_argument("--day", default=None, help="交易日 YYYY-MM-DD，默认今天")
    ap.add_argument("--dry", action="store_true", help="只在当前时刻抓 1 轮，验证链路")
    args = ap.parse_args(argv)

    cfg = load_cfg()
    if args.dry:
        from prep import load_pool
        day = args.day or _dt.now().strftime("%Y-%m-%d")
        pool = load_pool(day)
        items = [(it["code"], it["market"]) for it in pool["items"]] if pool else None
        src = ds.build_source(cfg, prefer="tencent")
        if not items:
            print("[!] 没有候选池，用全市场试一轮")
            df = src.spot()
        else:
            df = src.spot_codes(items)
        print("一轮返回 %d 只，字段样例：" % len(df))
        print(df.head(5)[["code", "name", "price", "open", "amount", "volume"]]
              .to_string(index=False))
        return 0

    stats = collect(cfg, day=args.day)
    print()
    print("采样统计：", stats)
    print()
    print("落盘情况：")
    print(describe_rounds(stats["day"]))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
