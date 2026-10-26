# -*- coding: utf-8 -*-
"""
统一入口 —— 一个程序，两条轨，按当前时间自动选路。

=======================================================================
【客户视角】只需要双击，不需要理解"实时"和"回算"的区别
=======================================================================
    双击 1-开始选股.bat（或等每日定时任务）
        ↓
    程序看现在几点，自己决定走哪条轨
        ↓
    出结果：output\\最新结果.txt

=======================================================================
【两条轨为什么必须都有】—— 不是冗余，是互补
=======================================================================
条件1 需要「9:25 竞价金额」、条件2 需要「9:25 竞价价格」。
9:25 之前这两个数**根本不存在**，所以：

  实时轨（live）
      09:15~09:25 之间跑。9:20 / 9:24 / 9:25 各抓一次全市场快照。
      · 优点：能在 9:25 刚结束就出结果，赶得上盘中决策
      · 缺点：抓的是"瞬时快照"，9:24 那次和 9:25 那次之间还有几十秒的
        变动没被捕捉；且一旦某次抓取失败，整场作废

  回算轨（backfill）
      09:25:40 之后任意时刻跑。用东财分笔明细还原 9:20/9:24/9:25。
      · 优点：数据是真实的累计匹配量，不丢中间过程；不挑时间，
        9:30 跑和 15:00 跑结果完全一致
      · 缺点：要多花 2~3 分钟；依赖东财分笔接口（该接口会整段抽风）

  降级链（本文件负责）
      回算轨发现东财分笔整段不可用
          → 用当天实时轨已经存下的快照复盘（结果标注"近似"）
          → 连快照都没有 → 明确报错，不留空白、不显示旧结果

=======================================================================
【硬边界】（写在这里防止以后又被问一遍）
=======================================================================
  · 09:25:00 之前，回算轨不可能工作 —— 9:25 的终值还没产生
  · 次日 09:15 之后，前一天的竞价明细已被覆盖，无法补算
  · 单实例锁：定时任务与手动双击撞车时，后来者直接退出。
    2026-10-19 实测过东财被高并发请求后整段掐断（clist/kline/details 全挂），
    两个进程同时打等于自己给自己限流。
=======================================================================
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path

import backfill as bf
import datasource as ds
import screener as sc
from datasource import OUT_DIR

AUCTION_OPEN = dtime(9, 15, 0)      # 集合竞价开始
LIVE_CUTOFF = dtime(9, 25, 0)       # 此后 9:25 终值已产生
COLLECT_END = dtime(9, 25, 45)      # 最晚可「开始采集」的时刻：9:20/9:24 都过
                                    # 了之后再开采集没有意义（9:25 那批可由腾讯
                                    # 分时盘后补，不必现场抢）
BACKFILL_SETTLE = 40                # 09:25:00 后再等 40s，等撮合明细落地
LOCK_FILE = OUT_DIR / ".runner.lock"

_MODE_LABEL = {"prep": "收盘预筛", "collect": "采样轨", "resolve": "定型轨",
               "live": "旧实时轨", "backfill": "东财回算（兜底）",
               "replay": "快照复盘（降级）"}


# ---------------------------------------------------------------- 单实例锁
def _acquire_lock():
    """非阻塞独占锁。返回句柄表示拿到，None 表示已有实例在跑。

    用 OS 级文件锁而非"标记文件"：进程异常退出时系统自动释放，不会残留
    死锁文件（客户下次就再也跑不起来了）。
    """
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_RDWR)
    except Exception:
        return -1                       # 锁都建不了就别拦着干活
    try:
        try:
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except ImportError:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError:
        try:
            os.close(fd)
        except Exception:
            pass
        return None


def _release_lock(fd):
    if not fd or fd == -1:
        return
    try:
        try:
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except ImportError:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(fd)
    except Exception:
        pass


# ---------------------------------------------------------------- 路由
def route(now: datetime) -> tuple[str, str]:
    """按当前时点决定走哪条轨。返回 (动作, 说明)。

    v1.3 起主路径是「采样轨」：9:20 / 9:24 两个窗口双源并行采样，
    9:25 改用撮合后的静止窗（9:25–9:29:50）。9:20 是唯一不可事后还原的
    时点（条件1 的分母），采样资源全部压在它上面。
    东财回算（依赖 details）仍为兜底 —— 当前网络下成功率 0~40%。
    """
    t = now.time()
    if t < COLLECT_END:
        if t < AUCTION_OPEN:
            return "collect", ("集合竞价 09:15 才开始（现在 %s），"
                               "程序会等到 09:19:40 开始采样" % now.strftime("%H:%M"))
        if t >= dtime(9, 20, 30):
            return "collect", ("采样轨（现在 %s）：9:20 窗口已过 —— 条件1 无法判定，"
                               "本次结果会标注为「低置信度」"
                               % now.strftime("%H:%M"))
        return "collect", ("采样轨（现在 %s）：9:20 窗口双源采样中，"
                           "之后还有 9:24 和 9:25 静止窗" % now.strftime("%H:%M"))
    return "resolve", ("竞价已定型（现在 %s），优先用当天采样结果；"
                       "9:25 缺失会用腾讯分时补，9:20/9:24 缺失则输出低置信度结果"
                       % now.strftime("%H:%M"))


def _countdown_wait(target: datetime):
    """等待到指定时刻，每秒刷新倒计时；Ctrl+C 可随时中断。"""
    while True:
        left = (target - datetime.now()).total_seconds()
        if left <= 0:
            return
        m, s = divmod(int(left), 60)
        print("    距开始还有 %02d:%02d ..." % (m, s), end="\r", flush=True)
        time.sleep(min(left, 1.0))


# ---------------------------------------------------------------- 降级
def _today_snapshots(day: str):
    """找当天实时轨存下的三份快照；缺任何一份都无法复盘（条件1 要 9:20）。"""
    d = sc.SNAP_DIR / day
    if not d.exists():
        return None
    got = {}
    for tag in ("0920", "0924", "0925"):
        fs = sorted(d.glob("spot_%s_*.json" % tag))
        if fs:
            got[tag] = fs[-1]
    return got if len(got) == 3 else None


def _notify_fail(day: str, reason: str):
    sc.write_fail(day, reason)


# ---------------------------------------------------------------- 主流程
def run_once(args) -> int:
    cfg = sc.load_cfg()
    now = datetime.now()
    day = now.strftime("%Y-%m-%d")

    if args.mode == "auto":
        action, note = route(now)
    else:
        action, note = args.mode, "手动指定 %s 模式" % _MODE_LABEL.get(
            args.mode, args.mode)

    print("=" * 70)
    print("  竞价选股助手   %s" % now.strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)
    print(">>> %s" % note)
    sc.log_line("runner action=%s mode=%s note=%s" % (action, args.mode, note))

    # ---- 收盘后预筛：把条件 4/5/6 提前算好 ----
    if action == "prep":
        return _do_prep(cfg, args)

    # ---- 采样轨：三个窗口多轮冗余采样 + 出结果 ----
    if action == "collect":
        return _do_collect(cfg, args, day)

    # ---- 定型轨（09:25:45 之后）：优先用当天采样数据 ----
    if action == "resolve":
        return _do_resolve(cfg, args, day)

    # ---- 兼容旧模式 ----
    if action == "wait_live":
        if args.no_wait:
            print("[!] 现在还没到集合竞价时间，本次不等待（--no-wait）。")
            return 4
        target = now.replace(hour=9, minute=19, second=45, microsecond=0)
        print(">>> 等待到 09:19:45 开始采样（可随时按 Ctrl+C 退出）")
        try:
            _countdown_wait(target)
        except KeyboardInterrupt:
            print("\n[!] 已取消。")
            return 4
        return _do_collect(cfg, args, day)

    if action == "live":
        # 旧的「三次快照」轨保留兼容，但默认已由采样轨取代
        if datetime.now().time() >= LIVE_CUTOFF:
            print("[!] 已过 09:25，无法回到 9:20/9:24 取数，转定型轨")
            return _do_resolve(cfg, args, day)
        src = ds.build_source(cfg, prefer=args.source)
        print(">>> 数据源：%s" % src.name)
        try:
            sc.run_live(cfg, src)
        except SystemExit as e:
            return int(e.code or 2)
        print("\n>>> 本次为【旧实时轨】：只抓了三次瞬时快照。")
        print("    建议用采样轨（不加 --mode 直接跑），它对单次失败有冗余。")
        return 0

    return _do_backfill(cfg, args, day)


def _do_prep(cfg, args) -> int:
    """收盘后预筛候选池。定时任务 15:30 触发，也可手动跑。"""
    import prep
    src_pref = "tencent" if args.source == "auto" else args.source
    # ★ 这里传 --kworkers（默认 12），但 build_pool 内部会夹到 HIST_WORKERS_MAX=4。
    #   预筛要打 2272 只日K，12 线程 ≈ 110 只/秒 会被腾讯 WAF 501，
    #   进而降级到新浪「不复权」日K → MA 口径漂移 → 漏临界股。
    #   夹取放在 build_pool 里做，任何调用方都绕不过去。
    res = prep.build_pool(cfg, for_date=getattr(args, "for_date", None),
                          workers=getattr(args, "kworkers", 10),
                          source=src_pref)
    if res is None:
        print("[X] 预筛未完成，候选池没有生成。")
        return 1
    print("\n>>> 预筛完成：%s 交易日可用，共 %d 只候选。"
          % (res["for_date"], res["count"]))
    return 0


def _do_collect(cfg, args, day) -> int:
    """采样轨主流程。异常一律兜住，绝不静默退出。"""
    import collector
    try:
        return collector.run_collect(cfg, day=day, quiet=False)
    except BaseException:
        import traceback
        tb = traceback.format_exc()
        sc.log_line("!! 采样轨异常\n" + tb)
        print(tb)
        last = (tb.strip().splitlines() or ["未知错误"])[-1][:160]
        sc.write_fail(day, "采样轨异常：" + last)
        return 1


def _do_resolve(cfg, args, day) -> int:
    """09:25:45 之后：优先用当天采样数据出结果，东财回算只作为兜底。

    为什么优先采样：条件1/2 需要的 9:20 量 / 9:24 价，采样轨已经用多轮冗余
    拿到了；东财 details 在当前网络下成功率 0~40%，没有理由把它当主路径。

    9:25 缺失时先用腾讯分时补（条件3 可后补），补不上才考虑东财。
    """
    import collector
    from prep import load_pool, build_pool

    snaps, meta, problems = collector.pick_samples(day)
    pool = load_pool(day)

    # ★ 池子缺失的兜底（2026-10-21 实盘事故）：昨夜 15:30 预筛因日K接口
    # 全部返回空而失败，pool_20261021.json 根本没生成。旧逻辑对 pool=None
    # 完全沉默：采样退化为全市场、条件4/5/6 静默不失效但结果里仍写「已验证」，
    # 最后输出 78 只未过任何历史条件的股票。宁可多花 2 分钟现场补建。
    if not pool or not pool.get("items"):
        print("[!] 今天没有候选池（预筛未完成或失败）—— 现场补建，约需 2 分钟")
        sc.log_line("!! 候选池缺失 %s，定型轨现场补建" % day)
        try:
            pool = build_pool(for_date=day, quiet=True)
        except Exception as e:
            sc.log_line("!! 现场补建候选池异常 %s: %s" % (type(e).__name__, e))
            pool = None
        if pool and pool.get("items"):
            print("[OK] 现场补建成功：%d 只" % int(pool.get("count") or 0))
            sc.log_line("候选池现场补建成功 %s 共 %d 只" % (day, int(pool.get("count") or 0)))
        else:
            print("[X] 现场补建失败 —— 条件4/5/6 无法校验，本次结果只能标为低置信度")
            sc.log_line("!! 候选池现场补建失败 %s" % day)
            pool = None

    if "0925" not in snaps:
        df925, note = collector.try_minute_0925(pool)
        if df925 is not None:
            snaps["0925"] = df925
            meta["0925"] = "9:25 %s" % note
            # 这条原始告警已经不成立了，去掉，避免结果里自相矛盾
            problems = [p for p in problems if "9:25" not in p]
        else:
            print("    [!] %s" % note)

    if all(k in snaps for k in ("0920", "0924", "0925")):
        # 【闸门】窗口齐全 ≠ 数据可用。2026-10-20 实盘事故：腾讯快照在 9:25
        # 撮合前 [6]/[37] 恒为 0，三个窗口都「采到了」，可条件1 的分母全是 0，
        # 17 只候选被静默剔除，最后输出「今日无股票」——假阴性，客户会当真。
        health = collector.amount_health(snaps)
        if not health["0920_ok"]:
            print("\n[!] 9:20 快照采到了（%d 只）但竞价金额全是 0 —— 数据源不给竞价期金额"
                  % health["0920_rows"])
            print("    腾讯 qt.gtimg.cn 在 9:25 撮合前只返回匹配价，成交量/成交额一律为 0。")
            print("    → 条件1（9:25额/9:20额）无法判定，条件2 的 9:24 价格仍有效。")
            if health["0924_ok"]:
                print("[!] 9:24 金额同样全 0，但价格有值 —— 条件2 照常判定。")
            cond = "条件1（9:20 竞价金额）"
            reason = ("9:20 快照采到了 %d 只（覆盖率正常），但当时的累计竞价金额全为 0 —— "
                      "数据源在集合竞价撮合前不返回成交额，不是采样失败，也不是今天没有股票。"
                      % health["0920_rows"])
            return collector._partial_output(
                sc, cfg, None, day, snaps, pool, problems, cond, ["0920"],
                reason=reason, cond2_ok=bool(health["0924_price_ok"]))
        print(">>> 当天采样数据完整，直接用采样结果（不依赖东财）")
        for k in ("0920", "0924", "0925"):
            print("    %s" % meta.get(k, "-"))
        src = ds.build_source(cfg, prefer=args.source)
        res, funnel = sc.screen(snaps, src, cfg, day, pool=pool)
        warn = "；".join(problems) if problems else None
        text = sc.render_result(res, funnel, meta, warn)
        print(text)
        sc.log_line("定型轨 %s 命中 %d 只\n%s"
                    % (day, 0 if res is None else len(res), text))
        fp = sc.export_csv(res, datetime.now())
        if fp:
            print("\n结果已导出：%s" % fp)
        print("最新结果：%s" % sc.save_latest(res, fp, day))
        return 0

    if "0925" in snaps:
        missing = [k for k in ("0920", "0924") if k not in snaps]
        cond = "、".join("条件%d" % (1 if k == "0920" else 2) for k in missing)
        print("\n[!] 缺 %s 窗口数据，%s 无法判定 —— 输出低置信度参考清单"
              % ("、".join(missing), cond))
        return collector._partial_output(sc, cfg, None, day, snaps, pool,
                                         problems, cond, missing)

    print("[!] 当天没有任何可用采样：%s" % ("；".join(problems) or "无采样记录"))
    print(">>> 回退东财回算（兜底路径，东财当前可能不可用）...")
    return _do_backfill(cfg, args, day)


def _do_backfill(cfg, args, day) -> int:
    """东财回算轨 —— v1.2 起降级为兜底路径。"""
    # 手动指定回算时若正好卡在 09:25:00~09:25:40，撮合记录可能还没落到分笔里
    now_t = datetime.now().time()
    if dtime(9, 25, 0) <= now_t < dtime(9, 25, BACKFILL_SETTLE):
        target = datetime.now().replace(hour=9, minute=25,
                                        second=BACKFILL_SETTLE, microsecond=0)
        print(">>> 等待撮合明细落地 ...")
        time.sleep(max(0.0, (target - datetime.now()).total_seconds()))

    src = ds.build_source(cfg, prefer=args.source)
    print(">>> 数据源：%s" % src.name)

    stats: dict = {}
    try:
        rc = bf.run_backfill(cfg, source=args.source,
                             workers=args.workers, kworkers=args.kworkers,
                             eworkers=args.eworkers, exact=not args.no_exact,
                             limit=args.limit, stats=stats)
    except BaseException:
        import traceback
        tb = traceback.format_exc()
        sc.log_line("!! 回算轨异常\n" + tb)
        print(tb)
        last = (tb.strip().splitlines() or ["未知错误"])[-1][:160]
        stats.setdefault("error", "回算过程异常：" + last)
        rc = 1

    total = int(stats.get("total") or 0)
    req_fail = int(stats.get("req_fail") or 0)
    aborted = bool(stats.get("aborted"))
    detail_dead = bool(total) and (req_fail >= total or aborted)

    if rc == 0 and not detail_dead:
        print("\n>>> 本次为【东财回算轨】：分笔明细还原 9:20/9:24/9:25，结果可复现。")
        return 0

    err = str(stats.get("error") or "").strip()
    # detail_dead：backfill 已明确判定「明细不可用」并主动不写结果文件，
    # 此时不能套用通用错误文案，要走下面的明细分支（先试采样复盘）。
    if rc != 0 and not err and not stats.get("detail_dead"):
        err = "回算未能完成（数据源不可用或返回不完整），详见 output\\logs\\ 里的日志"
    if err:
        print("\n[!] %s" % err)
        _notify_fail(day, err)
        print("[X] 已写入输出\\最新结果.txt（不会让你看到上一次的旧结果）")
        return 1

    if aborted:
        print("\n[!] 竞价分笔明细连续失败，已主动中止（请求失败 %d/%d）"
              % (req_fail, total))
        print("    中止是为了避免把本机 IP 打进东财风控冷却期。")
    else:
        print("\n[!] 竞价分笔明细整段不可用（请求失败 %d/%d）" % (req_fail, total))

    # 采样轨留下的多轮数据比旧的"三次快照"质量更好，优先用它复盘
    import collector
    snaps, meta, problems = collector.pick_samples(day)
    if all(k in snaps for k in ("0920", "0924", "0925")):
        print("[!] 改用当天采样数据出结果（多轮采样挑代表样本，精度高于三次快照）")
        from prep import load_pool
        res, funnel = sc.screen(snaps, src, cfg, day, pool=load_pool(day))
        text = sc.render_result(res, funnel, meta,
                                "本次为【降级】：东财分笔不可用，改用当天采样轨数据")
        print(text)
        sc.log_line("降级采样复盘 %s 命中 %d 只" % (day, 0 if res is None else len(res)))
        print("最新结果：%s" % sc.save_latest(res, None, day))
        return 0

    reason = ("东方财富分笔明细连续失败%s（%d/%d 只请求失败），"
              "且当天没有完整采样可用于复盘。"
              "通常是东财对高频请求的临时风控，等一会儿再跑即可。"
              % ("，已自动中止" if aborted else "", req_fail, total))
    _notify_fail(day, reason)
    print("[X] %s" % reason)
    return 1


# ---------------------------------------------------------------- CLI
def _cli():
    ap = argparse.ArgumentParser(description="集合竞价选股助手（采样轨为主，东财回算兜底）")
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "prep", "collect", "resolve",
                             "live", "backfill"],
                    help="auto=按当前时间自动选路（默认）；prep=收盘预筛；"
                         "collect=采样轨；resolve=用当天采样出结果")
    ap.add_argument("--for-date", default=None,
                    help="prep 模式：候选池服务的交易日 YYYY-MM-DD")
    ap.add_argument("--source", default="auto",
                    choices=["auto", "eastmoney", "tencent"], help="数据源")
    ap.add_argument("--no-wait", action="store_true",
                    help="还没到竞价时间时不要等待，直接退出")
    ap.add_argument("--no-exact", action="store_true",
                    help="回算轨跳过东财精确复核（全用腾讯估算值）")
    ap.add_argument("--workers", type=int, default=5, help="竞价明细并发数")
    ap.add_argument("--kworkers", type=int, default=12, help="日K并发数")
    ap.add_argument("--eworkers", type=int, default=3, help="东财复核并发数")
    ap.add_argument("--limit", type=int, default=0, help="只处理前N只（调试）")
    ap.add_argument("--selftest", action="store_true", help="环境自检")
    args = ap.parse_args()

    if args.selftest:
        cfg = sc.load_cfg()
        return sc.run_selftest(cfg, ds.build_source(cfg, prefer=args.source))

    fd = _acquire_lock()
    if fd is None:
        print("[!] 已经有一个程序在运行（多半是定时任务），本次直接退出。")
        print("    等它跑完再试，或看 output\\logs\\ 里的进度。")
        return 3
    try:
        return run_once(args)
    finally:
        _release_lock(fd)


def main():
    ds.setup_std_encoding()
    try:
        return _cli()
    except SystemExit:
        raise
    except BaseException:
        import traceback
        tb = traceback.format_exc()
        sc.log_line("!! 未捕获异常，进程退出\n" + tb)
        last = (tb.strip().splitlines() or ["未知错误"])[-1][:180]
        sc.write_fail(datetime.now().strftime("%Y-%m-%d"), "程序运行异常：" + last)
        print("\n[!] 程序异常退出，详情见 output\\logs\\ 与 最新结果.txt")
        print(tb)
        return 1


if __name__ == "__main__":
    sys.exit(main())
