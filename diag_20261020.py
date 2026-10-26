# -*- coding: utf-8 -*-
"""2026-10-20 竞价数据源缺陷诊断（只读，不改任何文件）

背景：今天 9:20 / 9:24 两个窗口共 57 轮快照，100 只股票的 volume/amount 全为 0，
      只有 9:25:04 之后才有值 —— 导致条件1（9:25额/9:20额>12倍）无法计算，
      17 只全部被排除，输出「今日无股票」。
本脚本用已落盘的快照复原漏斗，并回答两个问题：
  1) 被排除的 17 只是哪些？
  2) 如果条件1 数据正常，条件2 能过几只？
"""
import json
import glob
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def find_dirs():
    out = []
    for p in (ROOT / "output", ROOT / "delivery" / "竞价选股助手" / "output"):
        if (p / "snapshots" / "20261020" / "rounds").is_dir():
            out.append(p)
    return out


def load_round(day_dir: Path, tag: str, want_clock: str | None = None,
               need_amount: bool = False):
    """取某窗口里最贴近指定时刻的一轮；need_amount=True 时只接受金额非零的轮。"""
    fs = sorted(glob.glob(str(day_dir / ("r%s_*.json" % tag))))
    if not fs:
        return None
    if want_clock:
        target = want_clock.replace(":", "")
        fs.sort(key=lambda f: abs(int(Path(f).stem.split("_")[-2]) - int(target)))
    for f in fs:
        d = json.loads(Path(f).read_text(encoding="utf-8"))
        if need_amount:
            n = sum(1 for r in d["rows"] if (r.get("amount") or 0) > 0)
            if n == 0:
                continue
        return {str(r["code"]): r for r in d["rows"]}, Path(f).name, d["captured_at"]
    return {str(r["code"]): r for r in d["rows"]}, Path(fs[-1]).name, d["captured_at"]


def main():
    dirs = find_dirs()
    if not dirs:
        print("找不到 20261020 的 rounds 目录")
        return 1
    out = dirs[0]
    day_dir = out / "snapshots" / "20261020" / "rounds"

    # 窗口统计
    print("=" * 78)
    print("  一、三个窗口的数据完整性（100 只中 amount>0 的只数）")
    print("=" * 78)
    for tag, label in (("0920", "9:20"), ("0924", "9:24"), ("0925", "9:25")):
        fs = sorted(glob.glob(str(day_dir / ("r%s_*.json" % tag))))
        print("\n%s 窗口共 %d 轮：" % (label, len(fs)))
        for f in fs[:3] + (["..."] if len(fs) > 6 else []) + fs[-3:] if len(fs) > 6 else fs:
            if f == "...":
                print("   ...")
                continue
            d = json.loads(Path(f).read_text(encoding="utf-8"))
            rows = d["rows"]
            na = sum(1 for r in rows if (r.get("amount") or 0) > 0)
            print("   %-24s at=%s  amount>0 = %3d / %d"
                  % (Path(f).name, d["captured_at"][-8:], na, len(rows)))

    r920, f920, t920 = load_round(day_dir, "0920", "09:20:00")
    r924, f924, t924 = load_round(day_dir, "0924", "09:24:00")
    r925, f925, t925 = load_round(day_dir, "0925", need_amount=True)
    print("\n选定：920=%s(%s)  924=%s(%s)  925=%s(%s)"
          % (f920, t920, f924, t924, f925, t925))

    # 候选池
    pool = None
    for p in (out / "pool" / "pool_20261020.json",
              ROOT / "output" / "pool" / "pool_20261020.json"):
        if p.exists():
            pool = json.loads(p.read_text(encoding="utf-8"))
            break
    pmap = {str(it["code"]): it for it in pool["items"]} if pool else {}
    print("候选池：%s 只" % (len(pmap) or "缺失"))

    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))

    # 复原漏斗（前 5 条用 9:25 快照）
    rows = []
    for code, r in r925.items():
        it = pmap.get(code) or {}
        pc = (r.get("prev_close") or 0) or (it.get("ref_close") or 0)
        price = r.get("price") or 0
        amt = r.get("amount") or 0
        chg = (price / pc - 1) * 100 if pc else None
        fmv = r.get("float_mv") or 0
        p924 = (r924.get(code) or {}).get("price") or 0
        a920 = (r920.get(code) or {}).get("amount") or 0
        rows.append(dict(code=code, name=r.get("name"), price=price, prev=pc,
                         chg=chg, amt=amt, fmv=fmv, p924=p924, a920=a920,
                         in_pool=code in pmap, shares=it.get("shares")))

    def keep(f, label, lst):
        got = [x for x in lst if f(x)]
        print("  %-28s %4d -> %4d" % (label, len(lst), len(got)))
        return got

    print("\n" + "=" * 78)
    print("  二、漏斗复原（9:25 快照 + 候选池）")
    print("=" * 78)
    d = rows
    print("  %-28s %4d" % ("候选池全部", len(d)))
    d = keep(lambda x: x["in_pool"], "主板 & 非ST（池内）", d)
    d = keep(lambda x: x["chg"] is not None
             and cfg["auction_chg_min"] < x["chg"] < cfg["auction_chg_max"],
             "竞价涨跌幅 (%.1f%%, %.1f%%)" % (cfg["auction_chg_min"], cfg["auction_chg_max"]), d)
    d = keep(lambda x: x["amt"] > cfg["min_auction_amount"], "9:25金额 > %.0f万"
             % (cfg["min_auction_amount"] / 1e4), d)
    d = keep(lambda x: 0 < x["price"] < cfg["max_price"], "股价 < %.0f元" % cfg["max_price"], d)
    d = keep(lambda x: 0 < x["fmv"] < cfg["max_market_cap"], "市值 < %.0f亿"
             % (cfg["max_market_cap"] / 1e8), d)

    survive5 = d
    print("\n  过完 5 个条件的 %d 只：" % len(survive5))
    for x in sorted(survive5, key=lambda y: -y["amt"]):
        print("    %s %-6s 竞价%7.2f  涨幅%6.2f%%  金额%8.0f万  9:20额=%s"
              % (x["code"], (x["name"] or "")[:6], x["price"], x["chg"],
                 x["amt"] / 1e4, ("%.1f万" % (x["a920"] / 1e4)) if x["a920"] else "**0（数据缺失）**"))

    print("\n" + "=" * 78)
    print("  三、若条件1 数据正常，条件2（9:25价>=9:24价）能过几只")
    print("=" * 78)
    cond1_ok = [x for x in survive5 if x["a920"] > 0]
    pass2 = [x for x in survive5 if x["p924"] > 0 and x["price"] >= x["p924"]]
    print("  条件1 有数据的：%d / %d 只（其余 %d 只无法计算）"
          % (len(cond1_ok), len(survive5), len(survive5) - len(cond1_ok)))
    print("  仅看条件2 通过的：%d 只" % len(pass2))
    for x in sorted(pass2, key=lambda y: -y["amt"]):
        ratio = ""
        if x["a920"] > 0:
            ratio = "  放大 %.1f 倍" % (x["amt"] / x["a920"])
        print("    %s %-6s 9:25=%6.2f 9:24=%6.2f (+%.2f%%)  金额%8.0f万%s"
              % (x["code"], (x["name"] or "")[:6], x["price"], x["p924"],
                 (x["price"] / x["p924"] - 1) * 100, x["amt"] / 1e4, ratio))
    return 0


if __name__ == "__main__":
    sys.exit(main())
