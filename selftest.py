# -*- coding: utf-8 -*-
"""
端到端自检脚本
==============================================================================
用途：
  1. 盘前确认「数据源 + 筛选链路」是否正常（不用等 9:20）；
  2. 调试/改参数后快速看效果。

原理：
  非竞价时段拿不到真实的 9:20/9:24 竞价序列，因此这里用「当前快照」构造：
    - 9:25 快照 = 真实快照
    - 9:20 快照 = 成交额缩小 N 倍（模拟竞价资金后段涌入）
    - 9:24 快照 = 真实快照（价格与 9:25 相同，条件2 视为通过）
  这样能完整跑通 L1 + L2 全部代码路径。
  ※ 结果无实战意义，只验证链路。

用法：
  python selftest.py                    # 用配置里的默认源
  python selftest.py --source tencent
  python selftest.py --shrink 15        # 9:20 金额 = 真实/15
==============================================================================
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

import datasource as ds
from screener import load_cfg, render_result, screen


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="auto",
                    choices=["auto", "eastmoney", "tencent"])
    ap.add_argument("--shrink", type=float, default=15.0,
                    help="9:20 快照成交额的缩小倍数（模拟竞价放大）")
    args = ap.parse_args()

    cfg = load_cfg()
    src = ds.build_source(cfg, prefer=args.source)
    print(f">>> 数据源：{src.name}")

    print(">>> 拉取全市场快照 ...")
    snap = src.spot()
    print(f"    快照 {len(snap)} 只")

    s920 = snap.copy()
    s920["amount"] = snap["amount"] / args.shrink
    s924 = snap.copy()
    snaps = {"0920": s920, "0924": s924, "0925": snap}

    print(">>> 跑筛选链路 ...")
    res, funnel = screen(snaps, src, cfg, datetime.now().strftime("%Y-%m-%d"))
    print(render_result(
        res, funnel,
        {"0920": "构造值", "0924": "真实快照", "0925": "真实快照"},
        warn=f"自检模式：9:20 快照为构造值（真实/{args.shrink:.0f}），仅验证代码链路，"
             f"结果无实战意义"))
    return res


if __name__ == "__main__":
    main()
