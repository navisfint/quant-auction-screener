# -*- coding: utf-8 -*-
"""
数据源层
==============================================================================
统一两个免费行情源的字段口径，供 screener.py 调用。

  EastMoneySource （主）—— 东方财富，字段最全（盘中竞价 + 日K 换手率/成交额）
  TencentSource   （备）—— 腾讯快照（批量极快）+ 新浪日K（含成交额/换手率）

统一输出口径：
  spot()  -> DataFrame(index=code) 列：
      code, name, market(1=沪 0=深), price, chg_pct, amount, prev_close,
      total_mv, float_mv, turnover
      * 金额/市值单位 = 元
  kline() -> DataFrame 列：
      date, close, chg_pct, amount, turnover
      * amount 单位 = 元；turnover 单位 = %（百分数，如 0.21 表示 0.21%）
==============================================================================
"""

from __future__ import annotations

import json
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# 接口地址
# ---------------------------------------------------------------------------
EM_CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
EM_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

# 东财镜像域名（数据相同，可用性间歇）—— 见 http_json_hosts 说明
EM_CLIST_HOSTS = ["push2.eastmoney.com", "82.push2.eastmoney.com",
                  "push2delay.eastmoney.com"]
EM_KLINE_HOSTS = ["push2his.eastmoney.com", "82.push2his.eastmoney.com",
                  "push2.eastmoney.com"]
EM_DETAIL_HOSTS = ["82.push2.eastmoney.com", "push2delay.eastmoney.com",
                   "push2his.eastmoney.com", "push2.eastmoney.com"]
# ---------------------------------------------------------------------------
# 东财全局请求节流（2026-10-19 加，★ 红线：不要删）
# ---------------------------------------------------------------------------
# 为什么必须节流 —— 实测证据（2026-10-19 14:08~14:11，同一端点）：
#   14:11:20  单次请求（距上次约 3 分钟）  -> HTTP 200 成功
#   14:11:36  紧接着连发 6 次，间隔 1 秒    -> 0/6 全断
# 同一网址 3 分钟内结果相反，说明风控是**速率触发**的，不是永久封 IP。
# 社区口径（a-stock-data 防封文档 / efinance issue #208）：
#   >5 次/秒、单 IP 并发 ≥10、1 分钟 ≥200 次、5 分钟 ≥300 次 即触发封禁；
#   重度案例（1 小时 4.5 万请求）IP 封禁持续 20+ 小时。
# 我们过去的做法正好踩线：details 3~5 并发 + 单只重试 30 次 × 4 域名
#   => 45 只候选可打出上千次请求，客户机跑一次就被打进冷却期。
#
# 实现：用一把锁把「间隔控制」和「串行化」合并。lock 内 sleep 意味着
# 即使上层开了多线程，实际也是排队逐个发出，天然满足「不对东财并发」。
# 抖动是刻意的 —— 风控对「每 2 秒整一个请求」这种固定节拍最敏感。
_THROTTLE_LOCK = threading.Lock()
_LAST_EM_CALL = [0.0]

EM_MIN_INTERVAL = 2.0       # 秒；批量场景 1.5~2.0 是安全区


def em_throttle(min_interval: float | None = None) -> None:
    """所有东财请求的统一排队入口：调用方在「发请求之前」调一次即可。"""
    iv = EM_MIN_INTERVAL if min_interval is None else float(min_interval)
    with _THROTTLE_LOCK:
        wait = _LAST_EM_CALL[0] + iv - time.time()
        if wait > 0:
            time.sleep(wait + random.uniform(0.05, 0.35))
        _LAST_EM_CALL[0] = time.time()


# ---------------------------------------------------------------------------
# 东财整体可用性熔断
# ---------------------------------------------------------------------------
_BREAKER_LOCK = threading.Lock()


class _Breaker:
    """东财 clist / kline / details 是「一起活、一起死」的。

    实测（2026-10-19）：某些时段全部域名的全部接口同时 RemoteDisconnected，
    短则几十秒，长则几十分钟。此时逐只股票硬顶重试毫无意义 ——
    46 只候选 × 8 次重试 × 20s 超时 ≈ 40 分钟，表现就是"程序卡死"。

    策略：连续 threshold 次调用彻底失败 → 冷却 cooldown 秒，期间所有东财
    请求立刻报错，让上层快速走降级（腾讯源 / 当天快照复盘）。冷却结束
    自动半开，第一次成功即清零。
    """

    def __init__(self, name: str = "东财", threshold: int = 2,
                 cooldown: float = 90.0):
        self.name = name
        self.threshold = threshold
        self.cooldown = cooldown
        self.fails = 0
        self.open_until = 0.0

    def is_open(self) -> bool:
        return time.time() < self.open_until

    def mark_fail(self):
        with _BREAKER_LOCK:
            self.fails += 1
            if self.fails >= self.threshold:
                if not self.is_open():      # 只提示一次，别刷屏
                    print("    [!] %s接口连续失败，暂停 %d 秒后自动重试"
                          "（期间走备用源）" % (self.name, int(self.cooldown)))
                self.open_until = time.time() + self.cooldown

    def mark_ok(self):
        with _BREAKER_LOCK:
            self.fails = 0
            self.open_until = 0.0


# 阈值从 2 放宽到 8（2026-10-19）：实测风控是间歇性的 —— 连续 2 只失败
# 在速率风控下极易误触发，一熔断就整轮报「接口不可用」，把本来重试就能
# 过的请求全放弃了。8 次才熔断，配合 em_throttle 的 2 秒间隔，误判率大降；
# 冷却 120 秒比原来 90 秒更贴合实测的恢复节奏。
EM_BREAKER = _Breaker(name="东方财富", threshold=8, cooldown=120.0)
# 腾讯日K 单独的熔断器：它的封锁是 WAF 返回 501 验证页，跟东财的
# "连接被重置"不是一回事，冷却时间给长一点（WAF 封禁通常按分钟计）。
TX_BREAKER = _Breaker(name="腾讯日K", threshold=3, cooldown=180.0)

TX_QUOTE = "https://qt.gtimg.cn/q="
# 新浪快照：采集轨的第二条通道（2026-10-19 加入）
#   腾讯 qt.gtimg.cn 与新浪 hq.sinajs.cn 是两个完全独立的域名族，
#   历史上从未同时挂过。9:20 那一次抓取是「条件1」唯一的机会，
#   单源失败即当天作废，因此必须双源并行。
#   代价：新浪这档不给市值/换手率/涨跌幅，需自算或用预筛池的静态字段补。
SINA_QUOTE = "https://hq.sinajs.cn/list="
SINA_REFERER = {"Referer": "https://finance.sina.com.cn/"}
SINA_LIST = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
             "Market_Center.getHQNodeData")

# ★ 日K「复权口径」统计（2026-10-19 新增）
#   qfq  —— 腾讯前复权，与行情软件（同花顺/通达信默认）一致，MA 判定口径正确
#   none —— 新浪不复权，降级路径。除息日会让历史价格偏高 → MA 偏高 →
#          「昨收 > MA」被低估，**边界股会被漏掉**（实测 002972：qfq 通过、
#          不复权差 -0.127% 不通过，池子少 1 只）。
#   两者不能混为一谈，所以单独计数并在预筛结束时明确告知，绝不静默。
KLINE_ADJ = {"qfq": 0, "none": 0}
_KLINE_ADJ_LOCK = threading.Lock()


def _count_adj(kind: str) -> None:
    with _KLINE_ADJ_LOCK:
        KLINE_ADJ[kind] = KLINE_ADJ.get(kind, 0) + 1


def kline_adj_summary() -> str:
    """返回一行人话，用于在预筛/回算结束时提示口径是否发生降级。"""
    q, n = KLINE_ADJ.get("qfq", 0), KLINE_ADJ.get("none", 0)
    if not (q or n):
        return ""
    if n == 0:
        return "日K复权口径：前复权 %d 只（正常）" % q
    if q == 0:
        return ("日K复权口径：[!] 全部为不复权 %d 只（腾讯日K 不可用，已降级）"
                " —— MA 判定偏保守，可能漏掉临界股" % n)
    return ("日K复权口径：[!] 前复权 %d 只 + 不复权 %d 只（混用）"
            " —— 不复权那部分 MA 偏保守，可能漏掉临界股" % (q, n))

FS_ALL_A = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
# f17 = 今开 —— 即成集合竞价 9:25 的撮合价，盘后回算竞价条件时必须用
EM_SPOT_FIELDS = "f2,f3,f5,f6,f8,f12,f13,f14,f17,f18,f20,f21"
EM_SPOT_RENAME = {
    "f12": "code", "f14": "name", "f13": "market",
    "f2": "price", "f3": "chg_pct", "f5": "volume", "f6": "amount",
    "f8": "turnover", "f17": "open", "f18": "prev_close",
    "f20": "total_mv", "f21": "float_mv",
}
EM_KLINE_COLS = ["date", "open", "close", "high", "low", "volume", "amount",
                 "amplitude", "chg_pct", "chg", "turnover"]

def app_dir() -> Path:
    """程序根目录。
    打包成 exe 后 = exe 所在目录（用户看得见的那个文件夹）；
    源码运行 = 本文件所在目录。所有 config / output / logs 都落在这里。

    SCREENER_HOME：可选覆盖。用于「用源码复现客户目录里的数据」——
    例如客户是 delivery\\竞价选股助手\\ 在跑，要在本机看他的 output，
    设 SCREENER_HOME 指过去即可，不必把数据搬来搬去。
    仅在目录确实存在时生效，避免拼错路径后静默写到错误位置。
    """
    env = os.environ.get("SCREENER_HOME")
    if env:
        p = Path(env).expanduser()
        if p.is_dir():
            return p.resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


APP_DIR = app_dir()
OUT_DIR = APP_DIR / "output"
LOG_DIR = OUT_DIR / "logs"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def setup_std_encoding():
    """强制 stdout/stderr 为 UTF-8 —— 这块【不要】改成 GBK。

    机制（Python 3.6+ / PEP 528，Windows 专有）：
      · 输出连到【真实控制台】时，CPython 用专门的 console IO，它期望
        收到 UTF-8 字节，再经 WriteConsoleW 用 Unicode API 渲染 ——
        因此窗口里中文是否正常，跟 chcp 代码页无关，只取决于这里是不是
        UTF-8。若改成 GBK，console IO 仍按 UTF-8 解 → 窗口反而乱码。
      · 输出被【重定向到文件】时，写出的就是 UTF-8 字节。
    结论：UTF-8 在两种场景下都正确，是本项目的唯一正确选择。
    （已实测：客户机窗口中文正常。改 GBK 是错的，勿再"修复"。）
    """
    for s in (sys.stdout, sys.stderr):
        if s is not None:
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    return s


class WafBlocked(RuntimeError):
    """被目标站的 WAF / 风控直接拦截（HTTP 403 / 429 / 501 / 503）。

    重试毫无意义 —— 2026-10-19 实测：腾讯日K 接口返回 501 并跳
    waf.tencent.com 验证页，此时每只股票重试 4 次只会把封禁时间越拖越长，
    表现就是"全市场 1900 只跑了 20 分钟"。必须立刻停手、让上层降级。
    """


def http_get(session, url, params=None, retries=4, timeout=20, headers=None):
    last = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout,
                            headers=headers or {})
            if r.status_code in (403, 429, 501, 503):
                raise WafBlocked(f"HTTP {r.status_code} {url[:50]}")
            r.raise_for_status()
            return r
        except WafBlocked:
            raise                      # 拦截类错误不重试，直接上抛
        except Exception as e:
            last = e
            time.sleep(0.6 + 0.6 * i)
    raise RuntimeError(f"请求失败 {url[:60]}: {last}")


def http_json(session, url, params=None, retries=4, timeout=20, headers=None):
    return http_get(session, url, params, retries, timeout, headers).json()


def http_json_hosts(session, hosts, path, params, retries=8, timeout=20,
                    headers=None):
    """在多个等价域名间轮换请求。

    为什么需要：东财各镜像域名（push2 / 82.push2 / push2delay / push2his）
    返回同一份数据，但**可用性是间歇性的** —— 实测同一域名可能刚成功、
    下次就 ProxyError，换个域名又通。单域名重试会把时间全耗在死域名上。

    ★ 两级快速失败（2026-10-19 加，实测教训）：
      1) 连续 4 次「连接层」失败（ConnectionError/Timeout）即提前放弃 ——
         东财的封锁是整段式的（clist/kline/details 一起断），顶满 8 次
         结果一样，只是把 46 只候选的复核从 1 分钟拖成 40 分钟。
      2) 全局熔断：连续 2 次调用彻底失败后，冷却期内所有东财请求立即
         报错返回，让上层尽快走降级（腾讯源 / 快照复盘）。
    """
    if EM_BREAKER.is_open():
        raise RuntimeError("东方财富接口暂时不可用（熔断中，%d 秒后重试）"
                           % max(0, int(EM_BREAKER.open_until - time.time())))

    last = None
    tried = set()
    conn_streak = 0
    for i in range(retries):
        host = hosts[i % len(hosts)]
        tried.add(host)
        try:
            r = session.get(f"https://{host}{path}", params=params,
                            timeout=timeout, headers=headers or {})
            js = r.json()
            if isinstance(js, dict) and js.get("data"):
                EM_BREAKER.mark_ok()
                return js
            last = RuntimeError(f"空响应 {host}{path[:30]}")
            conn_streak = 0
        except Exception as e:
            last = e
            if isinstance(e, (requests.exceptions.ConnectionError,
                              requests.exceptions.Timeout)):
                conn_streak += 1
                if conn_streak >= 4 and len(tried) >= 2:
                    break           # 整段封锁，别再熬时间
            else:
                conn_streak = 0
        time.sleep(0.5 + 0.45 * i)
    EM_BREAKER.mark_fail()
    raise RuntimeError(f"多域名请求均失败 {path[:50]}: {last}")


# ===========================================================================
# 数据源基类
# ===========================================================================
class DataSource:
    name = "base"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.session = make_session()

    def spot(self) -> pd.DataFrame:
        raise NotImplementedError

    def kline(self, code: str, market: int) -> pd.DataFrame | None:
        raise NotImplementedError


# ===========================================================================
# 东方财富（主源）
# ===========================================================================
class EastMoneySource(DataSource):
    name = "eastmoney"

    def spot(self) -> pd.DataFrame:
        pz = int(self.cfg.get("page_size", 100))
        retries = int(self.cfg.get("http_retries", 4))
        rows, total = [], None
        page, failed = 1, []
        max_page = 200

        while page <= max_page:
            params = {
                "pn": page, "pz": pz, "po": 1, "np": 1,
                "fltt": 2, "invt": 2, "fid": "f12", "fs": FS_ALL_A,
                "fields": EM_SPOT_FIELDS,
            }
            try:
                js = http_json_hosts(self.session, EM_CLIST_HOSTS,
                                     "/api/qt/clist/get", params,
                                     retries=retries)
            except Exception:
                # 单页失败不再让整轮崩溃 —— 记下来最后补拉
                failed.append(page)
                page += 1
                if not rows and page > 5:
                    raise RuntimeError("东财快照连续 5 页失败，接口不可用")
                # ★ 失败页过多说明接口「半死」：能连上但大面积取不到，
                #   拼出来的残缺快照会让所有条件失真。宁可抛错去走腾讯源。
                if len(failed) >= max(3, (page - 1) // 3):
                    raise RuntimeError(
                        "东财快照失败页过多（%d/%d 页），数据不完整，改用备用源"
                        % (len(failed), page - 1))
                continue
            data = js.get("data") or {}
            diff = data.get("diff") or []
            if isinstance(diff, dict):
                diff = list(diff.values())
            if not diff:
                break
            rows.extend(diff)
            total = data.get("total") or total
            if total:
                max_page = min(max_page, (total + pz - 1) // pz)
            page += 1
            # ★ 2026-10-19：原来 page_sleep=0.06 秒 ≈ 16.7 次/秒，直接踩在
            # 东财封禁线（>5 次/秒）上方。改走全局节流，默认 0.5 秒/页
            # （60 页约 30 秒），QPS 压到 2 以下。
            em_throttle(self.cfg.get("page_sleep", 0.5))

        # 补拉先前失败的页（给更长重试）
        for p in failed:
            params = {
                "pn": p, "pz": pz, "po": 1, "np": 1,
                "fltt": 2, "invt": 2, "fid": "f12", "fs": FS_ALL_A,
                "fields": EM_SPOT_FIELDS,
            }
            em_throttle(self.cfg.get("page_sleep", 0.5))
            try:
                js = http_json_hosts(self.session, EM_CLIST_HOSTS,
                                     "/api/qt/clist/get", params,
                                     retries=retries * 3)
                diff = (js.get("data") or {}).get("diff") or []
                if isinstance(diff, dict):
                    diff = list(diff.values())
                rows.extend(diff)
            except Exception:
                print(f"    [警告] 第 {p} 页最终未取到，可能有少量股票缺失")

        if not rows:
            raise RuntimeError("东财快照为空")
        print(f"    分页 {page - 1} 页，失败 {len(failed)} 页，累计 {len(rows)} 行"
              + (f"（应 {total} 行）" if total else ""))
        df = pd.DataFrame(rows).rename(columns=EM_SPOT_RENAME)
        return _finalize_spot(df)

    def kline(self, code: str, market: int) -> pd.DataFrame | None:
        n = int(self.cfg.get("kline_limit", 130))
        # ★ 东财在 beg="0" 时 lmt 不生效，会返回全部历史（实测 600000 返回 6407 根），
        #   单只多传 50 倍数据。必须用 beg 限定起始日期 + 末尾 tail 双保险。
        beg = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
        params = {
            "secid": f"{int(market)}.{code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": 101, "fqt": 1, "beg": beg, "end": "20500101",
            "lmt": n,
        }
        em_throttle()            # ★ 东财所有端点共用一把节流锁（默认 2s）
        try:
            js = http_json_hosts(self.session, EM_KLINE_HOSTS,
                                 "/api/qt/stock/kline/get", params,
                                 retries=self.cfg.get("http_retries", 4))
        except Exception:
            return None
        klines = ((js.get("data") or {}).get("klines")) or []
        if not klines:
            return None
        rows = [x.split(",") for x in klines]
        df = pd.DataFrame(rows)
        if df.shape[1] < len(EM_KLINE_COLS):
            return None
        df = df.iloc[:, :len(EM_KLINE_COLS)]
        df.columns = EM_KLINE_COLS
        for c in EM_KLINE_COLS[1:]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return (df[["date", "close", "chg_pct", "amount", "turnover"]]
                .tail(n).reset_index(drop=True))


# ===========================================================================
# 腾讯 + 新浪（备源）
# ===========================================================================
class TencentSource(DataSource):
    """快照走腾讯批量（600只/请求，全市场约10个请求）；
    日K优先新浪（有成交额/换手率原始字段），无 akshare 时用腾讯日K + 流通股本反推。"""
    name = "tencent"
    BATCH = 600
    degraded_history = False      # 是否走了精度降级路径
    use_sina = True               # 单线程用新浪（有原始成交额/换手率）；
                                  # 多线程必须置 False —— V8 引擎非线程安全

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self._shares: dict[str, float] = {}

    def _stock_list(self) -> list[tuple[str, str, int]]:
        """[(code, symbol, market)] —— 带本地缓存，避免每次拉全市场列表。"""
        cache = OUT_DIR / "stock_list.json"
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86400:
            try:
                obj = json.loads(cache.read_text(encoding="utf-8"))
                items = [(x[0], x[1], int(x[2])) for x in obj["items"]]
                if len(items) > 3000:
                    return items
            except Exception:
                pass

        items, page = [], 1
        while True:
            params = {"page": page, "num": 100, "sort": "symbol", "asc": 1,
                      "node": "hs_a"}
            try:
                txt = http_get(self.session, SINA_LIST, params, retries=3,
                               headers={"Referer": "https://finance.sina.com.cn/"}).text
                arr = json.loads(txt) if txt.strip() else []
            except Exception:
                arr = []
            if not arr:
                break
            for it in arr:
                sym = it.get("symbol", "")
                if len(sym) >= 8:
                    items.append((sym[2:], sym, 1 if sym.startswith("sh") else 0))
            page += 1
            if page > 90:
                break
            time.sleep(0.15)

        if not items:
            raise RuntimeError("股票列表获取失败（新浪列表接口不可用）")
        cache.write_text(json.dumps({"updated": datetime.now().isoformat(),
                                     "items": items}, ensure_ascii=False),
                         encoding="utf-8")
        return items

    def spot(self) -> pd.DataFrame:
        items = self._stock_list()
        rows = []
        for i in range(0, len(items), self.BATCH):
            chunk = items[i:i + self.BATCH]
            q = ",".join(s for _, s, _ in chunk)
            try:
                txt = http_get(self.session, TX_QUOTE + q, retries=3,
                               headers={"Referer": "https://gu.qq.com/"}).content
                txt = txt.decode("gbk", errors="ignore")
            except Exception:
                continue
            rows.extend(_parse_tx_lines(txt))
            time.sleep(0.05)
        if not rows:
            raise RuntimeError("腾讯快照为空")
        df = _finalize_spot(pd.DataFrame(rows))
        # 缓存流通股本（= 流通市值 / 现价），供日K反推换手率使用
        ok = df["price"].notna() & (df["price"] > 0) & df["float_mv"].notna()
        self._shares = dict(zip(df.loc[ok, "code"],
                                (df.loc[ok, "float_mv"] / df.loc[ok, "price"])))
        return df

    def spot_codes(self, codes, batch: int | None = None,
                   retries: int = 1) -> pd.DataFrame:
        """只抓指定代码的快照 —— 候选池高频采样专用。

        为什么需要：全市场 5574 只 = 10 批 ≈ 1s/轮；候选池通常 1 批 ≈ 0.2s。
        9:19–9:26 要连续采样数十轮，按全市场采样会把请求量放大 10 倍，
        既慢又容易触发限流。预筛池把每轮压到 1 个请求。

        codes: ["600354", ...] 或 [("600354", 1), ...]（带 market）。
        返回空表表示这一轮失败 —— 调用方直接进入下一轮，不要在这一轮死磕，
        因为本轮只是几十个样本中的一个。
        """
        batch = batch or self.BATCH
        syms = []
        for it in codes:
            if isinstance(it, (tuple, list)):
                code, mkt = str(it[0]), int(it[1])
            else:
                code = str(it)
                mkt = 1 if code.startswith(("6", "9")) else 0
            syms.append(("sh" if mkt == 1 else "sz") + code.zfill(6))
        rows = []
        for i in range(0, len(syms), batch):
            q = ",".join(syms[i:i + batch])
            try:
                txt = http_get(self.session, TX_QUOTE + q, retries=retries,
                               headers={"Referer": "https://gu.qq.com/"}).content
                rows.extend(_parse_tx_lines(txt.decode("gbk", errors="ignore")))
            except Exception:
                continue
        if not rows:
            return pd.DataFrame()
        return _finalize_spot(pd.DataFrame(rows))

    def spot_codes_sina(self, codes, batch: int = 200,
                        retries: int = 1) -> pd.DataFrame:
        """只抓指定代码的新浪快照 —— 腾讯失败时的第二通道。

        2026-10-19 实测：100 只 1 次请求 1.45 秒，99/100 有效。
        batch 默认 200（不是 BATCH=600）：新浪把代码拼在 URL 里，
        600 只约 5400 字符逼近长度上限，200 只约 1800 字符更稳。

        返回空表表示这一轮失败。缺市值/换手率，由调用方用预筛池补
        （见 collector._backfill_static）。
        """
        syms = []
        for it in codes:
            if isinstance(it, (tuple, list)):
                code, mkt = str(it[0]), int(it[1])
            else:
                code = str(it)
                mkt = 1 if code.startswith(("6", "9")) else 0
            syms.append(("sh" if mkt == 1 else "sz") + code.zfill(6))
        rows = []
        for i in range(0, len(syms), batch):
            q = ",".join(syms[i:i + batch])
            try:
                r = http_get(self.session, SINA_QUOTE + q, retries=retries,
                             timeout=4, headers=SINA_REFERER)
                rows.extend(_parse_sina_lines(
                    r.content.decode("gbk", errors="ignore")))
            except Exception:
                continue
        if not rows:
            return pd.DataFrame()
        return _finalize_spot(pd.DataFrame(rows))

    def kline(self, code: str, market: int) -> pd.DataFrame | None:
        sym = ("sh" if int(market) == 1 else "sz") + str(code)
        # 优先新浪（有成交额与换手率）
        # 【严禁在多线程里走这条分支】akshare 的新浪日K依赖 py_mini_racer(V8)，
        # 该引擎不是线程安全的 —— 12 线程并发实测直接段错误崩掉整个进程
        # (mini_racer.dll 栈)。并发场景必须设 use_sina=False 走下面的纯
        # HTTP 腾讯日K + 流通股本反推。
        if self.use_sina:
            try:
                import akshare as ak
                df = ak.stock_zh_a_daily(symbol=sym, adjust="qfq")
                out = pd.DataFrame({
                    "date": df["date"].astype(str).str.slice(0, 10),
                    "close": pd.to_numeric(df["close"], errors="coerce"),
                    "chg_pct": pd.to_numeric(df["close"], errors="coerce")
                                .pct_change() * 100.0,
                    "amount": pd.to_numeric(df["amount"], errors="coerce"),
                    "turnover": pd.to_numeric(df["turnover"], errors="coerce") * 100.0,
                })
                n = self.cfg.get("kline_limit", 130)
                return out.tail(n).reset_index(drop=True)
            except Exception:
                pass
        # 降级：腾讯日K（无成交额/换手率字段）
        #   → 换手率用「流通股本」反推（实测与新浪真实值完全一致）
        #   → 成交额用「成交量 × 四价均价」估算（相邻两日同法估算，比值误差可抵消）
        # ★ 熔断只跳过「腾讯这一档」，必须继续往下走新浪兜底。
        #   2026-10-19 踩过：这里原来写成 `if TX_BREAKER.is_open(): return None`，
        #   结果腾讯被封时把新浪兜底也一起跳过了 —— 1887 只全判"日K无效"，
        #   程序却因为"日K全失败"之外的路径静默输出 0 只命中。
        if not TX_BREAKER.is_open():
            try:
                js = http_json(self.session,
                               "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                               {"param": f"{sym},day,,,{self.cfg.get('kline_limit', 130)},qfq"})
                node = (js.get("data") or {}).get(sym) or {}
                arr = node.get("qfqday") or node.get("day") or []
                if arr:
                    out = pd.DataFrame([r[:6] for r in arr],
                                       columns=["date", "open", "close", "high",
                                                "low", "volume"])
                    for c in ["open", "close", "high", "low", "volume"]:
                        out[c] = pd.to_numeric(out[c], errors="coerce")
                    out["chg_pct"] = out["close"].pct_change() * 100.0
                    vwap = out[["open", "close", "high", "low"]].mean(axis=1)
                    out["amount"] = out["volume"] * 100.0 * vwap      # 手 → 股
                    shares = self._shares.get(str(code))
                    out["turnover"] = (out["volume"] * 100.0 / shares * 100.0
                                       if shares and shares > 0 else None)
                    self.degraded_history = True
                    TX_BREAKER.mark_ok()
                    _count_adj("qfq")          # 腾讯前复权，MA 口径正确
                    return out[["date", "close", "chg_pct", "amount",
                                "turnover"]].reset_index(drop=True)
            except WafBlocked:
                TX_BREAKER.mark_fail()  # 被 WAF 拦，后续跳过腾讯、直接走新浪
            except Exception:
                pass

        # 降级2：新浪日K（纯 HTTP，**不走 akshare** —— 那条路依赖
        # py_mini_racer(V8)，多线程会段错误）。三个源里它最稳：2026-10-19
        # 实测腾讯日K 被 WAF 返回 501 时，新浪这个接口仍然 200 正常。
        # 代价：只有 OHLCV，成交额按量×均价估算、换手率按流通股本反推
        # （与本文件腾讯分支同一套算法，相邻两日同法估算、比值误差可抵销）。
        try:
            r = http_get(self.session,
                         "https://money.finance.sina.com.cn/quotes_service/"
                         "api/json_v2.php/CN_MarketData.getKLineData",
                         {"symbol": sym, "scale": "240", "ma": "no",
                          "datalen": str(self.cfg.get("kline_limit", 130))},
                         retries=2, timeout=12)
            arr = r.json()
            if not arr:
                return None
            out = pd.DataFrame(arr).rename(columns={"day": "date"})
            for c in ["open", "close", "high", "low"]:
                out[c] = pd.to_numeric(out.get(c), errors="coerce")
            # 新浪 volume 单位是「股」，本项目内部统一用「手」
            out["volume"] = pd.to_numeric(out.get("volume"),
                                          errors="coerce") / 100.0
            out["chg_pct"] = out["close"].pct_change() * 100.0
            vwap = out[["open", "close", "high", "low"]].mean(axis=1)
            out["amount"] = out["volume"] * 100.0 * vwap
            shares = self._shares.get(str(code))
            out["turnover"] = (out["volume"] * 100.0 / shares * 100.0
                               if shares and shares > 0 else None)
            self.degraded_history = True
            _count_adj("none")        # 新浪不复权：MA 偏保守，见 KLINE_ADJ 注释
            return out[["date", "close", "chg_pct", "amount",
                        "turnover"]].reset_index(drop=True)
        except Exception:
            return None


def _f(x):
    try:
        v = float(x)
        return v if v == v else None
    except Exception:
        return None


def _parse_tx_lines(txt: str) -> list[dict]:
    """解析腾讯 qt.gtimg.cn 批量返回（`v_sh600354="..."` 以 ; 分隔）为原始记录。

    字段索引来自 2026-10-19 实测（单只共 88 个 `~` 分隔字段）：
        [1] 名称  [2] 代码  [3] 现价  [4] 昨收  [5] 今开(=集合竞价成交价)
        [6] 累计成交量(手)  [32] 涨跌幅  [37] 累计成交额(万元)
        [38] 换手率  [44] 流通市值(亿)  [45] 总市值(亿)
    注意：9:25 之前 [5]/[6] 是虚拟撮合值，9:25 撮合完成后才是最终竞价结果。
    """
    rows = []
    for line in txt.split(";"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        body = line.split("=", 1)[1].strip().strip('"')
        f = body.split("~")
        if len(f) < 46:
            continue
        rows.append({
            "code": f[2], "name": f[1],
            "market": 1 if line.startswith("v_sh") else 0,
            "price": f[3], "prev_close": f[4], "open": f[5],
            "volume": f[6], "chg_pct": f[32], "turnover": f[38],
            "amount": (float(f[37]) * 1e4) if _f(f[37]) is not None else None,
            "float_mv": (float(f[44]) * 1e8) if _f(f[44]) is not None else None,
            "total_mv": (float(f[45]) * 1e8) if _f(f[45]) is not None else None,
        })
    return rows


def _parse_sina_lines(txt: str) -> list[dict]:
    """解析新浪快照 hq.sinajs.cn/list= 的批量返回。

    格式：`var hq_str_sh600354="敦煌种业,8.310,8.380,...,2026-10-19,15:00:00,00";`
    字段索引来自 2026-10-19 实测（单只 34 个 `,` 分隔字段）：
        [0] 名称   [1] 今开(=集合竞价成交价)   [2] 昨收   [3] 现价
        [4] 最高   [5] 最低   [6] 买一价   [7] 卖一价
        [8] 累计成交量(股)   [9] 累计成交额(元)

    与腾讯的差异（必须知道，否则会误用）：
      · 成交量单位是「股」，腾讯是「手」—— 这里除以 100 对齐
      · **没有市值、换手率、涨跌幅**：涨跌幅按 现价/昨收 自算；
        市值与换手率由调用方用预筛池的 `shares` 补齐（见 collector._backfill_static）
      · 停牌/无数据的股票返回空串，直接跳过
    """
    rows = []
    for line in txt.split(";"):
        line = line.strip()
        if not line or "=" not in line or '"' not in line:
            continue
        head, body = line.split("=", 1)
        sym = head.strip().replace("var hq_str_", "").strip()
        if len(sym) < 8:
            continue
        f = body.strip().strip(';"').split(",")
        if len(f) < 10 or not f[0]:
            continue
        price, prev_close = _f(f[3]), _f(f[2])
        # 停牌/未开盘时现价为 0，退用今开，再退用昨收
        if not price:
            price = _f(f[1]) or prev_close
        rows.append({
            "code": sym[2:], "name": f[0],
            "market": 1 if sym.startswith("sh") else 0,
            "price": price, "prev_close": prev_close, "open": _f(f[1]),
            "volume": (_f(f[8]) / 100.0) if _f(f[8]) is not None else None,
            "amount": _f(f[9]),
            "chg_pct": ((price / prev_close - 1.0) * 100.0
                        if price and prev_close else None),
            "turnover": None, "float_mv": None, "total_mv": None,
        })
    return rows


# ---------------------------------------------------------------------------
# 腾讯分时 —— 盘后还原集合竞价（条件3 的唯一事后通道）
# ---------------------------------------------------------------------------
MINUTE_QUERY = "https://web.ifzq.gtimg.cn/appstock/app/minute/query"


def minute_auction(code: str, market: int = 1, session=None,
                   retries: int = 1) -> dict | None:
    """用腾讯分时的第一根（09:30）还原集合竞价结果。

    【为什么这能当「盘后补数」】
    2026-10-19 实测 600354：分时第一根 `0930 8.31 4905 4076055.00`，
    与当天真实结果的 open=8.31 / 竞价量=4905 手 / 竞价额=4076055 元
    **逐位一致**。腾讯把 09:15–09:25 的竞价成交全部归到 09:30 这一根
    （09:31 起是含连续竞价的累计量），所以第一根 = 纯竞价结果。

    【只能补条件3，补不了条件1/2】
    分时从 09:30 才开始，给不了 9:20 的累计额和 9:24 的价。
    那两个时点仍然只能在当时抓 —— 这是交易机制决定的，不是数据源问题。

    返回 None 表示这只拿不到（停牌 / 当天无分时 / 第一根不是 09:30）。
    """
    sym = ("sh" if int(market) == 1 else "sz") + str(code).zfill(6)
    session = session or make_session()
    try:
        js = http_json(session, MINUTE_QUERY, {"code": sym},
                       retries=retries, timeout=10,
                       headers={"Referer": "https://gu.qq.com/"})
    except Exception:
        return None
    node = (js.get("data") or {}).get(sym) or {}
    arr = ((node.get("data") or {}).get("data")) or []
    if not arr:
        return None
    f = str(arr[0]).split()
    # 第一根必须是 0930。不是就说明数据异常（停牌补位、跨日等），宁可不采用
    if len(f) < 4 or f[0] != "0930":
        return None
    price, vol, amt = _f(f[1]), _f(f[2]), _f(f[3])
    if not price or price <= 0:
        return None
    return {"code": str(code).zfill(6), "market": int(market),
            "price": price, "volume": vol, "amount": amt}


def minute_auction_batch(codes, session=None, gap: float = 0.25,
                         retries: int = 1, on_progress=None) -> pd.DataFrame:
    """批量还原集合竞价。codes: [(code, market), ...] 或 [code, ...]。

    逐只请求 —— 分时接口不支持多代码（2026-10-19 实测传逗号列表返回空）。
    盘后补数没有时间压力，gap 默认 0.25s 已足够低；任一失败不影响其余。
    返回的框只有 code/market/price/volume/amount，
    其余字段（prev_close/name/市值）由调用方用预筛池补。
    """
    session = session or make_session()
    rows, n_fail = [], 0
    total = len(codes)
    for i, it in enumerate(codes, 1):
        if isinstance(it, (tuple, list)):
            code, mkt = str(it[0]).zfill(6), int(it[1])
        else:
            code = str(it).zfill(6)
            mkt = 1 if code.startswith(("6", "9")) else 0
        r = minute_auction(code, mkt, session=session, retries=retries)
        if r:
            rows.append(r)
        else:
            n_fail += 1
        if on_progress:
            on_progress(i, total, len(rows), n_fail)
        if gap and i < total:
            time.sleep(gap)
    if not rows:
        return pd.DataFrame()
    return _finalize_spot(pd.DataFrame(rows))


def _finalize_spot(df: pd.DataFrame) -> pd.DataFrame:
    """统一清洗：类型转换、去重、建索引。"""
    if "code" not in df.columns:
        df["code"] = None
    for c in ["name", "market"]:
        if c not in df.columns:
            df[c] = None
    for c in ["price", "chg_pct", "volume", "amount", "turnover",
              "open", "prev_close", "total_mv", "float_mv"]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df = df.dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="first")
    return df.set_index("code", drop=False)


# ---------------------------------------------------------------------------
def build_source(cfg: dict, prefer: str = "auto") -> DataSource:
    """prefer: auto / eastmoney / tencent"""
    if prefer == "eastmoney":
        return EastMoneySource(cfg)
    if prefer == "tencent":
        return TencentSource(cfg)
    # auto：先试东财，失败降级腾讯
    em = EastMoneySource(cfg)
    try:
        probe = http_json_hosts(em.session, EM_CLIST_HOSTS, "/api/qt/clist/get",
                                {"pn": 1, "pz": 5, "po": 1, "np": 1,
                                 "fltt": 2, "invt": 2, "fid": "f12",
                                 "fs": "m:1+t:2", "fields": "f12"},
                                retries=6, timeout=10)
        if (probe.get("data") or {}).get("diff"):
            return em
    except Exception:
        pass
    print("    [提示] 东方财富接口不可达，自动切换到「腾讯+新浪」备用源")
    return TencentSource(cfg)
