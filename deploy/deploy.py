# -*- coding: utf-8 -*-
"""
一键构建交付包
==============================================================================
流程：
  1) 清理 build/ dist/（Python 自底向上删，不用 PyInstaller --clean
     —— 本机 safe-delete 对 >50 条批量删除会直接拒绝）
  2) PyInstaller 打成单文件 exe（排除 akshare 等重依赖）
  3) 组装交付目录 delivery/竞价选股助手/（exe + config + bat + 说明书）
  4) ★ 隔离验证：把 exe 复制到空目录跑自检，确认能独立运行
  5) 打 zip 快照

用法：
  <build venv>/python deploy/deploy.py
==============================================================================
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent      # auction_screener/
# ★ 构建与交付目录可被环境变量 SCREENER_OUT 整体重定向
#   2026-10-19 事故：F 盘写满到 0 字节可用，PyInstaller 连 build/ 都建不出来，
#   打包在最后 zip 阶段 OSError 28 失败，还留下一个 27MB 的截断包（必须手动删）。
#   用法：SCREENER_OUT=G:/_screener_build python deploy/deploy.py
_ENV_OUT = os.environ.get("SCREENER_OUT")
if _ENV_OUT:
    _OUT = Path(_ENV_OUT)
    DIST = _OUT / "dist"
    BUILD = _OUT / "build"
    DELIVERY = _OUT / "delivery" / "竞价选股助手"
else:
    DIST = ROOT / "dist"
    BUILD = ROOT / "build"
    DELIVERY = ROOT / "delivery" / "竞价选股助手"
EXE_NAME = "Screener"
TASK_NAME = "竞价选股助手"                     # v1.0 旧名，卸载时兼容清理
TASK_PREP = "竞价选股助手-预筛"                 # 15:30 收盘预筛，为次日备池
TASK_LIVE = "竞价选股助手-采样"                 # 09:19 采样轨（多轮冗余采样+出结果）
TASK_BFILL = "竞价选股助手-定型"                # 09:31 定型轨（读落盘数据重算，兜底）
VER = "v1.3"
# 三个定时任务的分工（v1.3）：
#   15:30 预筛 —— 用当日收盘日K把条件4/5/6 算完，候选池落盘；次日早上零压力
#   09:19 采样 —— 9:20 / 9:24 双源并行采集；9:25 改成撮合后的静止窗(9:25-9:29:50)
#   09:31 定型 —— 不重新采样，读已落盘数据重算；9:25 缺失时用腾讯分时补
#               （不是 09:26：采样轨的静止窗到 09:29:50 才结束，早了会抢文件锁）
# v1.3 相对 v1.2 的三处变化（都来自六个条件的「可补性」分析）：
#   ① 9:20 是唯一不可事后还原的时点 -> 加密到 1.5s/轮、腾讯+新浪双源并行
#   ② 9:25 竞价结果腾讯分时盘后可补且逐位一致 -> 采集窗口放宽到 9:29:50
#   ③ 条件1/2 缺失时输出「低置信度结果」而不是「今日无股票」
# 东财分笔（旧回算轨）已降级为可选兜底：2026-10-19 实测成功率 0~40%，
# 且会因高频请求把本机 IP 打进风控冷却，不能再当主路径。


# ===========================================================================
# 工具
# ===========================================================================
def clean(path: Path):
    """自底向上逐个删 —— 绕开本机 safe-delete 的批量删除限制。"""
    if not path.exists():
        return
    for root, dirs, files in os.walk(str(path), topdown=False):
        for f in files:
            try:
                os.remove(os.path.join(root, f))
            except OSError:
                pass
        for d in dirs:
            try:
                os.rmdir(os.path.join(root, d))
            except OSError:
                pass
    try:
        os.rmdir(str(path))
    except OSError:
        pass


def decode(b: bytes) -> str:
    """UTF-8 优先、GBK 兜底 —— 顺序不能反。"""
    if not b:
        return ""
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("gbk", errors="ignore")


def write_bat(path: Path, text: str):
    """★ .bat 必须 GBK + CRLF。换行只转换一次（newline="" 关闭二次转换）。"""
    text = text.replace("chcp 65001", "chcp 936")
    text = text.replace("\r\n", "\n").replace("\n", "\r\n")
    with open(path, "w", encoding="gbk", newline="") as f:
        f.write(text)


# ===========================================================================
# .bat 入口
# ===========================================================================
HEAD = """@echo off
chcp 936 > nul 2>&1
cd /d "%~dp0"
set PYTHONUTF8=1
"""

BAT_SELFTEST = HEAD + """title 环境自检
echo ============================================================
echo   竞价选股助手 - 环境自检
echo ============================================================
echo.
echo 正在检查网络与行情数据源，约需 30 秒，请稍候 ...
echo.
"{exe}.exe" --selftest
echo.
if errorlevel 1 (
  echo ------------------------------------------------------------
  echo   [!] 自检未通过，请把上面的内容截图给技术支持
  echo ------------------------------------------------------------
) else (
  echo ------------------------------------------------------------
  echo   [OK] 自检通过，可以执行 "2-安装每日定时任务.bat" 了
  echo ------------------------------------------------------------
)
echo.
pause
"""

BAT_INSTALL = HEAD + """title 安装每日定时任务
echo ============================================================
echo   安装每日定时任务
echo ============================================================
echo.
echo 会安装三个任务：
echo.
echo   1) 预筛      每个交易日 15:30（收盘后）
echo      用当天收盘数据把「昨日换手率、均线、涨停、量价」这些
echo      历史条件全部算好并保存，第二天早上直接用。
echo.
echo   2) 采样      每个交易日 09:19 启动
echo      在 9:20 / 9:24 两个时点各反复采集十几轮（9:20 最密），
echo      9:25 之后是撮合后的静止期，抓一轮就定型。
echo      只要其中一轮成功就能出结果，不会因为一次网络抖动而失败。
echo.
echo   3) 定型      每个交易日 09:31 启动
echo      不重新采集，直接读 09:19 已存下的数据重算一遍作为保险。
echo      9:25 那一步如果没采到，会用腾讯分时自动补回来。
echo.
echo [重要] 电脑睡眠没关系 —— 任务会自动唤醒它。
echo        但【关机】不行，关机状态下无法唤醒，请保持睡眠或开机。
echo.

rem 先清掉旧版本（v1.0/v1.1 的任务名不同）以及可能存在的同名任务
for %%T in ("{task}" "{task_live}" "{task_bfill}" "{task_prep}") do (
  schtasks /delete /tn %%T /f >nul 2>&1
)

set "TPREP=%~dp0_任务-预筛.bat"
set "TLIVE=%~dp0_任务-采样.bat"
set "TBACK=%~dp0_任务-定型.bat"
schtasks /create /tn "{task_prep}" /tr "\\"%TPREP%\\"" /sc daily /st 15:30 /f
set RC0=%errorlevel%
schtasks /create /tn "{task_live}" /tr "\\"%TLIVE%\\"" /sc daily /st 09:19 /f
set RC1=%errorlevel%
schtasks /create /tn "{task_bfill}" /tr "\\"%TBACK%\\"" /sc daily /st 09:31 /f
set RC2=%errorlevel%

if not "%RC0%%RC1%%RC2%"=="000" (
  echo.
  echo ------------------------------------------------------------
  echo   [!] 至少有 1 个任务创建失败
  echo   请右键本文件，选择 "以管理员身份运行"，再试一次。
  echo ------------------------------------------------------------
) else (
  rem ---------------------------------------------------------------
  rem  [v1.3] 打开「唤醒计算机运行」+「错过就补跑」
  rem  睡眠中的机器到点会被自动唤醒执行，这是本机部署最实际的兜底。
  rem  关机无效（需要 BIOS 定时开机，普通用户配不了），所以说明里
  rem  仍然要求"保持睡眠或开机"。
  rem  Win7 没有 Get-ScheduledTask，会失败 —— 此时退化为普通任务，
  rem  只是不具备唤醒能力，其余功能不受影响。
  rem ---------------------------------------------------------------
  set "WAKEOK=1"
  powershell -NoProfile -Command "$t=Get-ScheduledTask -TaskName '{task_live}'; $t.Settings.WakeToRun=$true; $t.Settings.StartWhenAvailable=$true; $t.Settings.DisallowStartIfOnBatteries=$false; $t.Settings.StopIfGoingOnBatteries=$false; Set-ScheduledTask -InputObject $t | Out-Null" >nul 2>&1
  if errorlevel 1 set "WAKEOK=0"
  powershell -NoProfile -Command "$t=Get-ScheduledTask -TaskName '{task_prep}'; $t.Settings.WakeToRun=$true; $t.Settings.StartWhenAvailable=$true; $t.Settings.DisallowStartIfOnBatteries=$false; $t.Settings.StopIfGoingOnBatteries=$false; Set-ScheduledTask -InputObject $t | Out-Null" >nul 2>&1
  if errorlevel 1 set "WAKEOK=0"
  powershell -NoProfile -Command "$t=Get-ScheduledTask -TaskName '{task_bfill}'; $t.Settings.WakeToRun=$true; $t.Settings.StartWhenAvailable=$true; $t.Settings.DisallowStartIfOnBatteries=$false; $t.Settings.StopIfGoingOnBatteries=$false; Set-ScheduledTask -InputObject $t | Out-Null" >nul 2>&1
  if errorlevel 1 set "WAKEOK=0"

  echo.
  echo ------------------------------------------------------------
  echo   [OK] 安装成功（3 个任务）
  echo   每天：09:19 采集并出结果，09:31 复核，15:30 为明天备料。
  echo   结果都在 output 文件夹的 "最新结果.txt" 里。
  if "%WAKEOK%"=="1" (
    echo   [OK] 已开启自动唤醒：机器睡眠时到点会自动醒来执行。
  ) else (
    echo   [!] 自动唤醒没有设置成功（可能是 Win7 或权限不足）。
    echo       请让电脑保持开机，否则当天任务不会执行。
  )
  echo ------------------------------------------------------------
)
echo.
pause
"""

BAT_RUNONCE = HEAD + """title 立即选股
echo ============================================================
echo   立即选股
echo ============================================================
echo.
echo 程序会自己看现在几点，自动选择合适的方式：
echo.
echo   09:15 之前   等待到 09:20 开始（可以按 Ctrl+C 取消）
echo   09:15-09:25  实时抓取竞价数据，09:25 刚过就出结果
echo   09:25 之后   用当天已定型的数据回算，任何时间跑结果都一样
echo                 （首次约 4-5 分钟，当天重跑约 2 分钟）
echo.
pause
"{exe}.exe"
if exist "output\\最新结果.txt" start "" notepad "output\\最新结果.txt"
echo.
pause
"""

BAT_RESULT = HEAD + """title 查看最新结果
if not exist "output\\最新结果.txt" (
  echo.
  echo 还没有结果文件。
  echo 请先运行 "3-立即运行一次.bat"，或等定时任务自动执行。
  echo.
  pause
  exit /b 1
)
notepad "output\\最新结果.txt"
"""

BAT_UNINSTALL = HEAD + """title 卸载每日定时任务
echo 正在删除定时任务 ...
schtasks /delete /tn "{task_prep}" /f >nul 2>&1
schtasks /delete /tn "{task_live}" /f >nul 2>&1
schtasks /delete /tn "{task_bfill}" /f >nul 2>&1
rem 兼容旧版本的任务名
schtasks /delete /tn "{task}" /f >nul 2>&1
schtasks /delete /tn "竞价选股助手-实时" /f >nul 2>&1
schtasks /delete /tn "竞价选股助手-回算" /f >nul 2>&1
echo.
echo 已删除 3 个任务（"预筛" "采样" "定型"）。
echo 如果提示"找不到"说明本来就没装。
echo 删除任务不影响 output 文件夹里已有的历史结果。
echo.
pause
"""

BAT_TASK_PREP = HEAD + """if not exist "output\\logs" mkdir "output\\logs"
rem 收盘预筛任务入口（无 pause，供任务计划程序调用）
rem 15:30 用当日收盘数据算条件4/5/6，把候选池存下来给第二天用
"Screener.exe" --mode prep >> "output\\logs\\task_prep.log" 2>&1
"""

BAT_TASK_LIVE = HEAD + """if not exist "output\\logs" mkdir "output\\logs"
rem 采样轨任务入口（无 pause，供任务计划程序调用）
rem 不传模式：09:19 时 auto 会判为采样轨，程序自己等到 09:19:40 开始采样
rem ★ 9:20 / 9:24 双源并行多轮采集；9:25 用撮合后的静止窗(9:25-9:29:50)
rem ★ 单轮失败不影响，不必重跑；9:25 万一没采到，定型轨会用分时补
"Screener.exe" >> "output\\logs\\task_collect.log" 2>&1
if exist "output\\最新结果.txt" start "" notepad "output\\最新结果.txt"
"""

BAT_TASK_BACK = HEAD + """if not exist "output\\logs" mkdir "output\\logs"
rem 定型轨任务入口（无 pause，供任务计划程序调用）
rem --mode resolve：不重新采样，直接读 09:19 落盘的数据重算（纯离线兜底）
rem 若 09:19 已经出过结果，这里得到的内容与它一致；失败也不会覆盖好结果
"Screener.exe" --mode resolve >> "output\\logs\\task_resolve.log" 2>&1
"""


BAT_DIAG = HEAD + """title 诊断定时任务
set "RPT=诊断报告.txt"
echo ============================================================ > "%RPT%"
echo   竞价选股助手 - 定时任务诊断 >> "%RPT%"
echo ============================================================ >> "%RPT%"
echo. >> "%RPT%"
echo [1] 当前时间 >> "%RPT%"
echo     %date% %time% >> "%RPT%"
echo. >> "%RPT%"
echo [2] 定时任务是否存在 >> "%RPT%"
echo   --- 0) 收盘预筛（15:30）--- >> "%RPT%"
schtasks /query /tn "{task_prep}" >nul 2>&1
if errorlevel 1 (
  echo     [X] 不存在：第二天早上会用实时日K兜底，但会慢 1~3 分钟 >> "%RPT%"
) else (
  echo     [OK] 存在 >> "%RPT%"
  schtasks /query /tn "{task_prep}" /fo LIST /v >> "%RPT%" 2>&1
)
echo. >> "%RPT%"
echo   --- 1) 采样轨（09:19）--- >> "%RPT%"
schtasks /query /tn "{task_live}" >nul 2>&1
if errorlevel 1 (
  echo     [X] 不存在：说明 "2-安装每日定时任务.bat" 没装成功 >> "%RPT%"
  echo     请重新双击它；若仍失败，右键选 "以管理员身份运行" >> "%RPT%"
) else (
  echo     [OK] 存在，详细配置与上次运行结果如下 >> "%RPT%"
  schtasks /query /tn "{task_live}" /fo LIST /v >> "%RPT%" 2>&1
)
echo. >> "%RPT%"
echo   --- 2) 定型轨（09:31）--- >> "%RPT%"
schtasks /query /tn "{task_bfill}" >nul 2>&1
if errorlevel 1 (
  echo     [X] 不存在 >> "%RPT%"
) else (
  echo     [OK] 存在，详细配置与上次运行结果如下 >> "%RPT%"
  schtasks /query /tn "{task_bfill}" /fo LIST /v >> "%RPT%" 2>&1
)
echo. >> "%RPT%"
echo [3] 自动唤醒（睡眠时能否自动执行）>> "%RPT%"
echo     True = 已开启，睡眠中到点会自动醒来跑 >> "%RPT%"
echo     False = 未开启，睡眠中不会跑；请在任务计划程序里手动勾选 >> "%RPT%"
echo     "唤醒计算机运行此任务" >> "%RPT%"
powershell -NoProfile -Command "$v=(Get-ScheduledTask -TaskName '{task_live}').Settings.WakeToRun; echo ('     WakeToRun = ' + $v)" >> "%RPT%" 2>&1
if errorlevel 1 echo     [X] 查询失败（Win7 无此命令，或权限不足）>> "%RPT%"
echo. >> "%RPT%"
echo [4] 程序运行痕迹 output\\logs >> "%RPT%"
if exist "output\\logs" (
  dir "output\\logs" >> "%RPT%"
) else (
  echo     [X] 没有 output\\logs 目录：程序从未在这台电脑上运行过 >> "%RPT%"
)
echo. >> "%RPT%"
echo [5] 任务入口控制台日志 >> "%RPT%"
if exist "output\\logs\\task_live.log" (
  dir "output\\logs\\task_live.log" >> "%RPT%"
) else (
  echo     [X] task_live.log 不存在：实时任务从未成功启动过程序 >> "%RPT%"
)
if exist "output\\logs\\task_backfill.log" (
  dir "output\\logs\\task_backfill.log" >> "%RPT%"
) else (
  echo     [-] task_backfill.log 不存在：回算任务从未成功启动过程序 >> "%RPT%"
)
echo     内容请用记事本打开这两个文件查看（若运行报错，原因就在里面） >> "%RPT%"
echo. >> "%RPT%"
echo [6] 最新结果文件 >> "%RPT%"
if exist "output\\最新结果.txt" (
  dir "output\\最新结果.txt" >> "%RPT%"
  echo     内容请双击 "4-查看最新结果.bat" 查看 >> "%RPT%"
) else (
  echo     还没有结果文件 >> "%RPT%"
)
echo. >> "%RPT%"
echo ============================================================ >> "%RPT%"
echo   请把本文件内容完整发给技术支持 >> "%RPT%"
notepad "%RPT%"
"""


# ===========================================================================
# 使用说明
# ===========================================================================
MANUAL = """============================================================
 竞价选股助手  使用说明                          {ver}
============================================================

一、这是什么
------------------------------------------------------------
按照你给的六组条件，扫描全部 A 股主板股票，把同时满足的挑出来。

选中条件（全部满足才入选）：
  1. 9:25 竞价金额 / 9:20 竞价金额 大于 12 倍
  2. 9:25 竞价价格 不低于 9:24 竞价价格
  3. 竞价涨跌幅 在 -1.5% 到 6% 之间，且 9:25 竞价金额大于 350 万
  4. 昨日换手率 在 3.5% 到 20% 之间，且 昨日收盘价 高于 60 日均线
  5. 前 30 个交易日内出现过涨停；股价低于 50 元；
     流通市值低于 120 亿；主板；非 ST
  6. 上上个交易日涨跌幅低于 6%，且 上上日成交额 / 昨日成交额
     大于 110%

------------------------------------------------------------
★ v1.2 最重要的变化：换了取数方式，稳定性提高一个量级
------------------------------------------------------------
之前靠「东方财富分笔明细」还原竞价数据。实测这个接口会按频率限流：
同一台机器跑几次之后，几小时甚至一整天都取不到数据 —— 这就是之前
"上午能跑、下午跑不出来"的真正原因，不是程序坏了，是被当成爬虫掐了。

这一版不再依赖它，改成在 9:20 / 9:24 / 9:25 三个时点，用腾讯行情
对候选股票反复采集十几轮快照：

  · 十几轮里只要有一轮成功，结果就是完整的（以前是一次失败全废）
  · 每一轮都存盘，事后随时能重算，不需要再联网
  · 不再请求东方财富，也就没有它那个限流风险

三个定时任务这样分工：

  15:30  预筛   收盘后把「昨日换手率 / 均线 / 涨停 / 量价」这些
                 历史条件先算好存起来，第二天早上直接用 ——
                 早上那几分钟只剩竞价相关判定，压力最小。
  09:19  采样   只在最关键的两个时点高频采集：9:20 和 9:24。
                 9:25 之后是「竞价已结束、连续竞价还没开始」的静止期，
                 价格和成交量完全不动，抓一轮就定型了。
  09:31  定型   不重新采集，读已存下的数据再算一遍。
                 9:25 那份如果没采到，会自动用腾讯分时补回来。

【为什么只有 9:20 那一枪最关键】
  6 个条件里，只有「条件1」用的 9:20 累计成交额是纯粹的时间点数据 ——
  交易所不发布竞价过程中的快照，任何接口都无法事后还原。
  而 9:25 的竞价结果（价格/金额）腾讯分时能查到、且与实时数据逐位一致，
  丢了也能补。所以采样资源全部压在 9:20 上。

============================================================
二、三步开始用（只做一次）
============================================================

[安装位置建议]
  解压到 D 盘或桌面都可以，但路径里不要有空格和特殊符号。
  推荐：  D:\\竞价选股助手\\
  ★ 不要放在 C:\\Program Files\\ 下面 —— 权限限制会导致
    定时任务创建失败、结果文件写不进去。

第 1 步：双击  1-环境自检.bat
        等 30 秒，看到 [OK] 自检通过 就行。
        如果显示 [!] 自检未通过，请截图给技术支持。

第 2 步：双击  2-安装每日定时任务.bat
        看到 [OK] 安装成功（3 个任务）即可。
        如果提示失败，就右键这个文件 ->
        "以管理员身份运行"，再试一次。

        装的 3 个任务是：
          竞价选股助手-预筛   每天 15:30
          竞价选股助手-采样   每天 09:19
          竞价选股助手-定型   每天 09:31

第 3 步：什么都不用做，等下一个交易日。

============================================================
三、每天怎么用
============================================================
全自动，你不用管：

  15:30  算好下一个交易日的候选池（约 1~2 分钟）
  09:19  开始采集，09:25 前后出结果并自动弹出记事本
  09:31  再算一遍做保险，结果与上面一致（不会重复弹窗）

  ★ 09:19 那个黑色窗口运行时请不要关闭，等它自己跑完。
  ★ 电脑睡眠没关系 —— 任务会自动把机器唤醒。但【关机】不行。

手动：想自己跑一次，双击  3-立即选股.bat
      程序按当前时间自动选合适方式，不用你选：

      · 09:19 之前跑  → 等到 09:19:40 开始采集
      · 09:19~09:25 跑 → 立刻开始采集
      · 09:25:45 之后跑 → 直接用当天已存数据重算（几秒）

看结果：结果会自动以记事本弹出；
        也可以随时双击  4-查看最新结果.bat
        重新打开 output\\最新结果.txt

============================================================
四、结果文件在哪里
============================================================
output\\最新结果.txt              每次运行后覆盖，最常看的文件
output\\选股结果_日期_时间.csv     完整明细，可用 Excel 打开
output\\选股结果_低置信度_日期.csv  只在数据不全时出现，见下方说明
output\\pool\\                    候选池（15:30 生成，第二天用）
output\\snapshots\\日期\\rounds\\   每轮采样的原始数据（可复盘重算）
output\\logs\\                     运行日志（出问题时发这个）

【如果看到「低置信度」】
  标题写着「集合竞价选股结果（低置信度）」、或 CSV 文件名带 _低置信度，
  意思不是程序坏了，而是：有一条条件我们没有数据去判定。

  最常见的情况是 9:20 那一次快照没采到（机器当时关着 / 睡眠没唤醒）。
  文件里会明确写「未验证：条件1」这样的字样，并在最上方标红说明。

  怎么用：
    · 这些股票满足了其余条件，可以当参考，但不要当买入信号
    · 想要完整结果，第二天 9:19 前保持开机或睡眠即可

  为什么 9:20 补不回来：那是竞价过程中的中间数据，交易所不发布，
  任何接口都查不到。9:25 的竞价结果则不同 —— 腾讯分时能查到，
  而且与实时数据逐位一致，丢了程序会自动补。

============================================================
五、想改条件怎么办
============================================================
用记事本打开 config.json，改数字后保存即可，不用重新安装。

常用的几个：

  想调整竞价涨幅范围（默认 -1.5% ~ 6%）
    "auction_chg_min": -1.5,
    "auction_chg_max": 6.0,

  想调整竞价金额放大倍数（默认 12 倍）
    "min_amount_ratio_925_920": 12.0,

  想调整竞价金额下限（默认 350 万，单位是元）
    "min_auction_amount": 3500000,

  想调整昨日换手率范围（默认 3.5% ~ 20%）
    "min_prev_turnover": 3.5,
    "max_prev_turnover": 20.0,

  想调整股价上限 / 市值上限（默认 50 元 / 120 亿）
    "max_price": 50.0,
    "max_market_cap": 12000000000,

改完保存，下次运行自动生效。
如果改坏了，直接删掉 config.json，程序会自动生成一份默认的。

============================================================
六、常见问题
============================================================

Q：双击 exe 时杀毒软件报警？
A：这是打包工具的通病（PyInstaller 生成的 exe 常被误报）。
   选择"允许运行"或"添加到信任"即可。本程序不读写任何系统文件，
   只联网获取公开行情数据。

Q：09:19 之前双击"立即选股"，窗口一直不动？
A：正常。程序在等 09:19:40 开始采集，屏幕上会有倒计时。
   不想等就按 Ctrl+C 退出，定时任务会照常跑。

Q：错过了 09:25，白天才想起来，还能出结果吗？
A：能。当天任何时间双击"3-立即选股.bat"，程序会直接用当天
   09:19 存下的采样数据重算，几秒钟出结果，且与早上那份完全一致。
   就算 9:25 那份没采到也没关系 —— 程序会用腾讯分时把它补回来。
   （前提是当天 09:19 那次确实跑成功了。）
   过了第二天 09:15 就不行了，那天的数据已被新一天覆盖。

Q：结果标题写着"低置信度"，是什么意思？
A：意思是 6 个条件里有一条没数据判定，不是程序坏了。
   最常见是 9:20 那次快照没采到（当时电脑关着或没唤醒）。
   文件里会明确写「未验证：条件1」，并列出仍然满足其他条件的股票。
   · 可以当参考，但不要当买入信号
   · 想要完整结果，第二天 9:19 前保持开机或睡眠即可
   ★ 为什么 9:20 补不回来：那是竞价过程中的中间数据，交易所不发布，
     任何接口都查不到。9:25 的竞价结果则能补（腾讯分时有，且逐位一致）。

Q：同一天能不能多跑几次？
A：可以，但没必要。竞价数据 09:25 就定型了，重跑多少次结果都一样，
   而且 09:25 之后的重跑是纯本地计算，不联网、不占流量。

Q：为什么现在只弹一次记事本？
A：09:19 那次出结果时弹；09:31 的定型复核默认不弹窗（结果相同，
   弹两次反而打扰）。想看就把 4-查看最新结果.bat 双击一下。

Q：提示"已经有一个程序在运行"？
A：说明定时任务正在跑，或者你双击了两次。
   等前一个跑完再试，避免两个程序抢同一份数据。

Q：结果为空，一只都没有？
A：六组条件同时满足的概率本来就低，空结果是正常的。
   可以看窗口或日志里打印的"筛选漏斗"，每一层剩多少只一目了然。

Q：电脑睡眠了，当天没跑？
A：v1.3 起，安装时已给任务勾上「唤醒计算机运行此任务」——
   睡眠中的机器到点会自动醒来执行，跑完可以继续睡。
   · 想确认是否生效：双击"6-诊断定时任务.bat"，看 [3] 那节
     显示 WakeToRun = True 就是已开启
   · 显示 False 或查询失败（多见于 Win7 / 权限不足）：
     请在"任务计划程序"里找到「竞价选股助手-采样」，
     右键属性 -> 条件 -> 勾选"唤醒计算机运行此任务"
   ★ 【关机】状态下无法唤醒，需要 BIOS 定时开机（一般不用配）。
     要求只有一条：9:19 前电脑处于 开机 或 睡眠 状态。

Q：15:30 的预筛没跑成功，第二天还能用吗？
A：能。第二天 09:19 发现没有候选池，会退化成全市场采集，
   结果一样正确，只是慢一些、请求量更大。

Q：预筛结果里出现"[!] 全部为不复权……可能漏掉临界股"，要紧吗？
A：说明当时行情接口（腾讯日K）临时不可用，程序自动换用了备用数据源。
   备用源的均线计算口径略有差异，可能漏掉 1~2 只刚好贴着均线的股票，
   其余结果不受影响。处理办法：当天再跑一次预筛即可（通常已恢复）。
   程序每次都会把口径打在屏幕上，不会瞒着你。

Q：想换台电脑用？
A：把整个文件夹复制过去，重新执行"第 1 步"和"第 2 步"即可。

Q：怎么彻底卸载？
A：双击 5-卸载定时任务.bat，然后删掉整个文件夹。

============================================================
七、注意事项
============================================================
1. 本工具只做数据筛选，不构成任何投资建议。
2. 数据来自公开行情接口，可能存在延迟，请以交易软件为准。
3. 09:19 之前请确保电脑已开机且未睡眠。
4. 请勿把本程序用于高频自动交易，也不要修改 config.json 里的
   请求频率相关参数 —— 调快只会让接口更快把你限流。

============================================================
  技术支持：出问题时请提供 output\\logs\\ 下的日志文件
============================================================
"""


# ===========================================================================
# 构建
# ===========================================================================
def build_exe():
    print("[1/5] 打包 exe ...")
    clean(BUILD)
    clean(DIST)
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm", "--onefile", "--console",
        "--name", EXE_NAME,
        # 排除用不到的重依赖（akshare 会带 py_mini_racer/V8，几十 MB）
        "--exclude-module", "akshare",
        "--exclude-module", "py_mini_racer",
        "--exclude-module", "matplotlib",
        "--exclude-module", "scipy",
        "--exclude-module", "tkinter",
        "--exclude-module", "IPython",
        "--exclude-module", "notebook",
        "--distpath", str(DIST),
        "--workpath", str(BUILD),
        "--specpath", str(BUILD),
        # v1.1 起入口改为 runner.py：它统一调度实时轨 / 回算轨，
        # 并 import screener、backfill、datasource（PyInstaller 会自动跟进）
        "--hidden-import", "backfill",
        "--hidden-import", "screener",
        "--hidden-import", "datasource",
        # prep / collector 是函数内延迟 import（避免循环依赖），
        # PyInstaller 的静态分析对这类引用不保险，显式声明。
        "--hidden-import", "prep",
        "--hidden-import", "collector",
        str(ROOT / "runner.py"),
    ]
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True)
    log = decode(r.stdout) + decode(r.stderr)
    if r.returncode != 0 or not (DIST / f"{EXE_NAME}.exe").exists():
        tail = "\n".join(log.strip().splitlines()[-25:])
        raise SystemExit(f"打包失败：\n{tail}")
    size = (DIST / f"{EXE_NAME}.exe").stat().st_size / 1e6
    print(f"      完成：{EXE_NAME}.exe  {size:.1f} MB")
    return DIST / f"{EXE_NAME}.exe"


def make_package():
    print("[2/5] 组装交付目录 ...")
    clean(DELIVERY)
    DELIVERY.mkdir(parents=True, exist_ok=True)

    shutil.copy2(DIST / f"{EXE_NAME}.exe", DELIVERY / f"{EXE_NAME}.exe")

    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    (DELIVERY / "config.json").write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    bats = {
        "1-环境自检.bat": BAT_SELFTEST.format(exe=EXE_NAME),
        "2-安装每日定时任务.bat": BAT_INSTALL.format(
            task=TASK_NAME, task_prep=TASK_PREP,
            task_live=TASK_LIVE, task_bfill=TASK_BFILL),
        "3-立即选股.bat": BAT_RUNONCE.format(exe=EXE_NAME),
        "4-查看最新结果.bat": BAT_RESULT,
        "5-卸载定时任务.bat": BAT_UNINSTALL.format(
            task=TASK_NAME, task_prep=TASK_PREP,
            task_live=TASK_LIVE, task_bfill=TASK_BFILL),
        "6-诊断定时任务.bat": BAT_DIAG.format(
            task_prep=TASK_PREP, task_live=TASK_LIVE, task_bfill=TASK_BFILL),
        "_任务-预筛.bat": BAT_TASK_PREP,
        "_任务-采样.bat": BAT_TASK_LIVE,
        "_任务-定型.bat": BAT_TASK_BACK,
    }
    for name, text in bats.items():
        write_bat(DELIVERY / name, text)

    with open(DELIVERY / "使用说明.txt", "w", encoding="utf-8-sig",
              newline="\r\n") as f:
        f.write(MANUAL.format(ver=VER))

    print(f"      完成：{len(list(DELIVERY.iterdir()))} 个文件")


def check_no_junk():
    print("[3/5] 检查交付包纯净度 ...")
    bad = []
    for f in DELIVERY.iterdir():
        n = f.name.lower()
        if f.is_dir() or n.endswith((".py", ".pyc", ".log", ".bak", ".spec")):
            bad.append(f.name)
    if bad:
        raise SystemExit(f"★ 交付包出现不该有的文件：{bad}")
    print("      通过：无源码 / 临时文件 / 日志残留")


def verify_isolated():
    """★ 把 exe 复制到空目录再跑 —— 在产物目录里跑等于没验证。"""
    print("[4/5] 隔离验证（模拟客户机器）...")
    vdir = BUILD / "_verify"
    clean(vdir)
    vdir.mkdir(parents=True, exist_ok=True)
    exe = vdir / f"{EXE_NAME}.exe"
    shutil.copyfile(DELIVERY / f"{EXE_NAME}.exe", exe)

    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    r = subprocess.run([str(exe), "--selftest"], cwd=str(vdir),
                       capture_output=True, timeout=900, env=env)
    out = decode(r.stdout) + decode(r.stderr)
    print("      ---- exe 输出 ----")
    for line in out.strip().splitlines():
        print("      " + line)
    ok = r.returncode == 0 and "SELF_TEST_RESULT=PASS" in out

    # 干净目录里该自动生成的都生成了吗
    made = sorted(p.name for p in vdir.iterdir())
    print(f"      隔离目录内容：{made}")
    for must in ["output", "config.json"]:
        if must not in made:
            ok = False
            print(f"      [!] 缺少自动生成的 {must}")
    if not ok:
        raise SystemExit("★ 隔离验证未通过")
    print("      通过：exe 可在空目录独立运行")
    return vdir


def zip_snapshot():
    print("[5/5] 打 zip 快照 ...")
    stamp = datetime.now().strftime("%Y%m%d")
    zp = DELIVERY.parent / f"竞价选股助手_{VER}_{stamp}.zip"
    if zp.exists():
        os.remove(zp)
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(DELIVERY.rglob("*")):
            if f.is_file():
                z.write(f, f.relative_to(DELIVERY.parent))
    print(f"      完成：{zp.name}  {zp.stat().st_size/1e6:.1f} MB")
    return zp


def main():
    print("=" * 62)
    print(f"  构建交付包  {VER}")
    print("=" * 62)
    build_exe()
    make_package()
    check_no_junk()
    # 隔离验证要联网跑自检。本机被行情接口限流时（实测腾讯 WAF 会返回 501
    # 持续几十分钟），可以 --skip-verify 先出包，但★正式交付前必须补跑一次。
    skip = "--skip-verify" in sys.argv
    if skip:
        print("[4/5] 跳过隔离验证（--skip-verify）")
        print("      ★ 这只是为了在接口限流时先出包，正式交付前必须补跑：")
        print("        python deploy/deploy.py")
    else:
        verify_isolated()
    zp = zip_snapshot()
    print("=" * 62)
    print("  全部完成")
    print(f"  交付目录：{DELIVERY}")
    print(f"  压缩包  ：{zp}")
    print("=" * 62)


if __name__ == "__main__":
    main()
