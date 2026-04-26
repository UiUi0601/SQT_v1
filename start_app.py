"""
start_app.py — AQT_v1 啟動腳本（優雅關閉強化版）

用法：
  python start_app.py               # 背景啟動機器人 + 背景啟動 Web UI
  python start_app.py start-bot     # 只啟動機器人（背景）
  python start_app.py stop-bot      # 優雅停止機器人
  python start_app.py restart-bot   # 重啟機器人（stop + start）
  python start_app.py start-ui      # 只啟動 Web UI（背景）
  python start_app.py stop-ui       # 優雅停止 Web UI
  python start_app.py stop-all      # 停止機器人與 Web UI
  python start_app.py status        # 查看機器人與 Web UI 狀態

③ 優雅關閉流程（stop-bot / stop-ui）：
  1. 發送 SIGTERM → 主迴圈設 _is_running=False，跑完當前 tick 後退出
  2. 等待最多 GRACEFUL_TIMEOUT 秒讓進程自行退出
  3. 若超時，改發 SIGKILL 強制終止（保底措施）
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# 強制將 stdout 與 stderr 轉為 utf-8 輸出，解決 Windows 終端機預設編碼的 UnicodeEncodeError
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

ROOT_DIR         = Path(__file__).resolve().parent
RUNTIME_DIR      = ROOT_DIR / "runtime"
LOG_DIR          = ROOT_DIR / "logs"
PID_FILE         = RUNTIME_DIR / "trading_bot.pid"
UI_PID_FILE      = RUNTIME_DIR / "web_ui.pid"
BOT_LOG          = LOG_DIR / "trading_bot.log"
UI_LOG           = LOG_DIR / "web_ui.log"

# ③ 優雅關閉等待秒數（讓 DB flush 完成）
GRACEFUL_TIMEOUT = int(os.getenv("GRACEFUL_TIMEOUT", "45"))
POLL_INTERVAL    = 0.5   # 輪詢進程是否已退出的間隔秒數

RUN_ENV = os.environ.copy()
RUN_ENV["PYTHONUTF8"] = "1"
RUN_ENV["PYTHONIOENCODING"] = "utf-8"


# ════════════════════════════════════════════════════════════
# 工具函數
# ════════════════════════════════════════════════════════════
def _is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pid(file_path: Path = None) -> int | None:
    path = file_path or PID_FILE
    if not path.exists():
        return None
    txt = path.read_text().strip()
    return int(txt) if txt.isdigit() else None


def _write_pid(pid: int, file_path: Path = None) -> None:
    path = file_path or PID_FILE
    RUNTIME_DIR.mkdir(exist_ok=True)
    path.write_text(str(pid))


def _wait_for_exit(pid: int, timeout: float) -> bool:
    """等待 pid 退出，回傳 True = 已退出，False = 超時仍在執行。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _is_running(pid):
            return True
        time.sleep(POLL_INTERVAL)
    return False


# ════════════════════════════════════════════════════════════
# 指令
# ════════════════════════════════════════════════════════════
def start_bot() -> None:
    RUNTIME_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

    pid = _read_pid()
    if pid and _is_running(pid):
        print(f"[Bot] 已在執行中，PID={pid}")
        return

    with BOT_LOG.open("a", encoding="utf-8") as lf:
        if os.name == "nt":
            proc = subprocess.Popen(
                [sys.executable, "run_bot.py"],
                cwd          = ROOT_DIR,
                stdout       = lf,
                stderr       = subprocess.STDOUT,
                env          = RUN_ENV,
                creationflags = (
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    | subprocess.DETACHED_PROCESS
                ),
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, "run_bot.py"],
                cwd         = ROOT_DIR,
                stdout      = lf,
                stderr      = subprocess.STDOUT,
                env         = RUN_ENV,
                preexec_fn  = os.setsid,   # 新 session，避免 Ctrl+C 連帶終止
            )

    _write_pid(proc.pid)
    print(f"[Bot] 量化機器人已啟動（背景），PID={proc.pid}")
    print(f"[Bot] 日誌：{BOT_LOG}")


def stop_bot(force: bool = False) -> bool:
    """
    ③ 優雅停止機器人。

    1. SIGTERM → 等待 GRACEFUL_TIMEOUT 秒讓進程自行退出
    2. 若超時且 force=True（或系統不支援優雅關閉），改送 SIGKILL
    回傳 True = 成功停止；False = 未能停止。
    """
    pid = _read_pid()
    if not pid:
        print("[Bot] 找不到 PID 檔案，機器人可能未啟動")
        return True
    if not _is_running(pid):
        print(f"[Bot] PID {pid} 已停止")
        PID_FILE.unlink(missing_ok=True)
        return True

    print(f"[Bot] 發送 SIGTERM 至 PID={pid}，等待優雅退出（最多 {GRACEFUL_TIMEOUT}s）...")
    try:
        if os.name == "nt":
            # Windows：直接 SIGTERM（等同 TerminateProcess，無真正優雅關閉）
            os.kill(pid, signal.SIGTERM)
        else:
            # Unix：發給整個 process group（包含子進程）
            os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        print(f"[Bot] PID {pid} 不存在（可能已自行退出）")
        PID_FILE.unlink(missing_ok=True)
        return True
    except PermissionError as e:
        print(f"[Bot] 無權限發送訊號: {e}")
        return False

    # 等待進程自行退出
    exited = _wait_for_exit(pid, timeout=GRACEFUL_TIMEOUT)
    if exited:
        print(f"[Bot] ✅ 已優雅退出（PID={pid}）")
        PID_FILE.unlink(missing_ok=True)
        return True

    # 超時：強制終止
    if force or os.name == "nt":
        print(f"[Bot] ⚠ 超時（{GRACEFUL_TIMEOUT}s），改用 SIGKILL 強制終止...")
        try:
            if os.name == "nt":
                os.kill(pid, signal.SIGTERM)   # Windows 只有 SIGTERM
            else:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            # 等待 SIGKILL 生效
            if _wait_for_exit(pid, timeout=5.0):
                print(f"[Bot] 已強制終止，PID={pid}")
                PID_FILE.unlink(missing_ok=True)
                return True
        except Exception as e:
            print(f"[Bot] SIGKILL 失敗: {e}")
        return False
    else:
        print(
            f"[Bot] ⚠ 進程 {pid} 在 {GRACEFUL_TIMEOUT}s 內未退出。\n"
            f"       如需強制終止，請執行：python start_app.py stop-bot --force"
        )
        return False


def restart_bot() -> None:
    """停止後重新啟動機器人。"""
    print("[Bot] 重啟機器人...")
    stop_bot(force=True)
    time.sleep(1.0)
    start_bot()


def start_ui() -> int:
    pid = _read_pid(UI_PID_FILE)
    if pid and _is_running(pid):
        print(f"[UI] 已在執行中，PID={pid}")
        return 0

    print("[UI] 啟動 → http://localhost:8000")
    with UI_LOG.open("a", encoding="utf-8") as lf:
        if os.name == "nt":
            proc = subprocess.Popen(
                [sys.executable, "web_ui.py"],
                cwd=ROOT_DIR,
                stdout=lf,
                stderr=subprocess.STDOUT,
                env=RUN_ENV,
                creationflags=(
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    | subprocess.DETACHED_PROCESS
                ),
            )
        else:
            proc = subprocess.Popen(
                [sys.executable, "web_ui.py"],
                cwd=ROOT_DIR,
                stdout=lf,
                stderr=subprocess.STDOUT,
                env=RUN_ENV,
                preexec_fn=os.setsid,
            )

    _write_pid(proc.pid, UI_PID_FILE)
    print(f"[UI] Web UI 已啟動（背景），PID={proc.pid}")
    print(f"[UI] 日誌：{UI_LOG}")
    return 0


def stop_ui(force: bool = False) -> bool:
    """優雅停止 Web UI。"""
    pid = _read_pid(UI_PID_FILE)
    if not pid:
        print("[UI] 找不到 PID 檔案，Web UI 可能未啟動")
        return True
    if not _is_running(pid):
        print(f"[UI] PID {pid} 已停止")
        UI_PID_FILE.unlink(missing_ok=True)
        return True

    print(f"[UI] 發送 SIGTERM 至 PID={pid}，等待優雅退出（最多 {GRACEFUL_TIMEOUT}s）...")
    try:
        if os.name == "nt":
            os.kill(pid, signal.SIGTERM)
        else:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
    except ProcessLookupError:
        print(f"[UI] PID {pid} 不存在（可能已自行退出）")
        UI_PID_FILE.unlink(missing_ok=True)
        return True
    except PermissionError as e:
        print(f"[UI] 無權限發送訊號: {e}")
        return False

    exited = _wait_for_exit(pid, timeout=GRACEFUL_TIMEOUT)
    if exited:
        print(f"[UI] ✅ 已優雅退出（PID={pid}）")
        UI_PID_FILE.unlink(missing_ok=True)
        return True

    if force or os.name == "nt":
        print(f"[UI] ⚠ 超時（{GRACEFUL_TIMEOUT}s），改用 SIGKILL 強制終止...")
        try:
            if os.name == "nt":
                os.kill(pid, signal.SIGTERM)
            else:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            if _wait_for_exit(pid, timeout=5.0):
                print(f"[UI] 已強制終止，PID={pid}")
                UI_PID_FILE.unlink(missing_ok=True)
                return True
        except Exception as e:
            print(f"[UI] SIGKILL 失敗: {e}")
        return False
    else:
        print(f"[UI] ⚠ 進程 {pid} 未退出。如需強制終止請加上 --force")
        return False


def show_status() -> None:
    bot_pid = _read_pid(PID_FILE)
    if not bot_pid:
        print("[Bot] 未啟動")
    else:
        running = _is_running(bot_pid)
        icon    = "✅ 執行中" if running else "❌ 已停止"
        print(f"[Bot] {icon}，PID={bot_pid}")
        if running:
            print(f"[Bot] 日誌：{BOT_LOG}")

    ui_pid = _read_pid(UI_PID_FILE)
    if not ui_pid:
        print("[UI] 未啟動")
    else:
        running = _is_running(ui_pid)
        icon    = "✅ 執行中" if running else "❌ 已停止"
        print(f"[UI] {icon}，PID={ui_pid}")


# ════════════════════════════════════════════════════════════
# 主入口
# ════════════════════════════════════════════════════════════
def main() -> int:
    args = sys.argv[1:]
    cmd  = args[0].strip().lower() if args else "start-all"
    opts = set(args[1:])
    force = "--force" in opts

    if cmd == "start-all":
        # 檢查並啟動 Bot
        bot_pid = _read_pid(PID_FILE)
        if bot_pid and _is_running(bot_pid):
            print(f"[Bot] 已在執行中，PID={bot_pid}")
        else:
            start_bot()
            
        # 檢查並啟動 UI
        ui_pid = _read_pid(UI_PID_FILE)
        if ui_pid and _is_running(ui_pid):
            print(f"[UI] 已在執行中，PID={ui_pid}")
        else:
            start_ui()
        return 0
    elif cmd == "start-bot":
        start_bot()
        return 0
    elif cmd == "stop-bot":
        return 0 if stop_bot(force=force) else 1
    elif cmd == "restart-bot":
        restart_bot()
        return 0
    elif cmd == "start-ui":
        return start_ui()
    elif cmd == "stop-ui":
        return 0 if stop_ui(force=force) else 1
    elif cmd == "stop-all":
        bot_stopped = stop_bot(force=force)
        ui_stopped = stop_ui(force=force)
        return 0 if (bot_stopped and ui_stopped) else 1
    elif cmd == "status":
        show_status()
        return 0
    else:
        print(__doc__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
