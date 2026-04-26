"""
run_bot.py — AQT_v1 機器人入口
環境變數 BOT_VERSION:
  grid   → 動態網格機器人（預設）
  v4     → 舊版多策略量化機器人
"""
import os
from dotenv import load_dotenv
load_dotenv()

def main():
    version = os.getenv("BOT_VERSION", "grid").strip().lower()
    if version == "v4":
        print("[Runner] v4 多策略量化機器人")
        from crypto_main_v4 import run_bot
    else:
        print("[Runner] 動態網格機器人")
        from crypto_main_grid import run_grid_bot as run_bot
    run_bot()

if __name__ == "__main__":
    main()
