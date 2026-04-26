#!/usr/bin/env python3
"""
scripts/save_to_keyring.py — 將 API 金鑰存入系統 Keyring（⑤ Phase G）

支援平台：
  - macOS：Keychain（最高安全等級，TouchID/FaceID 保護）
  - Linux：Secret Service（gnome-keyring / KWallet）
  - Windows：Credential Manager

安裝依賴：
  pip install keyring

使用方式：
  python scripts/save_to_keyring.py

儲存後啟動機器人：
  from quant_system.utils.secrets_loader import load_from_keyring
  load_from_keyring()   # 在 run_bot.py 最頂端呼叫
"""
import getpass
import sys
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)


def main() -> None:
    print("=" * 60)
    print("  AQT_v1 — API 金鑰 Keyring 儲存工具")
    print("=" * 60)

    try:
        import keyring
    except ImportError:
        print("\n❌ 缺少依賴：keyring")
        print("   請執行：pip install keyring")
        sys.exit(1)

    SERVICE = "aqt_v1"

    print(f"\n此工具將 Binance API 金鑰儲存至系統 Keyring（服務名：{SERVICE}）")
    print("儲存後金鑰受系統加密保護，不再需要 .env 檔案中的明文金鑰。\n")

    api_key = getpass.getpass("🔑 Binance API Key（不顯示）：").strip()
    if not api_key:
        print("❌ API Key 不能為空")
        sys.exit(1)

    api_secret = getpass.getpass("🔑 Binance API Secret（不顯示）：").strip()
    if not api_secret:
        print("❌ API Secret 不能為空")
        sys.exit(1)

    keyring.set_password(SERVICE, "BINANCE_API_KEY",    api_key)
    keyring.set_password(SERVICE, "BINANCE_API_SECRET", api_secret)

    print("\n✅ API 金鑰已安全儲存至系統 Keyring")
    print(f"   服務：{SERVICE}")
    print()
    print("🚀 在 run_bot.py 最頂端加入以下代碼來使用：")
    print()
    print("   from quant_system.utils.secrets_loader import load_from_keyring")
    print("   load_from_keyring()")
    print()
    print("   之後可從 .env 移除 BINANCE_API_KEY 和 BINANCE_API_SECRET")


if __name__ == "__main__":
    main()
