#!/usr/bin/env python3
"""
scripts/encrypt_env.py — 加密 .env → .env.enc（⑤ Phase G）

功能：
  讀取明文 .env，以 Master Password 加密為 .env.enc。
  加密後可安全提交至 Git（密文），明文 .env 應從版控排除。

加密演算法：
  PBKDF2-HMAC-SHA256（480,000 次迭代）衍生 256-bit 金鑰
  → Fernet（AES-128-CBC + HMAC-SHA256）對稱加密

使用方式：
  cd C:\\Users\\hp\\Desktop\\AQT_v1
  python scripts/encrypt_env.py

互動式流程：
  1. 輸入 .env 路徑（預設 .env）
  2. 輸入 Master Password（不顯示）
  3. 確認 Master Password
  4. 輸出 .env.enc

解密（啟動機器人時）：
  方式 A：AQT_MASTER_PASSWORD=my_pwd python run_bot.py
  方式 B：python -c "from quant_system.utils.secrets_loader import load_encrypted_env; load_encrypted_env()"

安全建議：
  - Master Password 至少 16 字元，包含大小寫字母、數字、符號
  - 不要將 Master Password 寫入任何檔案或 Git
  - 使用 Docker Secret / GitHub Secret / AWS SSM Parameter Store 傳遞
  - 定期（每 90 天）更換 Master Password 並重新加密
"""
import getpass
import os
import sys

# 確保可以找到 quant_system 模組
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)


def main() -> None:
    print("=" * 60)
    print("  AQT_v1 — 環境變數加密工具（.env → .env.enc）")
    print("=" * 60)

    # ── 確認依賴 ──────────────────────────────────────────────
    try:
        import cryptography  # noqa: F401
    except ImportError:
        print("\n❌ 缺少依賴：cryptography")
        print("   請執行：pip install cryptography")
        sys.exit(1)

    from quant_system.utils.secrets_loader import _encrypt_text

    # ── 輸入路徑 ──────────────────────────────────────────────
    default_env = os.path.join(_ROOT, ".env")
    env_path_input = input(f"\n.env 路徑 [{default_env}]：").strip()
    env_path = env_path_input or default_env

    if not os.path.exists(env_path):
        print(f"\n❌ 找不到 {env_path}")
        sys.exit(1)

    # ── 讀取 .env ─────────────────────────────────────────────
    with open(env_path, encoding="utf-8") as f:
        env_text = f.read()

    # 統計有效鍵數量（排除注釋和空行）
    key_count = sum(
        1 for line in env_text.splitlines()
        if line.strip() and not line.strip().startswith("#") and "=" in line
    )
    print(f"\n📄 讀取 {env_path}：{key_count} 個有效環境變數")

    # ⚠ 安全警告：顯示將被加密的敏感變數名（不顯示值）
    print("\n將加密的環境變數（僅顯示 KEY 名，不顯示值）：")
    for line in env_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key = line.split("=")[0].strip()
            is_sensitive = any(
                s in key.upper()
                for s in ["KEY", "SECRET", "TOKEN", "PASSWORD", "PASS"]
            )
            marker = "🔐" if is_sensitive else "  "
            print(f"  {marker} {key}")

    # ── 輸入 Master Password ──────────────────────────────────
    print()
    while True:
        pwd = getpass.getpass("🔑 輸入 Master Password（不顯示，至少 12 字元）：")
        if len(pwd) < 12:
            print("   ❌ 密碼至少 12 字元，請重試")
            continue
        pwd2 = getpass.getpass("🔑 再次確認 Master Password：")
        if pwd != pwd2:
            print("   ❌ 兩次輸入不一致，請重試")
            continue
        break

    # ── 加密 ─────────────────────────────────────────────────
    print("\n⏳ 加密中（PBKDF2 × 480,000 次迭代，請稍候）...")
    encrypted = _encrypt_text(env_text, pwd)

    # ── 輸出 .env.enc ────────────────────────────────────────
    out_path = env_path + ".enc"
    with open(out_path, "wb") as f:
        f.write(encrypted)

    size_kb = len(encrypted) / 1024
    print(f"\n✅ 加密完成！")
    print(f"   輸出：{out_path}  ({size_kb:.1f} KB)")
    print()
    print("📌 後續步驟：")
    print(f"   1. 將 {out_path} 提交至 Git（密文，安全）")
    print(f"   2. 確認 .env 已在 .gitignore（不提交明文）")
    print()
    print("🚀 啟動機器人時的解密方式：")
    print()
    print("   方式 A（環境變數，推薦 CI/CD）：")
    print("     AQT_MASTER_PASSWORD=your_pwd python run_bot.py")
    print()
    print("   方式 B（互動式輸入）：")
    print("     在 run_bot.py 最頂端加入：")
    print("       from quant_system.utils.secrets_loader import load_encrypted_env")
    print("       load_encrypted_env()  # 提示輸入密碼後啟動")
    print()
    print("   方式 C（Docker）：")
    print("     docker run --env AQT_MASTER_PASSWORD=... aqt_v1:latest")
    print()

    # ── 驗證：嘗試解密確認加密正確 ───────────────────────────
    print("🔍 驗證加密完整性...")
    from quant_system.utils.secrets_loader import _decrypt_file
    from pathlib import Path
    try:
        decrypted = _decrypt_file(Path(out_path), pwd)
        if decrypted == env_text:
            print("   ✅ 驗證通過，加密資料完整")
        else:
            print("   ⚠️  驗證警告：解密內容與原始不完全一致")
    except Exception as e:
        print(f"   ❌ 驗證失敗：{e}")


if __name__ == "__main__":
    main()
