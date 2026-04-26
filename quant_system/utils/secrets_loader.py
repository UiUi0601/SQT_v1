"""
quant_system/utils/secrets_loader.py — 機密管理強化（⑤ Phase G）

功能：
  在啟動時解密 .env.enc（加密的環境變數），注入至 os.environ，
  取代直接讀取明文 .env 檔案。

加密架構：
  ┌─────────────────────────────────────────────────────────┐
  │  明文 .env  →  encrypt_env.py  →  .env.enc （AES加密） │
  │                                                         │
  │  啟動時：                                               │
  │  輸入 Master Password → PBKDF2 衍生 Fernet 金鑰        │
  │                       → 解密 .env.enc                   │
  │                       → 注入 os.environ                 │
  └─────────────────────────────────────────────────────────┘

使用方式一：互動式（手動輸入密碼）
  python -c "
  from quant_system.utils.secrets_loader import load_encrypted_env
  load_encrypted_env()   # 提示輸入 Master Password
  "

使用方式二：CI/CD 管線（密碼由環境變數或 Docker Secret 傳入）
  AQT_MASTER_PASSWORD=my_secret python run_bot.py

使用方式三：系統 Keyring（macOS Keychain / Linux Secret Service）
  python -c "
  from quant_system.utils.secrets_loader import load_from_keyring
  load_from_keyring()
  "

依賴：
  pip install cryptography  （requirements.txt 已包含）

安全注意事項：
  - .env.enc 可以安全地提交至 Git（加密過的密文）
  - Master Password 絕對不能放入 .env.enc 本身
  - 建議使用 Docker Secret / GitHub Secret / AWS SSM 傳遞 Master Password
  - 不要在日誌或終端中顯示 Master Password
"""
from __future__ import annotations

import base64
import getpass
import io
import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── 常數 ────────────────────────────────────────────────────
_ENC_FILE    = ".env.enc"   # 加密檔案預設名稱
_SALT_SIZE   = 16           # PBKDF2 salt 長度（bytes）
_ITERATIONS  = 480_000      # PBKDF2 迭代次數（OWASP 2023）
_MAGIC       = b"AQTENV1"   # 檔案魔術頭（4 bytes），用於格式驗證


# ════════════════════════════════════════════════════════════
# 公開 API
# ════════════════════════════════════════════════════════════
def load_encrypted_env(
    enc_path:        Optional[str] = None,
    master_password: Optional[str] = None,
    override:        bool          = False,
) -> int:
    """
    ⑤ 解密 .env.enc 並注入至 os.environ。

    Args:
        enc_path        — 加密檔案路徑（預設：與 .env 同目錄的 .env.enc）
        master_password — 解密密碼（None = 從環境變數 AQT_MASTER_PASSWORD 讀取，
                          再找不到 = 互動式提示輸入）
        override        — True = 覆蓋已存在的環境變數；False = 跳過已設定的

    Returns:
        成功注入的環境變數數量

    Raises:
        FileNotFoundError — .env.enc 不存在
        ValueError        — 密碼錯誤或檔案格式損毀
        ImportError       — cryptography 套件未安裝
    """
    _require_cryptography()

    path = Path(enc_path or _find_enc_file())
    if not path.exists():
        raise FileNotFoundError(
            f"找不到加密環境變數檔案：{path}\n"
            f"請先執行：python scripts/encrypt_env.py"
        )

    pwd = master_password or _get_master_password()
    env_text = _decrypt_file(path, pwd)

    # 解析 .env 格式並注入
    count = 0
    for line in env_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")  # 移除引號
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = val
            count += 1

    log.info(f"[Secrets] ⑤ 已從 {path} 解密並注入 {count} 個環境變數")
    return count


def load_from_keyring(
    service:  str = "aqt_v1",
    username: str = "binance_api_key",
) -> bool:
    """
    ⑤ 從系統 Keyring 載入 API 金鑰。

    支援：
      - macOS Keychain
      - Linux Secret Service（需安裝 secretstorage）
      - Windows Credential Manager

    使用前請先儲存：
      python scripts/save_to_keyring.py

    Args:
        service  — Keyring 服務名稱（對應 AQT 應用）
        username — 憑證用戶名（用於查找 BINANCE_API_KEY）

    Returns:
        True = 成功載入；False = Keyring 不可用或金鑰不存在
    """
    try:
        import keyring  # type: ignore
    except ImportError:
        log.warning("[Secrets] keyring 套件未安裝，跳過 Keyring 載入")
        return False

    try:
        api_key    = keyring.get_password(service, "BINANCE_API_KEY")
        api_secret = keyring.get_password(service, "BINANCE_API_SECRET")

        if api_key:
            os.environ.setdefault("BINANCE_API_KEY", api_key)
        if api_secret:
            os.environ.setdefault("BINANCE_API_SECRET", api_secret)

        if api_key and api_secret:
            log.info(f"[Secrets] ⑤ 已從系統 Keyring ({service}) 載入 API 金鑰")
            return True
        else:
            log.warning("[Secrets] Keyring 中找不到 AQT API 金鑰")
            return False

    except Exception as e:
        log.warning(f"[Secrets] Keyring 載入失敗: {e}")
        return False


# ════════════════════════════════════════════════════════════
# 內部：加解密核心
# ════════════════════════════════════════════════════════════
def _derive_key(password: str, salt: bytes) -> bytes:
    """PBKDF2-HMAC-SHA256 衍生 32-byte Fernet 金鑰。"""
    import hashlib
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _ITERATIONS,
        dklen=32,
    )
    return base64.urlsafe_b64encode(dk)   # Fernet 需要 base64url 格式


def _encrypt_text(plain_text: str, password: str) -> bytes:
    """
    加密文字為二進位格式：
      MAGIC(7) | SALT(16) | FERNET_TOKEN(...)
    """
    from cryptography.fernet import Fernet
    import secrets as _sec
    salt  = _sec.token_bytes(_SALT_SIZE)
    key   = _derive_key(password, salt)
    token = Fernet(key).encrypt(plain_text.encode("utf-8"))
    return _MAGIC + salt + token


def _decrypt_file(path: Path, password: str) -> str:
    """解密 .env.enc，回傳明文字串。"""
    from cryptography.fernet import Fernet, InvalidToken
    data = path.read_bytes()

    # 驗證魔術頭
    if not data.startswith(_MAGIC):
        raise ValueError(
            f"檔案格式錯誤：{path} 不是有效的 AQT 加密環境變數檔案\n"
            "（可能是舊版格式或非此工具加密的檔案）"
        )

    salt  = data[len(_MAGIC) : len(_MAGIC) + _SALT_SIZE]
    token = data[len(_MAGIC) + _SALT_SIZE :]
    key   = _derive_key(password, salt)

    try:
        plain = Fernet(key).decrypt(token)
        return plain.decode("utf-8")
    except InvalidToken:
        raise ValueError(
            "Master Password 錯誤或檔案損毀，無法解密。\n"
            "請確認密碼正確，或重新執行 scripts/encrypt_env.py 重新加密。"
        )


def _get_master_password() -> str:
    """按優先順序取得 Master Password。"""
    # 1. 環境變數（CI/CD、Docker Secret 注入）
    pwd = os.getenv("AQT_MASTER_PASSWORD", "")
    if pwd:
        log.debug("[Secrets] 使用環境變數 AQT_MASTER_PASSWORD")
        return pwd

    # 2. 互動式終端輸入
    try:
        pwd = getpass.getpass("🔐 請輸入 AQT Master Password（解密環境變數）：")
        if not pwd:
            raise ValueError("Master Password 不能為空")
        return pwd
    except (EOFError, KeyboardInterrupt):
        raise ValueError("未輸入 Master Password，中止啟動")


def _find_enc_file() -> str:
    """在常見路徑尋找 .env.enc。"""
    candidates = [
        _ENC_FILE,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", _ENC_FILE),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return _ENC_FILE   # 找不到時回傳預設值（讓 load_encrypted_env 拋出 FileNotFoundError）


def _require_cryptography() -> None:
    try:
        import cryptography  # noqa: F401
    except ImportError:
        raise ImportError(
            "cryptography 套件未安裝，請執行：\n"
            "  pip install cryptography"
        )
